#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""M6 night：夜间批处理，三个子任务。

依据 ARCHITECTURE v1.3.8 D.4.5 / D.6.1 第 6 步：
  compress  S8 提炼：normal 中 s8_pending=true → 碎片(≤30字)进 permanent、原文进 cold、条目离场
  retag     补标签：short/normal 中 tags 为空 或 标签数≤1 且正文>15 字 → 模型补标签（只补不覆盖）
  reweight  weight 复核：normal 中 mid 条目批量复核（30 条/批）
  merge     已随 core 取消而废弃（调用时返回 skipped）

失败处理：调模型失败 → 相关条目标 degraded=true、写 ops.log、不重试。
沙盒默认走桩模型（--stub-model 默认开），不烧 Token；--real-model 才尝试外部调用（本沙盒不实现）。
退出码：0 成功 / 2 通道不明或根目录越界 / 3 写失败 / 5 模型失败（置 degraded）/ 6 抢锁失败
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REAL_MEMORY = (Path.home() / ".dsh" / "memory").resolve()
CHANNEL_RE = re.compile(r"^(qqbot|weixin|wx)-v?\d*-(group|c2c|single)[-:]([0-9a-z]+)", re.I)
FRAGMENT_CHARS = 30
RETAG_BATCH = 30
REWEIGHT_BATCH = 30
TAG_HINTS = [
    ("#语言", ["Python", "Java", "Rust", "Go"]), ("#工具", ["Docker", "VS Code", "Vim", "Markdown"]),
    ("#饮食", ["吃", "喝", "咖啡", "苹果"]), ("#运动", ["爬山", "健身"]), ("#宠物", ["猫", "狗"]),
    ("#工作", ["代码", "测试", "服务器"]), ("#家庭", ["孩子", "老婆", "父母"]), ("#财务", ["股票", "基金", "预算"]),
]


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt) -> str:
    return dt.isoformat(timespec="seconds")


def detect_channel(sid):
    m = CHANNEL_RE.match((sid or "").strip())
    if not m:
        return None
    return "weixin" if m.group(1).lower().startswith(("weixin", "wx")) else "qq"


def guard_root(root):
    try:
        root.relative_to(REAL_MEMORY)
    except ValueError:
        return
    print("拒绝启动：MEMORY_DIR 指向真实记忆库", file=sys.stderr)
    raise SystemExit(2)


def log_ops(root, channel, detail):
    try:
        with (root / channel / "ops.log").open("a", encoding="utf-8") as fh:
            fh.write(f"{iso(now())} night {detail}\n")
    except OSError:
        pass



def acquire_locks(base: Path, stations) -> list:
    """按固定顺序对涉及的夹层加锁；任一失败返回 []（调用方退 6）。"""
    handles = []
    for name in sorted(set(stations)):
        d = base / name
        d.mkdir(parents=True, exist_ok=True)
        try:
            fh = (d / "pool.lock").open("w")
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            handles.append(fh)
        except OSError:
            for h in handles:
                try:
                    fcntl.flock(h, fcntl.LOCK_UN); h.close()
                except OSError:
                    pass
            return []
    return handles


def release_locks(handles) -> None:
    for h in handles:
        try:
            fcntl.flock(h, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            h.close()
        except OSError:
            pass


def load(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except ValueError:
                continue
    return rows


def save(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(path)


def append(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def stub_tags(text):
    return [tag for tag, kws in TAG_HINTS if any(k in text for k in kws)][:3]


def needs_retag(r):
    tags = r.get("tags") or []
    return (not tags) or (len(tags) <= 1 and len(r.get("text") or "") > 15)


def job_compress(root, channel, at, fail):
    base = root / channel
    normal = load(base / "normal" / "pool.jsonl")
    targets = [r for r in normal if r.get("s8_pending")]
    if not targets:
        return {"job": "compress", "touched": 0, "degraded": 0, "calls": 0}
    if fail:
        for r in targets:
            r["degraded"] = True
        save(base / "normal" / "pool.jsonl", normal)
        log_ops(root, channel, f"compress failed degraded={len(targets)}")
        return {"job": "compress", "touched": 0, "degraded": len(targets), "calls": 1}

    frags, cold, remain = [], [], []
    for r in normal:
        if not r.get("s8_pending"):
            remain.append(r)
            continue
        frag = dict(r)
        raw = r.get("text") or ""
        frag["text"] = raw[:FRAGMENT_CHARS]
        frag["orig_len"] = len(raw)          # 原文长度：判断压过还是原样搬
        frag.update(entered_perm_at=iso(at), entered_flow_at=None, stage=None,
                    s8_pending=False, kept=False, degraded=False)
        frags.append(frag)
        cold_row = dict(r)
        cold_row.update(purged_from="normal", purged_at=iso(at))
        cold.append(cold_row)
    append(base / "permanent" / "pool.jsonl", frags)
    append(base / "cold" / "pool.jsonl", cold)
    save(base / "normal" / "pool.jsonl", remain)
    log_ops(root, channel, f"compress fragments={len(frags)} cold={len(cold)}")
    return {"job": "compress", "touched": len(frags), "degraded": 0, "calls": 1}


def job_retag(root, channel, fail):
    base = root / channel
    out = {"job": "retag", "batches": 0, "tagged": 0, "degraded": 0, "calls": 0}
    for station in ("short", "normal"):
        rows = load(base / station / "pool.jsonl")
        targets = [r for r in rows if needs_retag(r)]
        if not targets:
            continue
        if fail:
            for r in targets:
                r["degraded"] = True
            save(base / station / "pool.jsonl", rows)
            out["degraded"] += len(targets)
            out["calls"] += 1
            out["batches"] += 1
            continue
        for i in range(0, len(targets), RETAG_BATCH):
            batch = targets[i:i + RETAG_BATCH]
            for r in batch:
                extra = [t for t in stub_tags(r.get("text", "")) if t not in (r.get("tags") or [])]
                r["tags"] = list(dict.fromkeys((r.get("tags") or []) + extra))
                out["tagged"] += 1
            out["batches"] += 1
            out["calls"] += 1
        save(base / station / "pool.jsonl", rows)
    log_ops(root, channel, f"retag {json.dumps(out, ensure_ascii=False)}")
    return out


def job_reweight(root, channel, fail):
    base = root / channel
    rows = load(base / "normal" / "pool.jsonl")
    mids = [r for r in rows if r.get("weight") == "mid"]
    out = {"job": "reweight", "checked": len(mids), "changed": 0, "degraded": 0, "calls": 0}
    if not mids:
        return out
    if fail:
        for r in mids:
            r["degraded"] = True
        save(base / "normal" / "pool.jsonl", rows)
        out["degraded"] = len(mids)
        out["calls"] = 1
        return out
    for i in range(0, len(mids), REWEIGHT_BATCH):
        for r in mids[i:i + REWEIGHT_BATCH]:
            text = r.get("text") or ""
            if any(k in text for k in ("喜欢", "讨厌", "习惯")) and not re.search(r"\d", text):
                r["weight"] = "high"
                out["changed"] += 1
        out["calls"] += 1
    save(base / "normal" / "pool.jsonl", rows)
    log_ops(root, channel, f"reweight {json.dumps(out, ensure_ascii=False)}")
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="M6 night 夜间批处理")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--job", required=True, choices=["compress", "retag", "reweight", "merge"])
    ap.add_argument("--memory-dir", default=None)
    ap.add_argument("--now", default=None)
    ap.add_argument("--stub-model", action="store_true", default=True)
    ap.add_argument("--fail", action="store_true", help="模拟模型失败，用于测 degraded 路径")
    args = ap.parse_args(argv)

    channel = (args.channel or detect_channel(os.environ.get("DSH_SESSION_ID")) or "")
    if channel not in ("qq", "weixin"):
        print("通道判不明", file=sys.stderr)
        return 2
    root = Path(args.memory_dir or os.environ.get("MEMORY_DIR") or REAL_MEMORY).expanduser().resolve()
    guard_root(root)
    at = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else now()

    if args.job == "merge":
        print(json.dumps({"job": "merge", "skipped": True, "note": "core 已取消，merge 废弃"}, ensure_ascii=False))
        return 0

    stations = {"compress": ("normal", "permanent", "cold"),
                "retag": ("short", "normal"),
                "reweight": ("normal",)}.get(args.job, ())
    handles = acquire_locks(root / channel, stations)
    if not handles:
        print("抢锁失败，跳过本轮", file=sys.stderr)
        return 6
    try:
        if args.job == "compress":
            out = job_compress(root, channel, at, args.fail)
        elif args.job == "retag":
            out = job_retag(root, channel, args.fail)
        else:
            out = job_reweight(root, channel, args.fail)
    except OSError as exc:
        log_ops(root, channel, f"write_fail job={args.job} {str(exc)[:60]}")
        print(f"写入失败：{exc}", file=sys.stderr)
        return 3
    finally:
        release_locks(handles)
    print(json.dumps(out, ensure_ascii=False))
    return 5 if out.get("degraded") and args.fail else 0


if __name__ == "__main__":
    sys.exit(main())
