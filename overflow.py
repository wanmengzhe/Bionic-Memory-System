#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""M2 overflow：检查 short 水位，超限则按规则挤 20% 到 normal/key。

依据 ARCHITECTURE v1.3.7 D.6.1 第 2 步：
  * 上限：8000 字 / 400 条，任一触发即溢出；一次挤 20%
  * 顺序：low 最老 → mid 最老；high 不挤（但 high 占比 >60% 时最老 high 强制降 mid 再挤）
  * 落点：含**未到期**时间节点 → key（pinned=true, entered_key_at）；否则 normal（entered_flow_at=挤出时刻, stage=1）
  * 交接：先写目标夹，再给源条目 moved=true
退出码：0 未触发或成功 / 2 通道判不明或根目录越界 / 3 读写失败 / 6 抢锁失败
"""
from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REAL_MEMORY = (Path.home() / ".dsh" / "memory").resolve()
CHAR_LIMIT = 8000
ITEM_LIMIT = 400
HIGH_SHARE_LIMIT = 0.6
EVICT_RATIO = 0.2

FUTURE_RE = re.compile(r"(下周三|下周[一二三四五六日天]|明天|后天|这个月底|下个月|明年\s*\d{1,2}\s*月|\d{1,2}\s*月\s*\d{1,2}\s*[号日])")
PAST_RE = re.compile(r"(昨天|上周[一二三四五六日天]|上个月|前天)")
CHANNEL_RE = re.compile(r"^(qqbot|weixin|wx)-v?\d*-(group|c2c|single)[-:]([0-9a-z]+)", re.I)


def now_dt() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def detect_channel(sid: str) -> str | None:
    m = CHANNEL_RE.match((sid or "").strip())
    if not m:
        return None
    return "weixin" if m.group(1).lower().startswith(("weixin", "wx")) else "qq"


def guard_root(root: Path) -> None:
    try:
        root.relative_to(REAL_MEMORY)
    except ValueError:
        return
    print("拒绝启动：MEMORY_DIR 指向真实记忆库 ~/.dsh/memory", file=sys.stderr)
    raise SystemExit(2)


def load_jsonl(path: Path) -> list[dict]:
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


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(path)


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def has_future_node(text: str) -> bool:
    if PAST_RE.search(text):
        return False
    return bool(FUTURE_RE.search(text))


def log_ops(root: Path, channel: str, action: str, detail: str) -> None:
    try:
        with (root / channel / "ops.log").open("a", encoding="utf-8") as fh:
            fh.write(f"{iso(now_dt())} overflow {action} {detail}\n")
    except OSError:
        pass


def pick_victims(rows: list[dict], need_chars: int) -> list[dict]:
    """按 low→mid 最老选，high 不选；high 占比 >60% 时先降最老 high 为 mid。"""
    total_high = sum(len(r.get("text", "")) for r in rows if r.get("weight") == "high")
    total_chars = sum(len(r.get("text", "")) for r in rows) or 1

    work = [dict(r) for r in rows]
    if total_high / total_chars > HIGH_SHARE_LIMIT:
        highs = sorted([r for r in work if r.get("weight") == "high"],
                       key=lambda r: r.get("created_at") or "")
        demote = max(1, math.ceil(len(highs) * 0.1))
        for r in highs[:demote]:
            r["weight"] = "mid"
            r["_demoted"] = True

    ordered = sorted(work, key=lambda r: ({"low": 0, "mid": 1, "high": 2}.get(r.get("weight"), 1),
                                          r.get("created_at") or ""))
    victims, acc = [], 0
    for r in ordered:
        if r.get("weight") == "high":
            continue
        victims.append(r)
        acc += len(r.get("text", ""))
        if acc >= need_chars:
            break
    return victims


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="M2 overflow：short 溢出搬运")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--session", default=None)
    ap.add_argument("--memory-dir", default=None)
    args = ap.parse_args(argv)

    channel = (args.channel or detect_channel(args.session or os.environ.get("DSH_SESSION_ID", "")) or "")
    if channel not in ("qq", "weixin"):
        print("通道判不明，拒绝处理", file=sys.stderr)
        return 2
    root = Path(args.memory_dir or os.environ.get("MEMORY_DIR") or REAL_MEMORY).expanduser().resolve()
    guard_root(root)

    base = root / channel
    short_path = base / "short" / "pool.jsonl"
    lock_path = base / "short" / "pool.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return 6
        rows = load_jsonl(short_path)
        total_chars = sum(len(r.get("text", "")) for r in rows)
        if total_chars <= CHAR_LIMIT and len(rows) <= ITEM_LIMIT:
            print(json.dumps({"evicted": [], "to_key": [], "to_normal": [], "batch_id": None,
                              "chars": total_chars, "items": len(rows), "triggered": False},
                             ensure_ascii=False))
            return 0

        need_chars = max(1, int(total_chars * EVICT_RATIO))
        victims = pick_victims(rows, need_chars)
        if not victims:
            print(json.dumps({"evicted": [], "to_key": [], "to_normal": [], "batch_id": None,
                              "triggered": False, "note": "无可挤出的 low/mid 条目"}, ensure_ascii=False))
            return 0

        batch_id = f"b-{int(now_dt().timestamp())}"
        at = iso(now_dt())
        to_key, to_normal, evicted_ids = [], [], []
        for v in victims:
            evicted_ids.append(v.get("id"))
            if has_future_node(v.get("text", "")):
                v.update(pinned=True, entered_key_at=at, entered_flow_at=None, stage=None,
                         batch_id=batch_id)
                to_key.append(v)
            else:
                v.update(pinned=False, entered_flow_at=at, stage=1, entered_key_at=None,
                         batch_id=batch_id)
                to_normal.append(v)

        try:
            if to_key:
                append_jsonl(base / "key" / "pool.jsonl", to_key)
            if to_normal:
                append_jsonl(base / "normal" / "pool.jsonl", to_normal)
            remain = [r for r in rows if r.get("id") not in set(evicted_ids)]
            write_jsonl(short_path, remain)
        except OSError as exc:
            log_ops(root, channel, "write_fail", str(exc)[:80])
            print(f"写入失败：{exc}", file=sys.stderr)
            return 3

        log_ops(root, channel, "evict",
                f"batch={batch_id} key={len(to_key)} normal={len(to_normal)} remain={len(remain)}")
        print(json.dumps({"evicted": evicted_ids, "to_key": [r["id"] for r in to_key],
                          "to_normal": [r["id"] for r in to_normal], "batch_id": batch_id,
                          "chars": total_chars, "items": len(rows), "triggered": True},
                         ensure_ascii=False))
        return 0


if __name__ == "__main__":
    sys.exit(main())
