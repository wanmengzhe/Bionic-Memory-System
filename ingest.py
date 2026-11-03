#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""M1 ingest：收一句话 → 判定 weight/标签 → 追加进 short/pool.jsonl。

只做入库，不做溢出、不做检索、不碰其他夹。依据 ARCHITECTURE v1.3.7 D.6.1 第 1 步。
退出码：0 成功 / 2 通道判不明或根目录越界 / 3 写入失败 / 6 抢锁失败
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REAL_MEMORY = (Path.home() / ".dsh" / "memory").resolve()

LOW_KW = ["哈哈哈", "吃饭了吗", "打了个哈欠", "嗯嗯", "无聊", "随便聊聊", "天气真好", "呵呵", "睡了"]
PREF_KW = ["我喜欢", "我爱", "我习惯", "我讨厌", "不吃", "不喝", "非常喜欢", "偏爱", "受不了"]
FUTURE_RE = re.compile(r"(下周三|下周[一二三四五六日天]|明天|后天|这个月底|下个月|明年\s*\d{1,2}\s*月|\d{1,2}\s*月\s*\d{1,2}\s*[号日])")
PAST_RE = re.compile(r"(昨天|上周[一二三四五六日天]|上个月|前天)")
TAG_RULES = [
    ("#语言", ["Python", "Java", "Rust", "Go", "JavaScript"]),
    ("#工具", ["Docker", "VS Code", "Vim", "Obsidian", "Notion", "Logseq", "Markdown"]),
    ("#饮食", ["吃", "喝", "咖啡", "茶", "苹果", "香蕉"]),
    ("#运动", ["爬山", "健身", "跑步", "游泳"]),
    ("#宠物", ["猫", "狗", "团子"]),
    ("#工作", ["代码", "单元测试", "提交", "服务器", "部署"]),
    ("#时间节点", ["考试", "出差", "会议", "开会", "报告"]),
]
CHANNEL_RE = re.compile(r"^(qqbot|weixin|wx)-v?\d*-(group|c2c|single)[-:]([0-9a-z]+)", re.I)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def detect_channel(session_id: str) -> str | None:
    """只认 qqbot/weixin 前缀；认不出返回 None（调用方拒写）。"""
    m = CHANNEL_RE.match((session_id or "").strip())
    if not m:
        return None
    return "weixin" if m.group(1).lower().startswith(("weixin", "wx")) else "qq"


def memory_root(env_dir: str | None) -> Path:
    root = Path(env_dir).expanduser().resolve() if env_dir else REAL_MEMORY
    return root


def guard_root(root: Path) -> None:
    """D.6.5：根目录落在 ~/.dsh/memory 之下即拒启（沙盒必须指到别处）。"""
    try:
        root.relative_to(REAL_MEMORY)
    except ValueError:
        return
    print("拒绝启动：MEMORY_DIR 指向真实记忆库 ~/.dsh/memory，请指到临时目录", file=sys.stderr)
    raise SystemExit(2)


def classify(text: str) -> tuple[str, str, list[str]]:
    """返回 (bucket, weight, tags)。bucket: drop / short"""
    t = text.strip()
    tags = [tag for tag, kws in TAG_RULES if any(k in t for k in kws)]
    if any(k in t for k in LOW_KW) and not any(k in t for k in PREF_KW):
        return "drop", "low", tags
    has_time = bool(FUTURE_RE.search(t) or PAST_RE.search(t))
    is_pref = any(k in t for k in PREF_KW)
    weight = "high" if (is_pref and not has_time) else "mid"
    return "short", weight, tags


def make_id(channel: str, text: str) -> str:
    digest = hashlib.sha1(f"{channel}\0{text}".encode("utf-8")).hexdigest()[:8]
    return f"{channel}-{int(time.time())}-{digest}"


def existing_texts(path: Path) -> set[str]:
    """读现有正文集合（按通道内文本判重，不依赖时间戳 id）；坏行跳过。"""
    texts = set()
    if not path.exists():
        return texts
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                texts.add(" ".join((json.loads(line).get("text") or "").split()))
            except ValueError:
                continue
    return texts


def append_locked(path: Path, row: dict) -> str:
    """持锁 → 去重 → 追加。返回 'written' 或 'duplicate'。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.parent / "pool.lock"
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SystemExit(6)
        try:
            if " ".join(row["text"].split()) in existing_texts(path):
                return "duplicate"
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            return "written"
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


KEY_QUOTA_CHARS = 20000


def key_over_quota(root: Path, channel: str) -> int:
    """key 夹总量（字数）超过 2 万字 → 写 alerts.log 一行，不自动处理。"""
    path = root / channel / "key" / "pool.jsonl"
    total = 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    total += len(json.loads(line).get("text") or "")
                except ValueError:
                    continue
    except OSError:
        return 0
    if total <= KEY_QUOTA_CHARS:
        return total
    # 节流：同一天只记一次
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    marker = root / channel / ".key_quota_alert_day"
    try:
        if marker.exists() and marker.read_text(encoding="utf-8").strip() == today:
            return total
    except OSError:
        pass
    try:
        with (root / channel / "alerts.log").open("a", encoding="utf-8") as fh:
            fh.write(f"{now_iso()} KEY_OVER_QUOTA chars={total} limit={KEY_QUOTA_CHARS}\n")
        marker.write_text(today + "\n", encoding="utf-8")
    except OSError:
        pass
    return total


def log_ops(root: Path, channel: str, action: str, detail: str) -> None:
    try:
        with (root / channel / "ops.log").open("a", encoding="utf-8") as fh:
            fh.write(f"{now_iso()} ingest {action} {detail}\n")
    except OSError:
        pass


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="M1 ingest：写 short 池")
    ap.add_argument("--channel", default=None, help="qq / weixin；缺省从 DSH_SESSION_ID 推断")
    ap.add_argument("--session", default=None, help="会话 ID（默认取 DSH_SESSION_ID）")
    ap.add_argument("--text", required=True)
    ap.add_argument("--tags", nargs="*", default=None, help="额外标签，可多个")
    ap.add_argument("--memory-dir", default=None, help="记忆根目录（沙盒必填，默认 MEMORY_DIR）")
    args = ap.parse_args(argv)

    session_id = args.session or os.environ.get("DSH_SESSION_ID", "")
    channel = (args.channel or detect_channel(session_id) or "").strip()
    if channel not in ("qq", "weixin"):
        print("通道判不明，拒绝写入", file=sys.stderr)
        return 2

    root = memory_root(args.memory_dir or os.environ.get("MEMORY_DIR"))
    guard_root(root)

    bucket, weight, tags = classify(args.text)
    tags = list(dict.fromkeys(tags + (args.tags or [])))
    if bucket == "drop":
        log_ops(root, channel, "drop", f"weight=low text={args.text[:20]}")
        print(json.dumps({"id": None, "bucket": "drop", "weight": weight, "tags": tags,
                          "created_at": now_iso()}, ensure_ascii=False))
        return 0

    row = {
        "id": make_id(channel, args.text),
        "text": args.text.strip(),
        "tags": tags,
        "weight": weight,
        "created_at": now_iso(),
        "entered_flow_at": None,
        "entered_key_at": None,
        "entered_perm_at": None,
        "stage": None,
        "hits": 0,
        "last_hit_at": None,
        "superseded": False,
        "pinned": False,
        "moved": False,
        "batch_id": None,
        "degraded": False,
        "kept": False,
    }
    if weight == "high":
        row.update(pinned=False, entered_key_at=now_iso(), stage=None)
        target = root / channel / "key" / "pool.jsonl"
    else:
        target = root / channel / "short" / "pool.jsonl"
    try:
        result = append_locked(target, row)
    except SystemExit:
        log_ops(root, channel, "lock_fail", args.text[:20])
        raise
    except OSError as exc:
        log_ops(root, channel, "write_fail", str(exc)[:80])
        print(f"写入失败：{exc}", file=sys.stderr)
        return 3

    if target.parent.name == "key" and result == "written":
        key_over_quota(root, channel)

    if result == "duplicate":
        log_ops(root, channel, "duplicate", f"id={row['id']} text={args.text[:20]}")
        print(json.dumps({"id": row["id"], "bucket": "duplicate", "weight": weight,
                          "tags": tags, "created_at": row["created_at"]}, ensure_ascii=False))
        return 0

    log_ops(root, channel, "ingest", f"id={row['id']} weight={weight}")
    print(json.dumps({"id": row["id"], "bucket": ("key" if weight == "high" else "short"),
                      "weight": weight, "tags": tags,
                      "created_at": row["created_at"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
