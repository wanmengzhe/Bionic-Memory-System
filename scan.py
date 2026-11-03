#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""M5 scan：10 天扫 key（降级到 normal）/ 30 天扫 permanent（打 kept 或物理删除）。

依据 ARCHITECTURE v1.3.8：
  10d：只扫 key 夹，含可解析日期且已过期，或 last_hit_at 超 30 天且 hits<2 → 降 normal
       降级时写 entered_flow_at=降级时刻、stage=1，并清空 entered_key_at
  30d：只扫 permanent 库中 kept=false 且 entered_perm_at 满 30 天者
       能留 → kept=true；明确无价值 → 物理删除（删前把 id/时间/原因/原文/batch_id 写 ops.log）
  两者互不重叠；S1–S7 一概不碰
退出码：0 成功 / 2 通道不明或根目录越界 / 3 写失败 / 6 抢锁失败
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REAL_MEMORY = (Path.home() / ".dsh" / "memory").resolve()
CHANNEL_RE = re.compile(r"^(qqbot|weixin|wx)-v?\d*-(group|c2c|single)[-:]([0-9a-z]+)", re.I)
DATE_RE = re.compile(r"(\d{1,2})\s*月\s*(\d{1,2})\s*[号日]|(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})")
KEY_DAYS = 10
PERM_DAYS = 30
STALE_HIT_DAYS = 30
STALE_HITS = 2


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt) -> str:
    return dt.isoformat(timespec="seconds")


def parse_iso(text):
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


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
            fh.write(f"{iso(now())} scan {detail}\n")
    except OSError:
        pass


def alert_dup(root, channel, ids, err):
    """同条两边的告警：写 alert 文件，生产环境由外部巡检读走（沙盒不直连邮件）。"""
    try:
        p = root / channel / "alerts.log"
        with p.open("a", encoding="utf-8") as fh:
            fh.write(f"{iso(now())} DUP_RISK ids={ids} err={err[:60]}\n")
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


def expired_by_date(text, at):
    """正文里写了具体日期且该日期已过 → True（保守：解析不出就算未过期）。"""
    m = DATE_RE.search(text or "")
    if not m:
        return False
    try:
        if m.group(1):
            month, day = int(m.group(1)), int(m.group(2))
            year = at.year
            d = datetime(year, month, day, tzinfo=timezone.utc)
        else:
            d = datetime(int(m.group(3)), int(m.group(4)), int(m.group(5)), tzinfo=timezone.utc)
        return d < at
    except ValueError:
        return False


def scan_key(root, channel, at):
    base = root / channel / "key"
    path = base / "pool.jsonl"
    lock_path = base / "pool.lock"
    base.mkdir(parents=True, exist_ok=True)
    try:
        lock_fh = lock_path.open("w")
    except OSError as exc:
        log_ops(root, channel, f"lock_open_fail err={str(exc)[:60]}")
        return None, 3
    with lock_fh as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return None, 6
        rows = load(path)
        demote, keep = [], []
        for r in rows:
            entered = parse_iso(r.get("entered_key_at"))
            if entered is None:
                keep.append(r)
                continue
            age_days = (at - entered).total_seconds() / 86400
            last = parse_iso(r.get("last_hit_at"))
            stale = last is not None and (at - last).days > STALE_HIT_DAYS and int(r.get("hits", 0)) < STALE_HITS
            if age_days >= KEY_DAYS and (expired_by_date(r.get("text"), at) or stale):
                r.update(entered_flow_at=iso(at), stage=1, entered_key_at=None, pinned=False)
                demote.append(r)
            else:
                keep.append(r)
        if demote:
            extra = load(base.parent / "normal" / "pool.jsonl")
            save(base.parent / "normal" / "pool.jsonl", extra + demote)
            try:
                save(path, keep)
            except OSError as exc:
                # append 成功、重写 key 失败：同条会同时存在于 key 与 normal，留痕并告警
                log_ops(root, channel,
                        f"dup_risk ids={[r.get('id') for r in demote]} "
                        f"reason=rewrite_key_failed err={str(exc)[:60]}")
                alert_dup(root, channel, [r.get("id") for r in demote], str(exc))
                return {"downgraded": [r.get("id") for r in demote], "kept": len(keep),
                        "warning": "rewrite_failed_dup_risk"}, 3
        return {"downgraded": [r.get("id") for r in demote], "kept": len(keep)}, 0


def scan_permanent(root, channel, at, delete_ids=None):
    base = root / channel / "permanent"
    path = base / "pool.jsonl"
    lock_path = base / "pool.lock"
    base.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return None, 6
        rows = load(path)
        keep, marked, deleted = [], [], []
        delete_ids = set(delete_ids or [])
        for r in rows:
            if r.get("kept"):
                keep.append(r)
                continue
            entered = parse_iso(r.get("entered_perm_at"))
            if entered is None or (at - entered).days < PERM_DAYS:
                keep.append(r)
                continue
            if r.get("id") in delete_ids:
                deleted.append(r)          # 判死：物理删除，不保留
            else:
                r["kept"] = True           # 少删原则：默认能留就留
                marked.append(r)
                keep.append(r)
        if deleted or marked:
            save(path, keep)
            for r in deleted:
                log_ops(root, channel,
                        f"purge id={r.get('id')} at={iso(at)} reason=scan30 rule=explicit "
                        f"batch=b-{int(at.timestamp())} text={r.get('text','')}")
        return {"kept_marked": [r.get("id") for r in marked],
                "purged": [r.get("id") for r in deleted],
                "remaining": len(keep)}, 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="M5 scan：10 天扫 key / 30 天扫 permanent")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--window", choices=["10d", "30d"], required=True)
    ap.add_argument("--memory-dir", default=None)
    ap.add_argument("--delete-id", action="append", default=None,
                    help="30d 时显式判死删除的 id，可多次；不给则只打 kept（少删原则）")
    ap.add_argument("--now", default=None, help="测试用：伪造当前时间 ISO8601")
    args = ap.parse_args(argv)

    channel = (args.channel or detect_channel(os.environ.get("DSH_SESSION_ID")) or "")
    if channel not in ("qq", "weixin"):
        print("通道判不明", file=sys.stderr)
        return 2
    root = Path(args.memory_dir or os.environ.get("MEMORY_DIR") or REAL_MEMORY).expanduser().resolve()
    guard_root(root)
    at = parse_iso(args.now) or now()

    if args.window == "10d":
        out, code = scan_key(root, channel, at)
    else:
        out, code = scan_permanent(root, channel, at, args.delete_id)
    if code != 0:
        return code
    log_ops(root, channel, f"window={args.window} {json.dumps(out, ensure_ascii=False)}")
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
