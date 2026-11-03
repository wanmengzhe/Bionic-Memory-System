#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""hook：轮询会话日志 → 取用户消息 → 去帧前缀 → 调 ingest 入池。

依据 ARCHITECTURE v1.3.9 + 上线清单：
  * 只取 user/message，且只取非群聊会话（qqbot-v2-c2c-* / weixin-v3-single-*）
  * 剥掉 "[QQ private chat message from ... | openid=...]" 这类帧前缀
  * 跳过 system-reminder、附件占位、空文本
  * 先 ingest 成功再推游标（宁可重复，只靠 ingest 去重兜底，绝不漏）
  * 单条失败跳过不阻塞；同一会话连续 3 轮失败 → 写 alerts.log HOOK_STALL 并跳过
退出码：0 正常 / 2 通道不明 / 3 状态写失败 / 6 抢锁失败
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HOME = Path(os.environ.get("DSH_HOME") or (Path.home() / ".dsh"))
SESSION_ROOT = HOME / "sessions"
REAL_MEMORY = (HOME / "memory").resolve()
FRAME_RE = re.compile(r"^\[[^\]\n]{0,200}\]\s*\n?", re.S)
SKIP_PREFIX = ("<system-reminder>", "[QQ attachment:", "[Weixin attachment:")
MAX_FAIL_STREAK = 3


def parse_since(text: str | None) -> datetime:
    """--since 支持 ISO8601 或 YYYY-MM-DD；缺省=今天 0 点（本地时区）。"""
    from datetime import date
    if not text:
        d = datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
        return d.astimezone(timezone.utc)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.astimezone()
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass
    try:
        d = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=datetime.now().astimezone().tzinfo)
        return d.astimezone(timezone.utc)
    except ValueError:
        raise SystemExit(f"--since 无法解析：{text}")


def event_time(ev: dict) -> datetime | None:
    ts = ev.get("time")
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(path: Path, detail: str) -> None:
    try:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(f"{now_iso()} hook {detail}\n")
    except OSError:
        pass


def guard_root(root: Path) -> None:
    try:
        root.relative_to(REAL_MEMORY)
    except ValueError:
        return
    print("拒绝启动：MEMORY_DIR 指向真实记忆库", file=sys.stderr)
    raise SystemExit(2)


def read_events(path: Path) -> list[dict]:
    """解压并解析会话日志；zstd 走系统命令，失败返回空。"""
    try:
        r = subprocess.run(["zstd", "-dc", str(path)], capture_output=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return []
    if r.returncode != 0:
        return []
    events = []
    for line in r.stdout.decode("utf-8", "ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except ValueError:
            continue
    return events


def extract_text(ev: dict) -> str:
    data = ev.get("data") or {}
    parts = []
    for block in data.get("content") or []:
        if block.get("type") == "text" and block.get("text"):
            parts.append(block["text"])
    text = "\n".join(parts).strip()
    text = FRAME_RE.sub("", text, count=1).strip()
    if not text or text.startswith(SKIP_PREFIX):
        return ""
    return text


def session_kind(name: str) -> str | None:
    """返回 c2c / single / group / other。"""
    low = name.lower()
    if not (low.startswith("qqbot") or low.startswith("weixin")):
        return None
    if "-group-" in low:
        return "group"
    if "-c2c-" in low:
        return "c2c"
    if "-single-" in low:
        return "single"
    return "other"


def collect_sessions(channel: str, sessions_root: Path | None = None) -> list[Path]:
    out = []
    root = sessions_root or SESSION_ROOT
    if not root.is_dir():
        return out
    for d in root.iterdir():
        if not d.is_dir():
            continue
        for sub in d.iterdir():
            if not sub.is_dir():
                continue
            kind = session_kind(sub.name)
            if kind not in ("c2c", "single"):
                continue                       # 群聊不进新池
            if channel == "qq" and not sub.name.lower().startswith("qqbot"):
                continue
            if channel == "weixin" and not sub.name.lower().startswith(("weixin", "wx")):
                continue
            f = sub / "session.jsonl.zstd"
            if f.is_file():
                out.append(f)
    return sorted(out, key=lambda p: p.stat().st_mtime, reverse=True)


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"cursors": {}, "fail": {}}


def save_state(path: Path, st: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def run_ingest(cmd: list[str], channel: str, text: str, memory_dir: str) -> int:
    args = [*cmd, "--channel", channel, "--text", text, "--memory-dir", memory_dir]
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=60)
        return r.returncode
    except (OSError, subprocess.TimeoutExpired):
        return 1


def poll_once(cfg) -> dict:
    channel = cfg["channel"]
    base = Path(cfg["memory_dir"]) / channel
    log_path = base / "ops.log"
    alerts = base / "alerts.log"
    st = load_state(cfg["state"])
    cursors, fails = st.setdefault("cursors", {}), st.setdefault("fail", {})
    stats = {"scanned": 0, "ingested": 0, "skipped": 0, "failed": 0, "stalled": 0,
             "before_since": 0}

    for f in collect_sessions(channel, Path(cfg["sessions_root"])):
        key = f.parent.name
        events = read_events(f)
        if not events:
            stats["skipped"] += 1
            continue
        stats["scanned"] += 1
        last = int(cursors.get(key, -1))
        for ev in events:
            if ev.get("type") != "user/message":
                continue
            seq = int(ev.get("seq") or 0)
            if seq <= last:
                continue
            et = event_time(ev)
            if et is not None and et < cfg["since"]:
                cursors[key] = seq          # 早于起点的直接跳过，游标推进但不入库
                stats["before_since"] += 1
                continue
            last = seq                              # 先推进内部指针，成功后落盘
            text = extract_text(ev)
            if not text:
                cursors[key] = seq
                stats["skipped"] += 1
                continue
            rc = run_ingest(cfg["ingest_cmd"], channel, text, cfg["memory_dir"])
            if rc == 0:
                cursors[key] = seq
                fails.pop(key, None)
                stats["ingested"] += 1
            else:
                fails[key] = int(fails.get(key, 0)) + 1
                stats["failed"] += 1
                log(log_path, f"ingest_fail rc={rc} session={key} streak={fails[key]} text={text[:20]}")
                if fails[key] >= MAX_FAIL_STREAK:
                    log(alerts, f"HOOK_STALL session={key} streak={fails[key]}")
                    cursors[key] = seq              # 连续失败：跳过该条，避免死循环
                    fails.pop(key, None)
                    stats["stalled"] += 1
    save_state(cfg["state"], st)
    return stats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="hook：会话日志 → ingest")
    ap.add_argument("--channel", required=True, choices=["qq", "weixin"])
    ap.add_argument("--memory-dir", default=str(REAL_MEMORY))
    ap.add_argument("--sessions-root", default=str(SESSION_ROOT))
    ap.add_argument("--interval", type=float, default=60)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--ingest-cmd", default=str(Path(__file__).with_name("ingest.py")))
    ap.add_argument("--since", default=None,
                    help="只灌此刻之后的消息（ISO8601 或 YYYY-MM-DD）；缺省=今天 0 点")
    args = ap.parse_args(argv)

    root = Path(args.memory_dir).expanduser().resolve()
    guard_root(root)

    cfg = {"channel": args.channel, "memory_dir": str(root),
           "state": root / args.channel / "hook-state.json",
           "ingest_cmd": [sys.executable, args.ingest_cmd],
           "sessions_root": str(Path(args.sessions_root).expanduser()),
           "since": parse_since(args.since)}

    lock_path = root / args.channel / "hook.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock = lock_path.open("w")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("另一个 hook 实例在跑", file=sys.stderr)
        return 6
    try:
        if args.once:
            print(json.dumps(poll_once(cfg), ensure_ascii=False))
            return 0
        while True:
            print(json.dumps(poll_once(cfg), ensure_ascii=False), flush=True)
            time.sleep(args.interval)
    finally:
        try:
            fcntl.flock(lock, fcntl.LOCK_UN); lock.close()
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
