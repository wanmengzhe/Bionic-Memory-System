#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""S0 scheduler：唯一常驻调度器，按段拉起 stage/scan/night，管轮次、空档、熔断、看门狗。

依据 ARCHITECTURE v1.3.8：
  * 8 段一轮，段间隔 30 分钟；一轮跑完那一刻 + 20 小时进空档，再开下一轮
  * 不卡 24 小时：卡了顺延，跑完再算
  * 单段超 30 分钟熔断（只杀当前段，不影响后续段）
  * 轮次超 12 小时强制结束本轮、未完成段记 degraded，进空档
  * 看门狗：只报一次打 reported；受 main.signal.json 的 silenced 控制（校验 written_by、24h 失效）；
    重报间隔 30 分钟、同一故障每天最多 5 次；空档期自身休眠不扫不报
退出码：0 正常 / 2 通道不明或根目录越界 / 3 写失败 / 6 抢锁失败
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
from datetime import datetime, timedelta, timezone
from pathlib import Path

REAL_MEMORY = (Path.home() / ".dsh" / "memory").resolve()
CHANNEL_RE = re.compile(r"^(qqbot|weixin|wx)-v?\d*-(group|c2c|single)[-:]([0-9a-z]+)", re.I)
SEGMENTS = 8
DEFAULT_INTERVAL = 30 * 60      # 段间隔
DEFAULT_REST_HOURS = 20         # 空档
SEGMENT_TIMEOUT = 30 * 60       # 单段熔断
MAX_ROUND_HOURS = 12            # 轮次超时
REPORT_INTERVAL = 30 * 60       # 重报间隔
REPORT_PER_DAY = 5
SIGNAL_TTL = 24 * 3600


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt) -> str:
    return dt.isoformat(timespec="seconds")


def parse_iso(t):
    if not t:
        return None
    try:
        d = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
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


def paths(root, channel):
    base = root / channel
    return {
        "state": base / "scheduler.json",
        "signal": root / "main.signal.json",
        "alerts": base / "alerts.log",
        "ops": base / "ops.log",
        "last_run": base / "last_run.json",
    }


def log(p, detail):
    try:
        with p["ops"].open("a", encoding="utf-8") as fh:
            fh.write(f"{iso(now())} scheduler {detail}\n")
    except OSError:
        pass


def load_state(p):
    try:
        return json.loads(p["state"].read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_state(p, st):
    p["state"].parent.mkdir(parents=True, exist_ok=True)
    tmp = p["state"].with_suffix(".tmp")
    tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p["state"])


def read_signal(p, session_id):
    """返回 True 表示主脑要求静默（校验来源 + 24h 失效）。"""
    try:
        s = json.loads(p["signal"].read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if not s.get("silenced"):
        return False
    if session_id and s.get("written_by") and s["written_by"] != session_id:
        log(p, f"signal_reject written_by={s.get('written_by')}")
        return False
    at = parse_iso(s.get("updated_at"))
    if at and (now() - at).total_seconds() > SIGNAL_TTL:
        log(p, "signal_expired")
        return False
    return True


def alert(p, detail):
    try:
        with p["alerts"].open("a", encoding="utf-8") as fh:
            fh.write(f"{iso(now())} WATCHDOG {detail}\n")
    except OSError:
        pass


def run_stage(stage_cmd, channel, segment, timeout, memory_dir):
    cmd = [sys.executable, stage_cmd, "--channel", channel, "--segment", str(segment),
           "--memory-dir", memory_dir]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"rc": r.returncode, "out": (r.stdout or "").strip()[:200], "fused": False}
    except subprocess.TimeoutExpired:
        return {"rc": -9, "out": "", "fused": True}


def tick(cfg) -> dict:
    p = paths(cfg["root"], cfg["channel"])
    st = load_state(p)
    at = now()
    result = {"at": iso(at), "action": None}

    # 空档判断
    round_done = parse_iso(st.get("round_done_at"))
    if round_done:
        next_start = round_done + timedelta(hours=cfg["rest_hours"])
        if at < next_start:
            result.update(action="rest", next_start=iso(next_start))
            return result
        st.update(round_start=iso(at), segment=0, round_done_at=None)
        save_state(p, st)

    if not st.get("round_start"):
        st.update(round_start=iso(at), segment=0)
        save_state(p, st)

    started = parse_iso(st["round_start"])
    if started and (at - started).total_seconds() > cfg["max_round_hours"] * 3600:
        st.update(round_done_at=iso(at), overdue=True)
        save_state(p, st)
        p["last_run"].write_text(json.dumps({"round_done_at": iso(at), "overdue": True},
                                            ensure_ascii=False), encoding="utf-8")
        log(p, "round_timeout forced_end")
        result.update(action="round_timeout")
        return result

    seg = int(st.get("segment") or 0)
    if seg >= cfg["segments"]:
        st.update(round_done_at=iso(at))
        save_state(p, st)
        p["last_run"].write_text(json.dumps({"round_done_at": iso(at)}, ensure_ascii=False),
                                 encoding="utf-8")
        log(p, "round_done")
        result.update(action="round_done")
        return result

    nxt = seg + 1
    out = run_stage(cfg["stage_cmd"], cfg["channel"], nxt, cfg["segment_timeout"], cfg["memory_dir"])
    st["segment"] = nxt
    st["last_segment_at"] = iso(at)
    if out["fused"]:
        st["fused"] = int(st.get("fused", 0)) + 1
        log(p, f"fuse segment=S{nxt}")
    save_state(p, st)
    result.update(action="ran_segment", segment=nxt, stage=out)
    return result


def watchdog(cfg) -> dict:
    """检查"该段是否在预期窗口内启动"。只报一次，受 silenced 控制，重报 30 分钟、每日≤5 次。"""
    p = paths(cfg["root"], cfg["channel"])
    st = load_state(p)
    at = now()
    round_done = parse_iso(st.get("round_done_at"))
    if round_done and at < round_done + timedelta(hours=cfg["rest_hours"]):
        return {"state": "rest", "reported": False}          # 空档期休眠

    expected = parse_iso(st.get("last_segment_at")) or parse_iso(st.get("round_start"))
    if expected and (at - expected).total_seconds() > cfg["interval"] + cfg["segment_timeout"]:
        overdue = True
    else:
        overdue = False
    if not overdue:
        return {"state": "ok", "reported": False}

    if read_signal(p, cfg["session_id"]):
        return {"state": "silenced", "reported": False}

    day = at.strftime("%Y-%m-%d")
    if st.get("report_day") != day:
        st["report_day"] = day
        st["report_count"] = 0
        st["reported"] = False
    count = int(st.get("report_count", 0))
    last = parse_iso(st.get("last_report_at"))

    if st.get("reported") and last and (at - last).total_seconds() < REPORT_INTERVAL:
        return {"state": "waiting", "reported": False}
    if count >= REPORT_PER_DAY:
        return {"state": "throttled", "reported": False}

    alert(p, f"segment_overdue expected={iso(expected) if expected else '-'} count={count + 1}")
    st.update(reported=True, report_count=count + 1, last_report_at=iso(at))
    save_state(p, st)
    return {"state": "reported", "reported": True, "count": count + 1}


def main(argv=None):
    ap = argparse.ArgumentParser(description="S0 scheduler")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--memory-dir", default=None)
    ap.add_argument("--stage-cmd", default=str(Path(__file__).with_name("stage.py")))
    ap.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    ap.add_argument("--rest-hours", type=float, default=DEFAULT_REST_HOURS)
    ap.add_argument("--segments", type=int, default=SEGMENTS)
    ap.add_argument("--segment-timeout", type=float, default=SEGMENT_TIMEOUT)
    ap.add_argument("--max-round-hours", type=float, default=MAX_ROUND_HOURS)
    ap.add_argument("--tick", action="store_true", help="跑一个调度 tick 后退出（测试用）")
    ap.add_argument("--watchdog-once", action="store_true", help="只跑一次看门狗检查")
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args(argv)

    channel = (args.channel or detect_channel(os.environ.get("DSH_SESSION_ID")) or "")
    if channel not in ("qq", "weixin"):
        print("通道判不明", file=sys.stderr)
        return 2
    root = Path(args.memory_dir or os.environ.get("MEMORY_DIR") or REAL_MEMORY).expanduser().resolve()
    guard_root(root)
    cfg = {"root": root, "channel": channel, "stage_cmd": args.stage_cmd,
           "interval": args.interval, "rest_hours": args.rest_hours, "segments": args.segments,
           "segment_timeout": args.segment_timeout, "max_round_hours": args.max_round_hours,
           "memory_dir": str(root), "session_id": os.environ.get("DSH_SESSION_ID", "")}

    p = paths(root, channel)
    lock_path = root / channel / "scheduler.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock = lock_path.open("w")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("已有 scheduler 实例在跑", file=sys.stderr)
        return 6

    try:
        if args.status:
            print(json.dumps({"state": load_state(p)}, ensure_ascii=False, indent=2))
            return 0
        if args.watchdog_once:
            print(json.dumps(watchdog(cfg), ensure_ascii=False))
            return 0
        if args.tick:
            out = tick(cfg)
            out["watchdog"] = watchdog(cfg)
            print(json.dumps(out, ensure_ascii=False))
            return 0
        while True:
            out = tick(cfg)
            w = watchdog(cfg)
            print(json.dumps({"tick": out, "watchdog": w}, ensure_ascii=False), flush=True)
            time.sleep(args.interval)
    finally:
        try:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
