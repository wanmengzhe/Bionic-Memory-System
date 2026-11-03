#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""M4 stage：推进 S1–S8；到 S8 只打 s8_pending=true，不压缩、不调模型。

依据 ARCHITECTURE v1.3.8：
  * 扫 normal 中 stage=k 的条目 → stage+1
  * 跳过 s8_pending=true 的条目（防越过 S8 原地循环）；判定顺序 s8_pending 优先
  * 走到 S8：打 s8_pending=true，stage 保持 8，条目留在 normal 等夜间批
  * 单段 ≤30 分钟，超时即熔断（退出码 4）
退出码：0 成功 / 2 通道不明或根目录越界 / 3 写失败 / 4 熔断 / 6 抢锁失败
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REAL_MEMORY = (Path.home() / ".dsh" / "memory").resolve()
CHANNEL_RE = re.compile(r"^(qqbot|weixin|wx)-v?\d*-(group|c2c|single)[-:]([0-9a-z]+)", re.I)
SEGMENT_TIMEOUT = 30 * 60  # 秒


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
            fh.write(f"{now_iso()} stage {detail}\n")
    except OSError:
        pass


def main(argv=None):
    ap = argparse.ArgumentParser(description="M4 stage 阶段推进")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--segment", type=int, default=None, help="只推进指定段（1..8）；缺省推进当前游标段")
    ap.add_argument("--memory-dir", default=None)
    args = ap.parse_args(argv)

    channel = (args.channel or detect_channel(os.environ.get("DSH_SESSION_ID")) or "")
    if channel not in ("qq", "weixin"):
        print("通道判不明", file=sys.stderr)
        return 2
    root = Path(args.memory_dir or os.environ.get("MEMORY_DIR") or REAL_MEMORY).expanduser().resolve()
    guard_root(root)

    base = root / channel
    normal_dir = base / "normal"
    path = normal_dir / "pool.jsonl"
    lock_path = normal_dir / "pool.lock"
    normal_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return 6
        rows = []
        if path.exists():
            with path.open("r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        continue

        stage_now = args.segment
        if stage_now is None:
            stages = [int(r.get("stage") or 0) for r in rows if r.get("stage")]
            stage_now = min(stages) if stages else 1
        stage_now = max(1, min(8, stage_now))

        advanced, s8, skipped, scanned = [], [], [], 0
        for r in rows:
            if r.get("s8_pending"):          # 优先跳过
                skipped.append(r.get("id"))
                continue
            if r.get("stage") != stage_now:
                continue
            scanned += 1
            if stage_now >= 8:
                r["s8_pending"] = True        # 只打标记，不压缩
                r["stage"] = 8
                s8.append(r.get("id"))
            else:
                r["stage"] = stage_now + 1
                advanced.append(r.get("id"))

        elapsed = time.time() - started
        if elapsed > SEGMENT_TIMEOUT:
            log_ops(root, channel, f"fuse segment=S{stage_now} elapsed={int(elapsed)}s")
            print(json.dumps({"segment": stage_now, "fused": True, "elapsed": int(elapsed)},
                             ensure_ascii=False))
            return 4

        try:
            tmp = path.with_suffix(path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            tmp.replace(path)
        except OSError as exc:
            log_ops(root, channel, f"write_fail seg=S{stage_now} {str(exc)[:60]}")
            print(f"写入失败：{exc}", file=sys.stderr)
            return 3

        log_ops(root, channel, f"seg=S{stage_now} scanned={scanned} advanced={len(advanced)} s8={len(s8)} skipped={len(skipped)}")
        print(json.dumps({"segment": stage_now, "scanned": scanned, "advanced": len(advanced),
                          "s8_fragments": len(s8), "skipped": len(skipped),
                          "advanced_ids": advanced, "s8_ids": s8}, ensure_ascii=False))
        return 0


if __name__ == "__main__":
    sys.exit(main())
