#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""M3 search：六站检索（context_window 由调用方传入，本模块负责后五站）。

依据 ARCHITECTURE v1.3.8：context_window → short_pool → normal → key → permanent → cold，命中即止。
只读 + 受限写（仅 hits / last_hit_at）。退出码：0 命中或零命中（看空数组）/ 2 通道不明 / 3 读失败 / 6 抢锁失败。
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
STATIONS = ["short", "normal", "key", "permanent"]   # cold 单独门控
COLD = "cold"
CHANNEL_RE = re.compile(r"^(qqbot|weixin|wx)-v?\d*-(group|c2c|single)[-:]([0-9a-z]+)", re.I)
K1, B = 1.5, 0.75


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def detect_channel(sid):
    m = CHANNEL_RE.match((sid or "").strip())
    if not m:
        return None
    return "weixin" if m.group(1).lower().startswith(("weixin", "wx")) else "qq"


def log_ops(channel_root, detail):
    """计数回写失败/跳过时留痕，可追溯。"""
    try:
        with (channel_root / "ops.log").open("a", encoding="utf-8") as fh:
            fh.write(f"{now_iso()} search {detail}\n")
    except OSError:
        pass


def guard_root(root):
    try:
        root.relative_to(REAL_MEMORY)
    except ValueError:
        return
    print("拒绝启动：MEMORY_DIR 指向真实记忆库", file=sys.stderr)
    raise SystemExit(2)


def grams(text):
    t = (text or "").lower()
    g = set()
    for run in re.findall(r"[\u4e00-\u9fff]+", t):
        if len(run) == 1:
            g.add(run)
        for i in range(len(run) - 1):
            g.add(run[i:i + 2])
    for w in re.findall(r"[a-z0-9]{2,}", t):
        g.add(w)
    return g


def load(station_dir):
    rows = []
    p = station_dir / "pool.jsonl"
    if not p.exists():
        return rows
    with p.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("superseded") or r.get("moved") or r.get("deleted"):
                continue
            rows.append(r)
    return rows


def bm25(query, docs):
    q = grams(query)
    if not q or not docs:
        return []
    n = len(docs)
    lens = [len(grams(d.get("text"))) or 1 for d in docs]
    avg = sum(lens) / n
    df = {}
    for g in q:
        df[g] = sum(1 for d in docs if g in grams(d.get("text")))
    scored = []
    for d, dl in zip(docs, lens):
        dg = grams(d.get("text"))
        score = 0.0
        for g in q:
            tf = 1 if g in dg else 0
            if tf == 0:
                continue
            idf = math.log(1 + (n - df[g] + 0.5) / (df[g] + 0.5))
            score += idf * (tf * (K1 + 1)) / (tf + K1 * (1 - B + B * dl / avg))
        if score > 0:
            scored.append((score, d))
    scored.sort(key=lambda x: -x[0])
    return scored


def main(argv=None):
    ap = argparse.ArgumentParser(description="M3 search 六站检索")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--query", required=True)
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--memory-dir", default=None)
    ap.add_argument("--no-count", action="store_true", help="不回写 hits（只读模式）")
    ap.add_argument("--ancient", action="store_true", help="显式要求才查 cold（“很久以前”）")
    args = ap.parse_args(argv)

    channel = (args.channel or detect_channel(os.environ.get("DSH_SESSION_ID")) or "")
    if channel not in ("qq", "weixin"):
        print("通道判不明", file=sys.stderr)
        return 2
    root = Path(args.memory_dir or os.environ.get("MEMORY_DIR") or REAL_MEMORY).expanduser().resolve()
    guard_root(root)
    base = root / channel

    try:
        stations = list(STATIONS) + ([COLD] if args.ancient else [])
        for station in stations:
            docs = load(base / station)
            hits = bm25(args.query, docs)[:args.limit]
            if not hits:
                continue
            out = []
            for score, d in hits:
                out.append({"id": d.get("id"), "bucket": station, "score": round(score, 4),
                            "text": d.get("text", "")})
            if not args.no_count:
                bump(base / station, [h["id"] for h in out])
            print(json.dumps(out, ensure_ascii=False))
            return 0
        print(json.dumps([], ensure_ascii=False))
        return 0
    except OSError as exc:
        print(f"读失败：{exc}", file=sys.stderr)
        return 3


def bump(station_dir, ids):
    """受限写：只改 hits / last_hit_at。抢不到锁就跳过，不影响返回。"""
    path = station_dir / "pool.jsonl"
    if not path.exists() or not ids:
        return
    lock_path = station_dir / "pool.lock"
    try:
        lock = lock_path.open("w")
    except OSError:
        return
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log_ops(station_dir.parent, f"count_skip lock_busy station={station_dir.name} ids={len(ids)}")
        lock.close()
        return
    try:
        rows, changed = [], False
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("id") in ids:
                    r["hits"] = int(r.get("hits", 0)) + 1
                    r["last_hit_at"] = now_iso()
                    changed = True
                rows.append(r)
        if changed:
            tmp = path.with_suffix(path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            tmp.replace(path)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


if __name__ == "__main__":
    sys.exit(main())
