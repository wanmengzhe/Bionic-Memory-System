# 仿生认知记忆系统（模块集）

沙盒/生产两用的记忆流水线，纯 Python 标准库实现，**零第三方依赖**。
设计文档见 `ARCHITECTURE.md`（v1.4.0），本文件只讲怎么跑。

## 依赖

- Python 3.12（本机自带）
- 系统件：`zstd`（仅 `hook.py` 解压会话日志用）、`tar`（归档用）
- 无 pip 包、无 Node、无数据库

## 目录约定

```
memory/<通道>/
├── short/pool.jsonl        短期池（未进流程）
├── normal/pool.jsonl       普通夹（S1–S8 衰减主体）
├── key/pool.jsonl          免死区（核心偏好、时间节点）
├── permanent/pool.jsonl    永久词典库（S8 碎片归宿）
├── cold/pool.jsonl         归档
├── ops.log                 操作日志
├── alerts.log              告警
├── hook-state.json         钩子游标
└── scheduler.json          轮次状态
<通道> ∈ {qq, weixin}
```

## 八个模块

| 模块 | 作用 | 常用命令 |
| --- | --- | --- |
| `hook.py` | 扫会话日志 → 取用户消息 → 调 ingest | `python3 hook.py --channel qq --memory-dir <库> --interval 60` |
| `ingest.py` | 判定 weight/标签并入库（high→key，其余→short） | `python3 ingest.py --channel qq --text "..."` |
| `overflow.py` | short 超限挤出 20% 到 normal/key | `python3 overflow.py --channel qq` |
| `stage.py` | 推进 S1–S8，到 S8 只打 `s8_pending` | `python3 stage.py --segment 1` |
| `night.py` | 夜间批处理：compress / retag / reweight | `python3 night.py --job compress` |
| `scan.py` | 10 天扫 key / 30 天扫 permanent | `python3 scan.py --window 10d` |
| `search.py` | 六站检索（只读 + 计数回写） | `python3 search.py --query "..."` |
| `scheduler.py` | 常驻调度：轮次、熔断、看门狗 | `python3 scheduler.py --channel qq --memory-dir <库>` |

## 快速开始（沙盒）

```bash
export MEMORY_DIR=/tmp/mem-test
mkdir -p $MEMORY_DIR/qq/{short,normal,key,permanent,cold}
export DSH_SESSION_ID=qqbot-v2-c2c-demo

python3 ingest.py --text "我讨厌写代码时被人打断"     # high → key
python3 ingest.py --text "周末去城西喝手冲咖啡"        # mid  → short
python3 search.py --query "手冲咖啡"                   # 命中 short
python3 stage.py --segment 1                          # 推一段
python3 scheduler.py --status                         # 看轮次状态
```

**安全闸**：若 `MEMORY_DIR` 落在 `~/.dsh/memory` 之下，所有模块拒绝启动（退出码 2），防止误碰生产库。

## 退出码约定

`0` 成功（含零命中）/ `2` 通道判不明或库路径越界 / `3` 读写失败 / `4` 熔断 / `5` 模型失败（置 degraded）/ `6` 抢锁失败。

## 已知限制

- 检索分词为中文二字切片，会串味（「苹果」命中「苹果手机配件」），靠同义词表与语义重排兜底。
- `scan.py` 的日期解析只认 `X 月 X 号/日` 与 `YYYY-MM-DD`，相对词（月底、下个月）不解析。
- `permanent` 超 1 万条需转持久索引，当前未实现。
- 模型调用（补标签/复核/重排）默认走桩函数，接真实模型需自行接入。
