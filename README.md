# PySched — 基础分布式任务调度系统

一个最小可用的分布式任务调度系统，只包含核心的**调度、执行、容错**能力，无前端页面、无权限管理。

- **调度中心（scheduler）**：FastAPI 服务，负责任务 CRUD、计划扫描、派发、节点与运行记录管理。支持多实例同时运行。
- **执行器（worker）**：独立 FastAPI 服务，启动后注册并定时心跳，异步执行任务，超时/结束后回调调度中心。
- **存储**：单个 SQLite 文件（WAL 模式），保存任务配置、节点状态、运行记录。

## 功能清单

- 任务 HTTP 接口：增删改查、暂停/恢复、手动触发
- 两种触发方式：5 段式 cron 表达式、固定间隔（秒）
- 任务可配参数 `params`、超时 `timeout_seconds`、最大重试次数 `max_retries`
- 执行器注册 + 心跳，心跳超时自动标记离线
- 多调度中心实例并发扫描，靠数据库写锁 + `UNIQUE(task_id, scheduled_time)` 保证同一计划时间点只派发一次
- 轮询（round-robin）在线执行器派发，连接失败自动换下一台
- 执行器异步执行，失败按配置在**同一条运行记录**上自动重试，超时单独标记 `timeout`
- 三个内置样例处理器：`echo`（正常打印）、`flaky`（先失败后成功）、`slow`（模拟超时）

## 环境要求

- Python 3.14（代码同时兼容 3.11+）
- Poetry 2.2.1+

## 安装

```bash
poetry install
```

## 本地启动（2 个调度中心 + 2 个执行器）

所有调度中心共用同一个 SQLite 文件，分别监听不同端口：

```bash
# 终端 1：调度中心实例 A
poetry run pysched-scheduler --port 8001 --db pysched.db

# 终端 2：调度中心实例 B（同一个 db 文件）
poetry run pysched-scheduler --port 8002 --db pysched.db

# 终端 3：执行器 worker-1，同时向两个调度中心注册（--scheduler-url 可重复传入）
poetry run pysched-worker --name worker-1 --port 9001 \
  --scheduler-url http://127.0.0.1:8001 \
  --scheduler-url http://127.0.0.1:8002

# 终端 4：执行器 worker-2
poetry run pysched-worker --name worker-2 --port 9002 \
  --scheduler-url http://127.0.0.1:8001 \
  --scheduler-url http://127.0.0.1:8002
```

常用可调参数：`--heartbeat-timeout`（默认 15 秒）、`--scan-interval`（默认 1 秒）、
worker 侧 `--heartbeat-interval`（默认 5 秒）。

## 验收流程示例

### 1. 确认节点在线

```bash
curl http://127.0.0.1:8001/workers
# {"heartbeat_timeout_seconds":15.0,"workers":[{"name":"worker-1","status":"online",...}, ...]}
```

### 2. 创建每 5 秒执行一次的样例任务

```bash
curl -X POST http://127.0.0.1:8001/tasks -H "Content-Type: application/json" -d '{
  "name": "demo-echo",
  "handler": "echo",
  "trigger_type": "interval",
  "interval_seconds": 5,
  "params": {"message": "hello pysched"},
  "timeout_seconds": 30,
  "max_retries": 0
}'
```

等十几秒后查运行记录——每个计划时间点只有一条记录，能看到执行器名字和输出：

```bash
curl "http://127.0.0.1:8001/runs?limit=20"
```

### 3. 暂停 / 恢复

```bash
curl -X POST http://127.0.0.1:8001/tasks/1/pause
curl -X POST http://127.0.0.1:8001/tasks/1/resume
```

### 4. 失败重试（第一次失败、第二次成功）

```bash
curl -X POST http://127.0.0.1:8001/tasks -H "Content-Type: application/json" -d '{
  "name": "demo-flaky",
  "handler": "flaky",
  "trigger_type": "interval",
  "interval_seconds": 3600,
  "params": {"fail_times": 1},
  "timeout_seconds": 30,
  "max_retries": 2
}'
curl -X POST http://127.0.0.1:8001/tasks/2/trigger
curl http://127.0.0.1:8001/runs/1   # attempts_log 里先 false 后 true，最终 succeeded
```

### 5. 超时任务

```bash
curl -X POST http://127.0.0.1:8001/tasks -H "Content-Type: application/json" -d '{
  "name": "demo-slow",
  "handler": "slow",
  "trigger_type": "interval",
  "interval_seconds": 3600,
  "params": {"sleep_seconds": 30},
  "timeout_seconds": 2,
  "max_retries": 0
}'
```

### 6. 故障转移

停掉 worker-1 进程，等待心跳超时后 `GET /workers` 中其 `status` 变为 `offline`；
即使在超时窗口内，调度中心连接 worker-1 失败也会立即改派 worker-2。

## HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 创建任务 |
| GET | `/tasks` | 任务列表 |
| GET | `/tasks/{id}` | 任务详情 |
| PUT | `/tasks/{id}` | 更新任务（部分字段） |
| DELETE | `/tasks/{id}` | 删除任务 |
| POST | `/tasks/{id}/pause` | 暂停 |
| POST | `/tasks/{id}/resume` | 恢复 |
| POST | `/tasks/{id}/trigger` | 手动触发（可覆盖 params） |
| POST | `/workers/register` | worker 注册/心跳 |
| GET | `/workers` | 节点列表（含 online/offline） |
| GET | `/runs` | 运行历史（可按 task_id 过滤） |
| GET | `/runs/{id}` | 运行详情（含 attempts_log） |
| POST | `/runs/{id}/callback` | worker 结果回调 |
| POST | `/scan` | 立即执行一轮扫描（调试用） |

运行记录 `status` 取值：`pending`（待派发/等待重试）→ `running` → `succeeded` /
`failed` / `timeout`。

## 运行测试

```bash
poetry run pytest
```

测试覆盖：cron 解析、数据库唯一约束与重试状态机、任务 CRUD/校验/暂停恢复、
端到端派发（单时间点单记录）、失败重试、超时、worker 故障转移、心跳离线判定，
以及双调度中心实例下的去重派发。

## 项目结构

```
pysched/
  cli.py         # 命令行入口
  cron.py        # 5 段 cron 解析
  db.py          # SQLite 表结构与并发安全的数据访问层
  handlers.py    # echo / flaky / slow 三个内置处理器
  scanner.py     # 调度扫描、派发、轮询故障转移、卡死回收
  scheduler.py   # 调度中心 FastAPI 应用
  timeutil.py    # UTC 时间工具
  worker.py      # 执行器 FastAPI 应用与任务监督
tests/           # pytest 测试
```
