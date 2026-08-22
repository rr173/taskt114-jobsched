# taskt114-jobsched

持久化延时任务调度服务。为任务（job）提供入队、查询、重试、取消、死信（dead-letter）与递归调度（recurring schedule）能力；支持按队列暂停/恢复、优先级与状态过滤、执行尝试（attempt）记录与运行指标。所有任务、尝试、队列与递归调度定义持久化到 SQLite，进程重启后可完整恢复（中断的 running 任务会被回收为 pending 重新调度）。内置 worker 池以有界并发消费到期任务，失败按退避策略重试，超过最大尝试次数进入死信。

## 主要输入与输出

- 输入：HTTP JSON 请求（`/jobs`、`/schedules`、`/queues` 等），含 `queue`、`type`、`args`、`run_at`、`max_attempts`、`priority`、`interval_seconds` 等字段。
- 输出：JSON 响应（任务定义、状态、结果、尝试列表、队列统计、死信列表、运行指标等）。
- 内置处理器：`noop`（直接成功）、`echo`（返回 args 作为结果）、`fail`（必定失败用于演示重试/死信）、`slow`（可取消的慢任务）。

## 本地命令

```bash
go build ./...                              # 编译
go run . --smoke-test                        # 自检（不依赖外部服务、不依赖真实时间睡眠）
go run .                                    # 启动 HTTP 服务（默认 :8080，SQLite 文件 jobsched.db）
go test ./...                               # 测试
go vet ./...                                # 静态检查
```

## 业务约束

- 任务状态：pending / scheduled / running / succeeded / dead / cancelled。
- 递归调度：按 `interval` 周期重新入队，可启用/停用；最近一次触发时间持久化。
- 并发：worker 池大小为 16，每个到期任务仅被认领一次（原子认领 + 活跃集合去重）。
- 取消：运行中的任务通过 context 传播取消；取消后任务置为 cancelled。

## Docker 构建

构建脚本 `build_benzhi_docker.sh` 接收两个参数：

1. 镜像名（默认 `my-project`）
2. 目标平台（默认 `linux/amd64`）

```bash
# amd64
bash ./build_benzhi_docker.sh go-task-benzhi:amd64 linux/amd64
docker run -it go-task-benzhi:amd64
# arm64
bash ./build_benzhi_docker.sh go-task-benzhi:arm64 linux/arm64
docker run -it go-task-benzhi:arm64
```

进入容器后可用 `go version` 确认工具链版本为 `go1.26.3`。

## 双架构主镜像

主 `Dockerfile` 为多阶段构建（`golang:1.26.3-bookworm` 构建 + `alpine:3.20` 运行，`CGO_ENABLED=0`）：

```bash
docker buildx build --platform linux/amd64 --load -t go-task-check:amd64 .
docker run --rm go-task-check:amd64 --smoke-test
docker buildx build --platform linux/arm64 --load -t go-task-check:arm64 .
docker run --rm go-task-check:arm64 --smoke-test
```

两个容器都必须通过 `--smoke-test` 自检（不依赖外部服务、执行后自行退出）。

## 技术栈

- Go `1.26.3`（`GOTOOLCHAIN=local`）
- SQLite 引擎 `3.46.1`，纯 Go 驱动 `modernc.org/sqlite v1.52.0`（`CGO_ENABLED=0`）
- 依赖下载：`GOPROXY=https://goproxy.cn,direct`、`GOSUMDB=sum.golang.google.cn`
