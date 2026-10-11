# swoolefy-go-sidecar

Swoolefy 的本地 sidecar 进程。PHP 通过 Unix socket 调用它，当前插件是 RocketMQ。

模块语言版本是 Go 1.26。编译使用 Go 1.26，并且 `CGO_ENABLED=0`。RocketMQ SDK 固定在 v5.1.2。

## 技术架构

当前已经使用的只有 RocketMQ。一个 sidecar 声明对应一个本机进程、一个 Unix socket。PHP 不链接 RocketMQ SDK，发送、拉取、确认都在这个进程里完成。Kafka、AMQP、Redis 队列走 PHP，不经过这里。

```text
Swoole Worker / 自定义进程
        |  每次调用新建一条 Unix 连接，写一帧、读一帧、关闭
        v
cmd/sidecar
        |  读 SIDECAR_*，按 SIDECAR_PLUGINS 加载插件
        v
internal/framework          Unix socket、帧、路由、健康检查、退出
        |
        v
internal/rocketmq           唯一已接入的插件
        |  gRPC
        v
RocketMQ Proxy              当前 Test 使用 host:port，例如 192.168.1.103:8081
```

PHP 侧入口是 `Swoolefy\Core\Sidecar`。`EventCtrl::init()` 末尾由 `SidecarSupervisor` 把本二进制 `exec` 成 Swoole user process。业务进程在 `src/Support/Rocketmq`：`RocketmqProducer::send()`，以及两个消费进程基类里的 `receive` / `ack`。框架声明在 `src/Core/Sidecar/Config/SystemSidecar.php`，名称是 `rocketmq`。

### 分层

| 路径 | 已经承担的职责 |
| --- | --- |
| `cmd/sidecar` | 进程入口。配置错误或缺 `SIDECAR_SOCKET_PATH` 时以状态码 1 退出 |
| `internal/framework` | 与业务无关的运行时。不解析 RocketMQ 字段 |
| `internal/framework/ipc` | 长度帧、JSON 信封、方法路由。一条连接只处理一个请求 |
| `internal/framework/health` | 注册 `health.check` |
| `internal/framework/plugin` | 插件接口：`Register`、`Start`、`Stop`、`Ready` |
| `internal/rocketmq` | Producer、按消费组打开的 SimpleConsumer、回执确认 |

`Register` 只登记方法。`listen` 成功之后才 `Start`。Producer 在后台连接代理，连不上进程不退出，该次调用返回 `UNAVAILABLE`。Consumer 不在 `Start` 里拉消息，第一次 `receive` 时按本次的 topic 和 tag 创建客户端。

### 帧和并发

请求和响应都是 4 字节大端长度加上一个 JSON 对象。请求字段是 `requestId`、`method`、`payload`、`timeoutMs`。响应字段是 `requestId`、`code`、`message`、`payload`。

没有连接池，也没有多路复用。并发是 PHP 多个协程各自建连接。同时处理的请求数超过 `SIDECAR_MAX_INFLIGHT` 时返回 `OVERLOADED`。同一个消费组加同一个 topic 上，如果上一次 `receive` 还没结束，第二次也返回 `OVERLOADED`。

收到 `SIGTERM` 或 `SIGINT` 后停止接入，取消在途调用的上下文，等待在途请求最多 `SIDECAR_SHUTDOWN_GRACE_MS`，再停插件。Swoole 会在这个 user process 退出后重新拉起。

### 已经注册的方法

| 方法 | 谁在调用 | 行为 |
| --- | --- | --- |
| `health.check` | PHP `health()` | 返回监听是否就绪，以及 producer / consumer 的 ready 和发送计数 |
| `rocketmq.producer.send` | `RocketmqProducer::send()` | 校验 topic 是否在允许列表内，解码 `bodyBase64`，发给代理。`delayMs` 为 0 是立即投递，大于 0 是相对延迟 |
| `rocketmq.consumer.receive` | 消费进程基类 | 按 `consumerGroup` + `topic` 拉取。`maxNum` 限制在 1–32，`invisibleMs` 限制在 1–300000。空结果返回 `OK` 和空列表 |
| `rocketmq.consumer.ack` | `onMessage()` 成功之后 | 用 `receiptHandle`、`messageId`、`topic` 确认。不在进程里缓存 SDK 的消息对象 |

消息正文进出都是 Base64。Go 日志只记 sidecar 名、方法、`requestId`、`code`、耗时，不记正文和密钥。

代理地址、AccessKey、SecretKey 来自 PHP 传入的 `SIDECAR_ROCKETMQ_*`。端点只接受 `host:port`。Test 里消费进程单独注册，例如 topic `test-demo`，不在 Go 里写死消费循环。

### 这个进程不做的事

- 不自己循环 `receive`。拉多少、何时 `ack`，由 PHP 消费进程决定。`onMessage()` 抛异常时 PHP 不调用 `ack`。
- 不保存已拉取的消息。确认只带回收据字段。
- 字节已经写出之后如果结果不明，PHP 抛 `OutcomeUnknownException`。`send` 不会自动再发一次。
- 代理不可用时不退出。返回 `UNAVAILABLE` 并自行退避后重连。

在本目录执行。产物放到 `swoolefy/src/Core/Sidecar/Bin`。框架按启动 swoolefy 的系统选择二进制：macOS 用 `go-sidecar-mac`，Linux 用 `go-sidecar-linux`。

```bash
go version
# 需要 go1.26。
```

## macOS（go-sidecar-mac）

当前这份是 Apple Silicon，`darwin/arm64`，产物是 `Mach-O 64-bit executable arm64`。

```bash
GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 \
  go build -trimpath -ldflags "-s -w" \
  -o ../swoolefy/src/Core/Sidecar/Bin/go-sidecar-mac ./cmd/sidecar
file ../swoolefy/src/Core/Sidecar/Bin/go-sidecar-mac
```

苹果M系列芯片的GOARCH=arm64，Intel系列芯片的GOARCH=amd64，仍输出到同一个文件名。

## Linux（go-sidecar-linux）

当前这份是 amd64，静态链接的 `ELF 64-bit LSB executable, x86-64`。不链接 glibc，也不链接 musl。不要用 Go 1.20 编译，较新的内核开了 5 级页表时会在启动时崩溃：`fatal error: lfstack.push`。

```bash
rm -f ../swoolefy/src/Core/Sidecar/Bin/go-sidecar-linux
GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags "-s -w" \
  -o ../swoolefy/src/Core/Sidecar/Bin/go-sidecar-linux ./cmd/sidecar
go version -m ../swoolefy/src/Core/Sidecar/Bin/go-sidecar-linux
upx -9 ../swoolefy/src/Core/Sidecar/Bin/go-sidecar-linux
file ../swoolefy/src/Core/Sidecar/Bin/go-sidecar-linux
```

Linux arm64 把 `GOARCH` 换成 `arm64`，仍输出到同一个文件名。`ldd` 应提示不是动态可执行文件。`go version -m` 要在 `upx` 之前执行，第一行必须是 `go1.26`。压缩后再执行 `go version -m` 读不到构建信息。
