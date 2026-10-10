# swoolefy-go-sidecar

Swoolefy 的本地 sidecar 进程。PHP 通过 Unix socket 调用它，当前插件是 RocketMQ。

模块语言版本是 Go 1.26。编译使用 Go 1.26，并且 `CGO_ENABLED=0`。RocketMQ SDK 固定在 v5.1.2。

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

Intel Mac 把 `GOARCH` 换成 `amd64`，仍输出到同一个文件名。

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
