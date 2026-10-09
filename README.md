# swoolefy-go-sidecar

Swoolefy 的本地 sidecar 进程。PHP 通过 Unix socket 调用它，当前插件是 RocketMQ。

swoolefy 和这支二进制都跑在 Alpine Linux 上，通常是 Docker。Alpine 用 musl，容器里的内核仍是宿主机内核。

源码按 Go 1.20 编写。编译必须使用 Go 1.21 或更新的工具链，并且 `CGO_ENABLED=0`。这样产物是静态 ELF，不链接 glibc，也不链接 musl，可以直接放进 Alpine。Go 1.20 的运行时不支持 5 级页表；宿主机内核开了 5 级页表时，Alpine 容器里一样会在启动时崩溃：`fatal error: lfstack.push`。不要改用 RocketMQ SDK 的更新版本，那些版本要求 Go 1.24 语言版本。

## 生成 Linux 二进制

在本目录执行。可以在 macOS 上交叉编译，也可以在 Alpine 容器里编译。

这台 Mac 的 `/usr/local/go/bin/go` 是 **1.20.7**。直接执行下面的 `go build` 会再次编出会崩溃的二进制。先看版本，低于 1.21 就换工具链：

```bash
go version
# 需要 go1.21 或更高。go1.20.7 不能用。
```

Alpine / Linux amd64：

```bash
case "$(go env GOVERSION)" in
  go1.2[1-9]*|go1.[3-9]*|go[2-9]*) ;;
  *) echo "need Go >= 1.21, have $(go env GOVERSION)"; exit 1 ;;
esac
GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags "-s -w" -o go-sidecar ./cmd/sidecar
go version -m go-sidecar | head -n 1
```

`go version -m` 的第一行必须是 `go1.21` 或更高。若仍是 `go1.20.7`，Alpine 上会再次出现 `lfstack.push`。

Linux arm64：

```bash
GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -ldflags "-s -w" -o go-sidecar ./cmd/sidecar
```

确认产物是 Linux ELF，而不是当前系统的可执行文件：

```bash
file go-sidecar
```

amd64 应显示 `ELF 64-bit LSB executable, x86-64` 和 `statically linked`。arm64 应显示 `ARM aarch64`。在 Alpine 里 `ldd go-sidecar` 应提示不是动态可执行文件。

仓库里给 swoolefy 用的文件放在上一级目录，文件名是 `go-sidecar`：

```bash
case "$(go env GOVERSION)" in
  go1.2[1-9]*|go1.[3-9]*|go[2-9]*) ;;
  *) echo "need Go >= 1.21, have $(go env GOVERSION)"; exit 1 ;;
esac
GOPROXY=https://goproxy.cn,direct CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags "-s -w" \
  -o ../swoolefy/go-sidecar ./cmd/sidecar
go version -m ../swoolefy/go-sidecar | head -n 1
```

PHP 配置 `Config/sidecar.php` 的 `sidecar_exec_binary` 指向这支文件，或把它拷到默认路径 `/opt/swoolefy/bin/rocketmq-sidecar`。
