package framework

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

// PrepareSocket 与 PHP SocketEndpoint::prepare 使用同一条规则。
// 只有无人监听的 socket 残留才会删除。普通文件、目录、符号链接或仍在监听的路径直接失败。
func PrepareSocket(path string) error {
	if path == "" || !filepath.IsAbs(path) {
		return fmt.Errorf("sidecar socket path must be an absolute path")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("cannot create sidecar socket directory")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return fmt.Errorf("cannot chmod sidecar socket directory")
	}
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("sidecar socket path exists and is not a stale socket: %s", path)
	}
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("sidecar socket is already listening: %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("cannot unlink stale sidecar socket: %s", path)
	}
	return nil
}

// RemoveOwnSocket 只删除本进程留下的 socket，不跟随符号链接。
func RemoveOwnSocket(path string) {
	info, err := os.Lstat(path)
	if err != nil {
		return
	}
	if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
		return
	}
	_ = os.Remove(path)
}
