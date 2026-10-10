package framework

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
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

// ListenSocket 监听 UDS，并把文件权限收成 0600。
// 创建时用 umask，避免事后 chmod。Alpine 上挂载目录里的 socket 再 chmod 会得到 EINVAL。
func ListenSocket(path string) (net.Listener, error) {
	old := syscall.Umask(0o077)
	ln, err := net.Listen("unix", path)
	syscall.Umask(old)
	if err != nil {
		return nil, err
	}
	if err = restrictSocket(ln, path); err != nil {
		_ = ln.Close()
		RemoveOwnSocket(path)
		return nil, err
	}
	return ln, nil
}

func restrictSocket(ln net.Listener, path string) error {
	const mode os.FileMode = 0o600
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm() == mode {
		return nil
	}
	if ul, ok := ln.(*net.UnixListener); ok {
		if raw, rawErr := ul.SyscallConn(); rawErr == nil {
			var ferr error
			cerr := raw.Control(func(fd uintptr) {
				ferr = syscall.Fchmod(int(fd), uint32(mode))
			})
			if cerr == nil && ferr == nil {
				return nil
			}
		}
	}
	if err = os.Chmod(path, mode); err != nil {
		info, statErr := os.Lstat(path)
		if statErr == nil && info.Mode().Perm() == mode {
			return nil
		}
		return err
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
