package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config 是框架级配置，不含业务密钥。
type Config struct {
	Name           string
	SocketPath     string
	MaxFrameBytes  int
	DefaultTimeout int
	MaxTimeoutMs   int
	MaxInflight    int
	ShutdownGrace  int
	Plugins        []string
}

func Load() (Config, error) {
	var err error
	cfg := Config{
		Name:           os.Getenv("SIDECAR_NAME"),
		SocketPath:     os.Getenv("SIDECAR_SOCKET_PATH"),
		MaxFrameBytes:  16 * 1024 * 1024,
		DefaultTimeout: 3000,
		MaxTimeoutMs:   30000,
		MaxInflight:    128,
		ShutdownGrace:  5000,
	}
	if cfg.Plugins, err = SplitCSV(os.Getenv("SIDECAR_PLUGINS")); err != nil {
		return cfg, err
	}
	if cfg.SocketPath == "" || !strings.HasPrefix(cfg.SocketPath, "/") {
		return cfg, fmt.Errorf("SIDECAR_SOCKET_PATH")
	}
	if cfg.MaxFrameBytes, err = envInt("SIDECAR_MAX_FRAME_BYTES", cfg.MaxFrameBytes, 1024, 16*1024*1024); err != nil {
		return cfg, err
	}
	if cfg.MaxTimeoutMs, err = envInt("SIDECAR_MAX_TIMEOUT_MS", cfg.MaxTimeoutMs, 1, 300000); err != nil {
		return cfg, err
	}
	if cfg.DefaultTimeout, err = envInt("SIDECAR_DEFAULT_TIMEOUT_MS", cfg.DefaultTimeout, 1, cfg.MaxTimeoutMs); err != nil {
		return cfg, err
	}
	if cfg.MaxInflight, err = envInt("SIDECAR_MAX_INFLIGHT", cfg.MaxInflight, 1, 1024); err != nil {
		return cfg, err
	}
	if cfg.ShutdownGrace, err = envInt("SIDECAR_SHUTDOWN_GRACE_MS", cfg.ShutdownGrace, 0, 30000); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func envInt(key string, fallback, min, max int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s out of range", key)
	}
	return n, nil
}

// SplitCSV 切开逗号列表。空字符串得到空列表。空段或重复项是配置错误。
func SplitCSV(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			return nil, fmt.Errorf("empty csv item")
		}
		if _, ok := seen[item]; ok {
			return nil, fmt.Errorf("duplicate csv item %s", item)
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out, nil
}
