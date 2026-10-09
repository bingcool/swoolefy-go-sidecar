package config

import "testing"

func TestLoadDefaultsAndRange(t *testing.T) {
	t.Setenv("SIDECAR_SOCKET_PATH", "/tmp/sidecar.sock")
	t.Setenv("SIDECAR_PLUGINS", "rocketmq")
	t.Setenv("SIDECAR_MAX_TIMEOUT_MS", "1000")
	t.Setenv("SIDECAR_DEFAULT_TIMEOUT_MS", "1000")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxTimeoutMs != 1000 || cfg.MaxInflight != 128 || len(cfg.Plugins) != 1 {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadRejectsRelativeSocketAndDuplicatePlugin(t *testing.T) {
	t.Setenv("SIDECAR_SOCKET_PATH", "relative.sock")
	if _, err := Load(); err == nil {
		t.Fatal("relative socket")
	}
	t.Setenv("SIDECAR_SOCKET_PATH", "/tmp/sidecar.sock")
	t.Setenv("SIDECAR_PLUGINS", "rocketmq,rocketmq")
	if _, err := Load(); err == nil {
		t.Fatal("duplicate plugin")
	}
	t.Setenv("SIDECAR_PLUGINS", "rocketmq,,ai")
	if _, err := Load(); err == nil {
		t.Fatal("empty segment")
	}
}

func TestLoadRejectsTimeoutOutOfRange(t *testing.T) {
	t.Setenv("SIDECAR_SOCKET_PATH", "/tmp/sidecar.sock")
	t.Setenv("SIDECAR_MAX_TIMEOUT_MS", "0")
	if _, err := Load(); err == nil {
		t.Fatal("zero timeout")
	}
}
