package framework

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"swoolefy-go-sidecar/internal/framework/config"
	"swoolefy-go-sidecar/internal/framework/ipc"
	"swoolefy-go-sidecar/internal/framework/plugin"
)

func TestRunStopsOnSignalAndDeletesSocket(t *testing.T) {
	path := testSock(t, "run")
	_ = os.Remove(path)
	item := &orderPlugin{name: "first"}
	other := &orderPlugin{name: "second"}
	sig := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() {
		done <- RunSignals(config.Config{
			Name:          "rocketmq",
			SocketPath:    path,
			MaxFrameBytes: 65536,
			MaxTimeoutMs:  1000,
			MaxInflight:   4,
			ShutdownGrace: 200,
		}, []plugin.Plugin{item, other}, sig)
	}()
	waitListen(t, path)
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(ipc.Request{RequestID: "h", Method: "health.check", Payload: []byte(`{}`), TimeoutMs: 500})
	if err = ipc.WriteFrame(conn, raw); err != nil {
		t.Fatal(err)
	}
	body, err := ipc.ReadFrame(conn, 65536)
	_ = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.Response
	if err = json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	payload, _ := resp.Payload.(map[string]any)
	if resp.Code != "OK" || payload["version"] != "0.1.0" {
		t.Fatalf("%+v", resp)
	}
	if _, ok := payload["producerReady"]; ok {
		t.Fatal("producerReady must stay absent without the plugin field")
	}
	sig <- syscall.SIGTERM
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown hung")
	}
	if _, err = net.DialTimeout("unix", path, 200*time.Millisecond); err == nil {
		t.Fatal("socket still listening")
	}
	if item.stopped == 0 || other.stopped == 0 {
		t.Fatal("plugin stop was not called")
	}
	if other.stoppedAt > item.stoppedAt && item.stoppedAt != 0 {
		t.Fatal("plugins must stop in reverse order")
	}
}

func TestRunLeavesForeignSocket(t *testing.T) {
	path := testSock(t, "foreign")
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	err = RunSignals(config.Config{
		SocketPath:    path,
		MaxFrameBytes: 1024,
		MaxTimeoutMs:  1000,
		MaxInflight:   1,
	}, nil, nil)
	if err == nil {
		t.Fatal("live socket must fail startup")
	}
	conn, dialErr := net.Dial("unix", path)
	if dialErr != nil {
		t.Fatal(dialErr)
	}
	_ = conn.Close()
}

func TestPrepareRemovesStaleSocketOnly(t *testing.T) {
	path := testSock(t, "stale")
	_ = os.Remove(path)
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		_ = syscall.Close(fd)
		t.Fatal(err)
	}
	_ = syscall.Close(fd)
	if err = PrepareSocket(path); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("stale socket was kept")
	}

	file := testSock(t, "file")
	if err = os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file)
	if err = PrepareSocket(file); err == nil {
		t.Fatal("regular file")
	}
	if _, err = os.Stat(file); err != nil {
		t.Fatal("regular file was deleted")
	}
}

type orderPlugin struct {
	name      string
	stopped   int
	stoppedAt int64
	order     *int64
}

func (p *orderPlugin) Name() string { return p.name }

func (p *orderPlugin) Register(router plugin.MethodRegistrar) error { return nil }

func (p *orderPlugin) Start(ctx context.Context) error { return nil }

func (p *orderPlugin) Stop(ctx context.Context) error {
	p.stopped++
	p.stoppedAt = time.Now().UnixNano()
	time.Sleep(5 * time.Millisecond)
	return nil
}

func (p *orderPlugin) Ready() bool { return true }

func testSock(t *testing.T, name string) string {
	t.Helper()
	dir := "/tmp/swoolefy-sidecar-tests"
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir + "/" + name + ".sock"
}

func waitListen(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", path)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("socket did not start")
}
