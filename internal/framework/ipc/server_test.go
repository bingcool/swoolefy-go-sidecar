package ipc

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

func TestFrameRejectsEmptyAndOversize(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, nil); err == nil {
		t.Fatal("empty frame")
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{0, 0, 0, 0}), 16); err == nil {
		t.Fatal("zero length")
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{0, 0, 0, 20}), 8); err == nil {
		t.Fatal("oversize")
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{0, 0}), 16); err == nil {
		t.Fatal("short header")
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	_, err := DecodeRequest([]byte(`{"requestId":"a","method":"health.check","payload":{},"timeoutMs":1,"extra":1}`))
	if err == nil {
		t.Fatal("unknown field")
	}
}

func TestServerMethodNotFoundAndEmptyMethod(t *testing.T) {
	ln := listen(t)
	defer ln.Close()
	start(t, ln, &Server{
		Name:          "test",
		MaxFrameBytes: 65536,
		MaxTimeoutMs:  1000,
		MaxInflight:   4,
		Inflight:      &atomic.Int32{},
		Router:        NewRouter(),
		Root:          context.Background(),
	})
	missing := roundTrip(t, ln.Addr().String(), Request{RequestID: "a", Method: "missing", Payload: []byte(`{}`), TimeoutMs: 200})
	if missing.Code != "METHOD_NOT_FOUND" || missing.RequestID != "a" {
		t.Fatalf("%+v", missing)
	}
	empty := roundTrip(t, ln.Addr().String(), Request{RequestID: "b", Method: "", Payload: []byte(`{}`), TimeoutMs: 200})
	if empty.Code != "INVALID_ARGUMENT" {
		t.Fatalf("%+v", empty)
	}
}

func TestServerTruncatesTimeout(t *testing.T) {
	ln := listen(t)
	defer ln.Close()
	router := NewRouter()
	var got time.Duration
	if err := router.Handle("demo.wait", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("missing deadline")
			return plugin.OK(nil)
		}
		got = time.Until(deadline)
		return plugin.OK(map[string]any{})
	}); err != nil {
		t.Fatal(err)
	}
	start(t, ln, &Server{
		Name:          "test",
		MaxFrameBytes: 65536,
		MaxTimeoutMs:  300,
		MaxInflight:   2,
		Inflight:      &atomic.Int32{},
		Router:        router,
		Root:          context.Background(),
	})
	resp := roundTrip(t, ln.Addr().String(), Request{RequestID: "t", Method: "demo.wait", Payload: []byte(`{}`), TimeoutMs: 60000})
	if resp.Code != "OK" {
		t.Fatal(resp.Code)
	}
	if got > 400*time.Millisecond || got < 50*time.Millisecond {
		t.Fatalf("deadline %s", got)
	}
}

func TestServerOverloaded(t *testing.T) {
	ln := listen(t)
	defer ln.Close()
	router := NewRouter()
	entered := make(chan struct{})
	release := make(chan struct{})
	if err := router.Handle("demo.block", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		close(entered)
		<-release
		return plugin.OK(nil)
	}); err != nil {
		t.Fatal(err)
	}
	start(t, ln, &Server{
		Name:          "test",
		MaxFrameBytes: 65536,
		MaxTimeoutMs:  2000,
		MaxInflight:   1,
		Inflight:      &atomic.Int32{},
		Router:        router,
		Root:          context.Background(),
	})
	firstDone := make(chan Response, 1)
	go func() {
		firstDone <- roundTrip(t, ln.Addr().String(), Request{RequestID: "1", Method: "demo.block", Payload: []byte(`{}`), TimeoutMs: 1500})
	}()
	<-entered
	second := roundTrip(t, ln.Addr().String(), Request{RequestID: "2", Method: "demo.block", Payload: []byte(`{}`), TimeoutMs: 500})
	if second.Code != "OVERLOADED" {
		t.Fatalf("%+v", second)
	}
	close(release)
	first := <-firstDone
	if first.Code != "OK" {
		t.Fatalf("%+v", first)
	}
}

func TestDuplicateMethod(t *testing.T) {
	router := NewRouter()
	if err := router.Handle("health.check", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		return plugin.OK(nil)
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.Handle("health.check", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		return plugin.OK(nil)
	}); err == nil {
		t.Fatal("duplicate method")
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("unix", tempSock(t))
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func start(t *testing.T, ln net.Listener, srv *Server) {
	t.Helper()
	go srv.Serve(ln)
}

func roundTrip(t *testing.T, path string, req Request) Response {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if err = WriteFrame(conn, raw); err != nil {
		t.Fatal(err)
	}
	body, err := ReadFrame(conn, 65536)
	if err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err = json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func tempSock(t *testing.T) string {
	t.Helper()
	dir := "/tmp/swoolefy-sidecar-tests"
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir + "/" + t.Name() + ".sock"
}
