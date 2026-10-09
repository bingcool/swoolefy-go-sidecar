package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"time"

	sidelog "swoolefy-go-sidecar/internal/framework/log"
	"swoolefy-go-sidecar/internal/framework/plugin"
)

// Server 一条连接只处理一个请求，写完响应就关闭。
type Server struct {
	Name          string
	MaxFrameBytes int
	MaxTimeoutMs  uint32
	MaxInflight   int
	Inflight      *atomic.Int32
	Router        *Router
	Root          context.Context
}

func (s *Server) Serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || s.Root.Err() != nil {
				return
			}
			sidelog.Error(s.Name, "accept")
			continue
		}
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	now := s.Inflight.Add(1)
	defer s.Inflight.Add(-1)
	started := time.Now()
	if int(now) > s.MaxInflight {
		_ = writeResult(conn, "", plugin.Fail("OVERLOADED", "inflight limit"))
		sidelog.Access(s.Name, "", "", "OVERLOADED", time.Since(started).Milliseconds())
		return
	}
	_ = conn.SetDeadline(time.Now().Add(time.Duration(s.MaxTimeoutMs) * time.Millisecond))
	body, err := ReadFrame(conn, s.MaxFrameBytes)
	if err != nil {
		sidelog.Access(s.Name, "", "", "FRAME", time.Since(started).Milliseconds())
		return
	}
	req, err := DecodeRequest(body)
	if err != nil {
		_ = writeResult(conn, "", plugin.Fail("INVALID_ARGUMENT", "bad request"))
		sidelog.Access(s.Name, "", "", "INVALID_ARGUMENT", time.Since(started).Milliseconds())
		return
	}
	if req.Method == "" {
		_ = writeResult(conn, req.RequestID, plugin.Fail("INVALID_ARGUMENT", "method"))
		sidelog.Access(s.Name, "", req.RequestID, "INVALID_ARGUMENT", time.Since(started).Milliseconds())
		return
	}
	timeout := req.TimeoutMs
	if timeout == 0 || timeout > s.MaxTimeoutMs {
		timeout = s.MaxTimeoutMs
	}
	ctx, cancel := context.WithTimeout(s.Root, time.Duration(timeout)*time.Millisecond)
	defer cancel()
	handler, ok := s.Router.Dispatch(req.Method)
	var result plugin.Result
	if !ok {
		result = plugin.Fail("METHOD_NOT_FOUND", req.Method)
	} else {
		result = handler(ctx, req.Payload)
	}
	if !KnownCode(result.Code) {
		result = plugin.Fail("INTERNAL", "bad code")
	}
	if err := writeResult(conn, req.RequestID, result); err != nil {
		sidelog.Access(s.Name, req.Method, req.RequestID, "WRITE", time.Since(started).Milliseconds())
		return
	}
	sidelog.Access(s.Name, req.Method, req.RequestID, result.Code, time.Since(started).Milliseconds())
}

func writeResult(conn net.Conn, requestID string, result plugin.Result) error {
	if result.Payload == nil {
		result.Payload = map[string]any{}
	}
	raw, err := json.Marshal(Response{
		RequestID: requestID,
		Code:      result.Code,
		Message:   result.Message,
		Payload:   result.Payload,
	})
	if err != nil {
		return err
	}
	return WriteFrame(conn, raw)
}
