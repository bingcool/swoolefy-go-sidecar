package framework

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"swoolefy-go-sidecar/internal/framework/config"
	"swoolefy-go-sidecar/internal/framework/health"
	"swoolefy-go-sidecar/internal/framework/ipc"
	sidelog "swoolefy-go-sidecar/internal/framework/log"
	"swoolefy-go-sidecar/internal/framework/plugin"
)

// Run 监听 UDS，注册插件，并在 SIGTERM 后按固定顺序退出。
func Run(cfg config.Config, plugins []plugin.Plugin) error {
	return RunSignals(cfg, plugins, nil)
}

// RunSignals 允许测试传入自己的信号通道。sig 为 nil 时监听 SIGTERM 和 SIGINT。
func RunSignals(cfg config.Config, plugins []plugin.Plugin, sig <-chan os.Signal) error {
	router := ipc.NewRouter()
	state := &health.State{
		Started:    time.Now(),
		Inflight:   &atomic.Int32{},
		ListenerOK: &atomic.Bool{},
	}
	var contributors []plugin.HealthContributor
	for _, item := range plugins {
		if contributor, ok := item.(plugin.HealthContributor); ok {
			contributors = append(contributors, contributor)
		}
	}
	if err := health.Mount(router, state, contributors); err != nil {
		return err
	}
	for _, item := range plugins {
		if err := item.Register(router); err != nil {
			return err
		}
	}
	if err := PrepareSocket(cfg.SocketPath); err != nil {
		return err
	}
	ln, err := ListenSocket(cfg.SocketPath)
	if err != nil {
		return err
	}
	listened := true
	defer func() {
		if listened {
			_ = ln.Close()
		}
		RemoveOwnSocket(cfg.SocketPath)
	}()
	state.ListenerOK.Store(true)

	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, item := range plugins {
		if err := item.Start(root); err != nil {
			sidelog.Error(cfg.Name, "plugin "+item.Name()+" start")
		}
	}
	srv := &ipc.Server{
		Name:          cfg.Name,
		MaxFrameBytes: cfg.MaxFrameBytes,
		MaxTimeoutMs:  uint32(cfg.MaxTimeoutMs),
		MaxInflight:   cfg.MaxInflight,
		Inflight:      state.Inflight,
		Router:        router,
		Root:          root,
	}
	serveDone := make(chan struct{})
	go func() {
		srv.Serve(ln)
		close(serveDone)
	}()

	if sig == nil {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		defer signal.Stop(ch)
		sig = ch
	}
	<-sig
	state.ListenerOK.Store(false)
	cancel()
	listened = false
	_ = ln.Close()
	<-serveDone
	waitInflight(state.Inflight, time.Duration(cfg.ShutdownGrace)*time.Millisecond)
	stopPlugins(plugins)
	return nil
}

func waitInflight(inflight *atomic.Int32, grace time.Duration) {
	if grace < 0 {
		grace = 0
	}
	deadline := time.Now().Add(grace)
	for inflight.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

func stopPlugins(plugins []plugin.Plugin) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := len(plugins) - 1; i >= 0; i-- {
		if err := plugins[i].Stop(ctx); err != nil {
			sidelog.Error("", fmt.Sprintf("plugin %s stop", plugins[i].Name()))
		}
	}
}
