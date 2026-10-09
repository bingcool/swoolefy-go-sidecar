package health

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

const Version = "0.1.0"

// State 是 health.check 读取的进程状态。不在这里发探测请求。
type State struct {
	Started    time.Time
	Inflight   *atomic.Int32
	ListenerOK *atomic.Bool
}

// Mount 在业务插件 Register 之前挂上 health.check。
func Mount(router plugin.MethodRegistrar, state *State, contributors []plugin.HealthContributor) error {
	return router.Handle("health.check", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		body := map[string]any{
			"version":    Version,
			"uptimeMs":   time.Since(state.Started).Milliseconds(),
			"listenerOk": state.ListenerOK.Load(),
			"inflight":   int(state.Inflight.Load()),
		}
		for _, contributor := range contributors {
			contributor.Contribute(body)
		}
		return plugin.OK(body)
	})
}
