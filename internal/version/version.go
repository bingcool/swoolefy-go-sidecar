package version

import (
	"context"
	"encoding/json"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

// Version 是 sidecar 的发布版本。发新版本时只改这里，然后重新编译二进制。
const Version = "1.0.0"

// Mount 注册 sidecar.version。PHP 用它读取 Version，不探测代理。
func Mount(router plugin.MethodRegistrar) error {
	return router.Handle("sidecar.version", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		return plugin.OK(map[string]any{
			"version": Version,
		})
	})
}
