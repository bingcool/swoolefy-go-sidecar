package plugin

import (
	"context"
	"encoding/json"
)

// Handler 处理一帧里的 payload。ctx 的截止时间已经按 timeoutMs 截断。
type Handler func(ctx context.Context, payload json.RawMessage) Result

// Result 是一次方法调用的结果。Payload 必须能编码成 JSON 对象。
type Result struct {
	Code    string
	Message string
	Payload any
}

func OK(payload any) Result {
	if payload == nil {
		payload = map[string]any{}
	}
	return Result{Code: "OK", Message: "", Payload: payload}
}

func Fail(code, message string) Result {
	return Result{Code: code, Message: message, Payload: map[string]any{}}
}

// Plugin 是一个业务插件。Register 只登记方法，Start 才连接外部系统。
type Plugin interface {
	Name() string
	Register(router MethodRegistrar) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Ready() bool
}

// MethodRegistrar 避免插件依赖 ipc 包的具体类型，从而打断循环引用。
type MethodRegistrar interface {
	Handle(method string, h Handler) error
}

// HealthContributor 往 health.check 里追加字段。没有的键不要写。
type HealthContributor interface {
	Contribute(dst map[string]any)
}
