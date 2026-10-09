package ipc

import (
	"fmt"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

// Router 把 method 分给唯一的处理函数。
type Router struct {
	handlers map[string]plugin.Handler
}

func NewRouter() *Router {
	return &Router{handlers: map[string]plugin.Handler{}}
}

func (r *Router) Handle(method string, h plugin.Handler) error {
	if method == "" {
		return fmt.Errorf("empty method")
	}
	if _, exists := r.handlers[method]; exists {
		return fmt.Errorf("duplicate method %s", method)
	}
	r.handlers[method] = h
	return nil
}

func (r *Router) Dispatch(method string) (plugin.Handler, bool) {
	h, ok := r.handlers[method]
	return h, ok
}
