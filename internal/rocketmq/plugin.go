package rocketmq

import (
	"context"
	"encoding/json"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

// Plugin 是第一个业务插件。Producer 和 Consumer 的 SDK 客户端都留在本包。
type Plugin struct {
	cfg      Config
	producer *Producer
	consumer *Consumer
}

func New(cfg Config) *Plugin {
	return &Plugin{
		cfg:      cfg,
		producer: newProducer(cfg),
		consumer: newConsumer(),
	}
}

func NewFromEnv() (*Plugin, error) {
	cfg, err := Load()
	if err != nil {
		return nil, err
	}
	item := New(cfg)
	item.consumer.factory = sdkFactory{cfg: cfg}
	return item, nil
}

func (p *Plugin) Name() string { return "rocketmq" }

func (p *Plugin) Register(router plugin.MethodRegistrar) error {
	if err := router.Handle("rocketmq.producer.send", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		return p.producer.Send(ctx, payload)
	}); err != nil {
		return err
	}
	if err := router.Handle("rocketmq.consumer.receive", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		return p.consumer.Receive(ctx, payload)
	}); err != nil {
		return err
	}
	if err := router.Handle("rocketmq.consumer.ack", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		return p.consumer.Ack(ctx, payload)
	}); err != nil {
		return err
	}
	return router.Handle("rocketmq.consumer.changeInvisible", func(ctx context.Context, payload json.RawMessage) plugin.Result {
		return p.consumer.ChangeInvisible(ctx, payload)
	})
}

// Capabilities 是 health.check 里的能力声明。PHP 用它判断当前 sidecar 是否支持某个字段或方法。
// 只追加，不修改已有字符串的含义。
var Capabilities = []string{
	"send.keys",
	"send.properties",
	"send.deliverAt",
	"send.messageGroup",
	"receive.meta",
	"consumer.changeInvisible",
	"subscribe.sql92",
}

func (p *Plugin) Start(ctx context.Context) error {
	if p.consumer.factory == nil {
		p.consumer.factory = sdkFactory{cfg: p.cfg}
	}
	p.consumer.ready.Store(true)
	if p.producer.sender == nil {
		go p.producer.maintain(ctx)
		return nil
	}
	p.producer.ready.Store(true)
	return nil
}

func (p *Plugin) Stop(ctx context.Context) error {
	err1 := p.producer.Close(ctx)
	err2 := p.consumer.Close(ctx)
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Plugin) Ready() bool {
	return p.producer.ready.Load()
}

func (p *Plugin) Contribute(dst map[string]any) {
	dst["producerReady"] = p.producer.ready.Load()
	dst["sendOk"] = p.producer.sendOK.Load()
	dst["sendFail"] = p.producer.sendFail.Load()
	dst["sendTimeout"] = p.producer.sendWait.Load()
	dst["consumerReady"] = p.consumer.ready.Load()
	dst["capabilities"] = append([]string(nil), Capabilities...)
}
