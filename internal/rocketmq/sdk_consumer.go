package rocketmq

import (
	"context"
	"errors"
	"strings"
	"time"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

type sdkFactory struct {
	cfg Config
}

func (f sdkFactory) Open(group string) (groupClient, error) {
	return &sdkGroup{
		cfg:   f.cfg,
		group: group,
		subs:  map[string]string{},
	}, nil
}

// sdkGroup 在返回前把 MessageView 抄成 inbound，不把视图放进 map。
// v5.1.2 的 SimpleConsumer 只在 Start 前读取订阅。先 Start 再 Subscribe 时，
// Proxy 的 client settings 里 subscription 为空，receive 会空指针。
type sdkGroup struct {
	cfg      Config
	group    string
	consumer golang.SimpleConsumer
	subs     map[string]string
}

func (g *sdkGroup) Subscribe(topic, tag string) error {
	expr := "*"
	if tag != "" && tag != "*" {
		expr = tag
	}
	if prev, ok := g.subs[topic]; ok && prev == expr && g.consumer != nil {
		return nil
	}
	g.subs[topic] = expr
	return g.start()
}

func (g *sdkGroup) start() error {
	if g.consumer != nil {
		_ = g.consumer.GracefulStop()
		g.consumer = nil
	}
	expressions := make(map[string]*golang.FilterExpression, len(g.subs))
	for topic, expr := range g.subs {
		if expr == "*" {
			expressions[topic] = golang.SUB_ALL
		} else {
			expressions[topic] = golang.NewFilterExpression(expr)
		}
	}
	cfg := sdkConfig(g.cfg)
	cfg.ConsumerGroup = g.group
	consumer, err := golang.NewSimpleConsumer(cfg,
		golang.WithAwaitDuration(15*time.Second),
		golang.WithSubscriptionExpressions(expressions),
	)
	if err != nil {
		return err
	}
	if err = consumer.Start(); err != nil {
		_ = consumer.GracefulStop()
		return err
	}
	g.consumer = consumer
	return nil
}

func (g *sdkGroup) Receive(ctx context.Context, maxNum int32, invisible time.Duration) ([]inbound, error) {
	if g.consumer == nil {
		return nil, errNotReady
	}
	views, err := g.consumer.Receive(ctx, maxNum, invisible)
	if err != nil {
		return nil, err
	}
	out := make([]inbound, 0, len(views))
	for _, view := range views {
		tag := ""
		if value := view.GetTag(); value != nil {
			tag = *value
		}
		out = append(out, inbound{
			MessageID:       view.GetMessageId(),
			Topic:           view.GetTopic(),
			Body:            append([]byte(nil), view.GetBody()...),
			Tag:             tag,
			Keys:            append([]string(nil), view.GetKeys()...),
			ReceiptHandle:   view.GetReceiptHandle(),
			DeliveryAttempt: int(view.GetDeliveryAttempt()),
		})
	}
	return out, nil
}

func (g *sdkGroup) Ack(ctx context.Context, receiptHandle, messageID, topic string) error {
	return ackReceipt(ctx, g.consumer, g.group, "", receiptHandle, messageID, topic)
}

func (g *sdkGroup) Close(ctx context.Context) error {
	_ = ctx
	if g.consumer == nil {
		return nil
	}
	return g.consumer.GracefulStop()
}

func isEmptyReceive(err error) bool {
	var rpc *golang.ErrRpcStatus
	if errors.As(err, &rpc) && rpc.Code == int32(v2.Code_MESSAGE_NOT_FOUND) {
		return true
	}
	return strings.Contains(err.Error(), "DEADLINE_EXCEEDED")
}
