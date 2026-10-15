package rocketmq

import (
	"context"
	"errors"
	"strings"
	"time"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// awaitDuration 加上 SDK 默认的 3 秒请求超时，是这一次 Receive 的上下文截止时间。
// Proxy 5.x 在请求里没有 longPollingTimeout 时，用「剩余时间 - requestTimeout/2」做长轮询，
// 并且不会短于 5 秒。15 秒会把空队列挂到约 16.5 秒。4 秒把长轮询压到这个下限附近。
// 选中的队列里已经有消息时，broker 会马上返回，不会等满这一轮。
const awaitDuration = 5 * time.Second

type sdkFactory struct {
	cfg Config
}

func (f sdkFactory) Open(group string) (groupClient, error) {
	return &sdkGroup{
		cfg:   f.cfg,
		group: group,
		subs:  map[string]subscription{},
	}, nil
}

// sdkGroup 在返回前把 MessageView 抄成 inbound，不把视图放进 map。
// v5.1.2 的 SimpleConsumer 只在 Start 前读取订阅。先 Start 再 Subscribe 时，
// Proxy 的 client settings 里 subscription 为空，receive 会空指针。
type sdkGroup struct {
	cfg      Config
	group    string
	consumer golang.SimpleConsumer
	subs     map[string]subscription
}

// Subscribe 加入或替换一个 Topic 的过滤条件并重启 SimpleConsumer。
// 启动失败时撤回这次改动：组内所有 Topic 共用一个客户端，一条非法 SQL 不能让其他 Topic 也起不来。
func (g *sdkGroup) Subscribe(topic string, sub subscription) error {
	prev, had := g.subs[topic]
	if had && prev == sub && g.consumer != nil {
		return nil
	}
	g.subs[topic] = sub
	err := g.start()
	if err == nil {
		return nil
	}
	if had {
		g.subs[topic] = prev
	} else {
		delete(g.subs, topic)
	}
	if len(g.subs) > 0 {
		_ = g.start()
	}
	return err
}

func filterExpression(sub subscription) *golang.FilterExpression {
	if sub.Kind == filterSQL92 {
		return golang.NewFilterExpressionWithType(sub.Expression, golang.SQL92)
	}
	if sub.Expression == "" || sub.Expression == "*" {
		return golang.SUB_ALL
	}
	return golang.NewFilterExpression(sub.Expression)
}

func (g *sdkGroup) start() error {
	if g.consumer != nil {
		_ = g.consumer.GracefulStop()
		g.consumer = nil
	}
	expressions := make(map[string]*golang.FilterExpression, len(g.subs))
	for topic, sub := range g.subs {
		expressions[topic] = filterExpression(sub)
	}
	cfg := sdkConfig(g.cfg)
	cfg.ConsumerGroup = g.group
	consumer, err := golang.NewSimpleConsumer(cfg,
		golang.WithAwaitDuration(awaitDuration),
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
		group := ""
		if value := view.GetMessageGroup(); value != nil {
			group = *value
		}
		var bornAt, deliverAt time.Time
		if value := view.GetBornTimestamp(); value != nil {
			bornAt = *value
		}
		if value := view.GetDeliveryTimestamp(); value != nil {
			deliverAt = *value
		}
		props := make(map[string]string, len(view.GetProperties()))
		for key, value := range view.GetProperties() {
			props[key] = value
		}
		out = append(out, inbound{
			MessageID:       view.GetMessageId(),
			Topic:           view.GetTopic(),
			Body:            append([]byte(nil), view.GetBody()...),
			Tag:             tag,
			Keys:            append([]string(nil), view.GetKeys()...),
			Properties:      props,
			MessageGroup:    group,
			BornAt:          bornAt,
			DeliverAt:       deliverAt,
			ReceiptHandle:   view.GetReceiptHandle(),
			DeliveryAttempt: int(view.GetDeliveryAttempt()),
		})
	}
	return out, nil
}

func (g *sdkGroup) Ack(ctx context.Context, receiptHandle, messageID, topic string) error {
	return ackReceipt(ctx, g.consumer, g.group, "", receiptHandle, messageID, topic)
}

func (g *sdkGroup) ChangeInvisible(ctx context.Context, receiptHandle, messageID, topic string, invisible time.Duration) (string, error) {
	return changeInvisibleReceipt(ctx, g.consumer, g.group, "", receiptHandle, messageID, topic, invisible)
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
