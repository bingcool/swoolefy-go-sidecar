package rocketmq

import (
	"context"
	"time"

	golang "github.com/apache/rocketmq-clients/golang/v5"
)

type sdkFactory struct {
	cfg Config
}

func (f sdkFactory) Open(group string) (groupClient, error) {
	cfg := sdkConfig(f.cfg)
	cfg.ConsumerGroup = group
	consumer, err := golang.NewSimpleConsumer(cfg)
	if err != nil {
		return nil, err
	}
	if err = consumer.Start(); err != nil {
		_ = consumer.GracefulStop()
		return nil, err
	}
	return &sdkGroup{consumer: consumer}, nil
}

// sdkGroup 在返回前把 MessageView 抄成 inbound，不把视图放进 map。
type sdkGroup struct {
	consumer golang.SimpleConsumer
}

func (g *sdkGroup) Subscribe(topic, tag string) error {
	filter := golang.SUB_ALL
	if tag != "" && tag != "*" {
		filter = golang.NewFilterExpression(tag)
	}
	return g.consumer.Subscribe(topic, filter)
}

func (g *sdkGroup) Receive(ctx context.Context, maxNum int32, invisible time.Duration) ([]inbound, error) {
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
	return ackReceipt(ctx, g.consumer, receiptHandle, messageID, topic)
}

func (g *sdkGroup) Close(ctx context.Context) error {
	_ = ctx
	return g.consumer.GracefulStop()
}
