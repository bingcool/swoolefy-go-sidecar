package rocketmq

import (
	"context"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

type sdkSender struct {
	producer golang.Producer
}

func openSDKProducer(ctx context.Context, cfg Config) (Sender, error) {
	opts := []golang.ProducerOption{}
	if !cfg.AllowAll && len(cfg.Topics) > 0 {
		opts = append(opts, golang.WithTopics(cfg.Topics...))
	}
	producer, err := golang.NewProducer(sdkConfig(cfg), opts...)
	if err != nil {
		return nil, err
	}
	if err = producer.Start(); err != nil {
		_ = producer.GracefulStop()
		return nil, err
	}
	_ = ctx
	return &sdkSender{producer: producer}, nil
}

func (s *sdkSender) Send(ctx context.Context, out outbound) (string, error) {
	msg := &golang.Message{Topic: out.Topic, Body: out.Body}
	if out.Tag != "" {
		msg.SetTag(out.Tag)
	}
	if len(out.Keys) > 0 {
		msg.SetKeys(out.Keys...)
	}
	for key, value := range out.Properties {
		msg.AddProperty(key, value)
	}
	// SDK 按是否设置了投递时间 / 消息分组决定消息类型（DELAY / FIFO / NORMAL），
	// 两者已在 buildOutbound 里互斥校验过。
	if !out.DeliverAt.IsZero() {
		msg.SetDelayTimestamp(out.DeliverAt)
	}
	if out.MessageGroup != "" {
		msg.SetMessageGroup(out.MessageGroup)
	}
	receipts, err := s.producer.Send(ctx, msg)
	if err != nil {
		return "", err
	}
	if len(receipts) == 0 || receipts[0].MessageID == "" {
		return "", errEmptyMessageID
	}
	return receipts[0].MessageID, nil
}

func (s *sdkSender) Close(ctx context.Context) error {
	_ = ctx
	return s.producer.GracefulStop()
}

func sdkConfig(cfg Config) *golang.Config {
	return &golang.Config{
		Endpoint: cfg.Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    cfg.AccessKey,
			AccessSecret: cfg.SecretKey,
		},
	}
}

var errEmptyMessageID = invalidArgumentError{reason: "empty message id"}
