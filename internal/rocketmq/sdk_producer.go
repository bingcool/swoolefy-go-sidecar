package rocketmq

import (
	"context"
	"time"

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

func (s *sdkSender) Send(ctx context.Context, topic string, body []byte, tag, key string, delay time.Duration) (string, error) {
	msg := &golang.Message{Topic: topic, Body: body}
	if tag != "" {
		msg.SetTag(tag)
	}
	if key != "" {
		msg.SetKeys(key)
	}
	if delay > 0 {
		// delay 是相对时长。用现在加上这段时间，不能把 Unix 毫秒再加一次。
		msg.SetDelayTimestamp(time.Now().Add(delay))
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
