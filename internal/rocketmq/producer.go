package rocketmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

// Sender 是一次发送。delay 为 0 表示立即投递，大于 0 是相对时长，不是 Unix 时间戳。
type Sender interface {
	Send(ctx context.Context, topic string, body []byte, tag, key string, delay time.Duration) (string, error)
	Close(ctx context.Context) error
}

type sendRequest struct {
	Topic      string `json:"topic"`
	BodyBase64 string `json:"bodyBase64"`
	Tag        string `json:"tag"`
	Key        string `json:"key"`
	DelayMs    int64  `json:"delayMs"`
}

type Producer struct {
	cfg      Config
	sender   Sender
	ready    atomic.Bool
	sendOK   atomic.Uint64
	sendFail atomic.Uint64
	sendWait atomic.Uint64
	opener   func(ctx context.Context) (Sender, error)
	sleep    func(ctx context.Context, d time.Duration) error
	backoff  time.Duration
}

func newProducer(cfg Config) *Producer {
	return &Producer{
		cfg: cfg,
		opener: func(ctx context.Context) (Sender, error) {
			return openSDKProducer(ctx, cfg)
		},
		sleep: sleepCtx,
	}
}

func (p *Producer) Send(ctx context.Context, raw json.RawMessage) plugin.Result {
	if !p.ready.Load() || p.sender == nil {
		return plugin.Fail("UNAVAILABLE", "producer not ready")
	}
	var req sendRequest
	if err := decodePayload(raw, &req); err != nil {
		return plugin.Fail("INVALID_ARGUMENT", "payload")
	}
	if !p.allow(req.Topic) {
		return plugin.Fail("INVALID_ARGUMENT", "topic")
	}
	body, err := base64.StdEncoding.DecodeString(req.BodyBase64)
	if err != nil || len(body) == 0 {
		return plugin.Fail("INVALID_ARGUMENT", "bodyBase64")
	}
	if req.DelayMs < 0 {
		return plugin.Fail("INVALID_ARGUMENT", "delayMs")
	}
	delay := time.Duration(req.DelayMs) * time.Millisecond
	messageID, err := p.sender.Send(ctx, req.Topic, body, req.Tag, req.Key, delay)
	if err != nil {
		if errors.Is(err, errEmptyMessageID) {
			p.sendFail.Add(1)
			return plugin.Fail("INTERNAL", "empty message id")
		}
		return p.classifySend(ctx, err)
	}
	if messageID == "" {
		p.sendFail.Add(1)
		return plugin.Fail("INTERNAL", "empty message id")
	}
	p.sendOK.Add(1)
	return plugin.OK(map[string]any{"messageId": messageID})
}

func (p *Producer) allow(topic string) bool {
	if topic == "" {
		return false
	}
	if p.cfg.AllowAll {
		return true
	}
	for _, item := range p.cfg.Topics {
		if item == topic {
			return true
		}
	}
	return false
}

func (p *Producer) classifySend(ctx context.Context, err error) plugin.Result {
	if ctx.Err() != nil {
		p.sendWait.Add(1)
		return plugin.Fail("TIMEOUT", "timeout")
	}
	var invalid invalidArgumentError
	if errors.As(err, &invalid) {
		p.sendFail.Add(1)
		return plugin.Fail("INVALID_ARGUMENT", "rejected")
	}
	p.sendFail.Add(1)
	p.ready.Store(false)
	if p.sender != nil {
		_ = p.sender.Close(context.Background())
		p.sender = nil
	}
	return plugin.Fail("UNAVAILABLE", "upstream unavailable")
}

func (p *Producer) maintain(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if p.ready.Load() && p.sender != nil {
			if !wait(ctx, p.sleep, 200*time.Millisecond) {
				return
			}
			continue
		}
		sender, err := p.opener(ctx)
		if err != nil || sender == nil {
			p.ready.Store(false)
			waitFor := p.backoff
			if waitFor == 0 {
				waitFor = time.Second
			}
			p.backoff = nextBackoff(waitFor)
			if !wait(ctx, p.sleep, waitFor) {
				return
			}
			continue
		}
		p.sender = sender
		p.ready.Store(true)
		p.backoff = 0
	}
}

func (p *Producer) Close(ctx context.Context) error {
	p.ready.Store(false)
	if p.sender == nil {
		return nil
	}
	err := p.sender.Close(ctx)
	p.sender = nil
	return err
}

func nextBackoff(cur time.Duration) time.Duration {
	if cur < time.Second {
		return time.Second
	}
	next := cur * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func wait(ctx context.Context, sleep func(context.Context, time.Duration) error, d time.Duration) bool {
	if sleep == nil {
		sleep = sleepCtx
	}
	return sleep(ctx, d) == nil
}

type invalidArgumentError struct {
	reason string
}

func (invalidArgumentError) Error() string { return "invalid argument" }
