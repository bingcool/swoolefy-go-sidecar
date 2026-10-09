package rocketmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

const (
	maxReceiveNum  = 32
	maxInvisibleMs = 5 * 60 * 1000
)

// inbound 是已经从 SDK 视图里拷出来的消息。函数返回后不再保留原视图。
type inbound struct {
	MessageID       string
	Topic           string
	Body            []byte
	Tag             string
	Keys            []string
	ReceiptHandle   string
	DeliveryAttempt int
}

// groupClient 是一个消费组上的一次调用。Ack 只用回执字段。
type groupClient interface {
	Receive(ctx context.Context, maxNum int32, invisible time.Duration) ([]inbound, error)
	Ack(ctx context.Context, receiptHandle, messageID, topic string) error
	Subscribe(topic, tag string) error
	Close(ctx context.Context) error
}

type clientFactory interface {
	Open(group string) (groupClient, error)
}

type receiveRequest struct {
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumerGroup"`
	ConsumerTag   string `json:"consumerTag"`
	MaxNum        int32  `json:"maxNum"`
	InvisibleMs   int64  `json:"invisibleMs"`
}

type receivedMessage struct {
	MessageID       string `json:"messageId"`
	Topic           string `json:"topic"`
	BodyBase64      string `json:"bodyBase64"`
	Tag             string `json:"tag"`
	Key             string `json:"key"`
	ReceiptHandle   string `json:"receiptHandle"`
	DeliveryAttempt int    `json:"deliveryAttempt"`
}

type ackRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	ReceiptHandle string `json:"receiptHandle"`
	MessageID     string `json:"messageId"`
	Topic         string `json:"topic"`
}

type groupState struct {
	client  groupClient
	subs    map[string]string
	nextTry time.Time
	backoff time.Duration
}

// Consumer 不做拉取循环，也不保存消息。
type Consumer struct {
	ready    atomic.Bool
	factory  clientFactory
	mu       sync.Mutex
	groups   map[string]*groupState
	inflight map[string]bool
	now      func() time.Time
}

func newConsumer() *Consumer {
	return &Consumer{
		groups:   map[string]*groupState{},
		inflight: map[string]bool{},
		now:      time.Now,
	}
}

func (c *Consumer) Receive(ctx context.Context, raw json.RawMessage) plugin.Result {
	if !c.ready.Load() {
		return plugin.Fail("UNAVAILABLE", "consumer not ready")
	}
	var req receiveRequest
	if err := decodePayload(raw, &req); err != nil {
		return plugin.Fail("INVALID_ARGUMENT", "payload")
	}
	if req.Topic == "" || req.ConsumerGroup == "" {
		return plugin.Fail("INVALID_ARGUMENT", "topic or consumerGroup")
	}
	if req.MaxNum <= 0 || req.MaxNum > maxReceiveNum || req.InvisibleMs <= 0 || req.InvisibleMs > maxInvisibleMs {
		return plugin.Fail("INVALID_ARGUMENT", "maxNum or invisibleMs")
	}
	key := req.ConsumerGroup + "\x00" + req.Topic
	if !c.beginReceive(key) {
		return plugin.Fail("OVERLOADED", "receive in progress")
	}
	defer c.endReceive(key)

	client, err := c.clientFor(req.ConsumerGroup)
	if err != nil {
		return plugin.Fail("UNAVAILABLE", "consumer group")
	}
	if err = c.ensureSubscription(req.ConsumerGroup, req.Topic, req.ConsumerTag); err != nil {
		var invalid invalidArgumentError
		if errors.As(err, &invalid) {
			return plugin.Fail("INVALID_ARGUMENT", "subscription")
		}
		return plugin.Fail("UNAVAILABLE", "subscription")
	}
	messages, err := client.Receive(ctx, req.MaxNum, time.Duration(req.InvisibleMs)*time.Millisecond)
	if err != nil {
		if ctx.Err() != nil {
			return plugin.OK(map[string]any{"messages": []any{}})
		}
		return classify(ctx, err)
	}
	out := make([]receivedMessage, 0, len(messages))
	for _, item := range messages {
		if item.Topic != "" && item.Topic != req.Topic {
			continue
		}
		out = append(out, receivedMessage{
			MessageID:       item.MessageID,
			Topic:           item.Topic,
			BodyBase64:      base64.StdEncoding.EncodeToString(item.Body),
			Tag:             item.Tag,
			Key:             strings.Join(item.Keys, ","),
			ReceiptHandle:   item.ReceiptHandle,
			DeliveryAttempt: item.DeliveryAttempt,
		})
	}
	return plugin.OK(map[string]any{"messages": out})
}

func (c *Consumer) Ack(ctx context.Context, raw json.RawMessage) plugin.Result {
	if !c.ready.Load() {
		return plugin.Fail("UNAVAILABLE", "consumer not ready")
	}
	var req ackRequest
	if err := decodePayload(raw, &req); err != nil {
		return plugin.Fail("INVALID_ARGUMENT", "payload")
	}
	if req.ConsumerGroup == "" || req.ReceiptHandle == "" || req.MessageID == "" || req.Topic == "" {
		return plugin.Fail("INVALID_ARGUMENT", "ack")
	}
	client, err := c.clientFor(req.ConsumerGroup)
	if err != nil {
		return plugin.Fail("UNAVAILABLE", "consumer group")
	}
	if err = client.Ack(ctx, req.ReceiptHandle, req.MessageID, req.Topic); err != nil {
		if errors.Is(err, errReceiptAckUnsupported) {
			return plugin.Fail("INTERNAL", "ack api unavailable")
		}
		return classify(ctx, err)
	}
	return plugin.OK(map[string]any{})
}

func (c *Consumer) beginReceive(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight[key] {
		return false
	}
	c.inflight[key] = true
	return true
}

func (c *Consumer) endReceive(key string) {
	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()
}

func (c *Consumer) clientFor(group string) (groupClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.groups[group]
	if state != nil && state.client != nil {
		return state.client, nil
	}
	now := c.now()
	if state != nil && now.Before(state.nextTry) {
		return nil, errNotReady
	}
	if c.factory == nil {
		return nil, errNotReady
	}
	client, err := c.factory.Open(group)
	if err != nil || client == nil {
		waitFor := time.Second
		if state != nil && state.backoff > 0 {
			waitFor = state.backoff
		}
		if state == nil {
			state = &groupState{subs: map[string]string{}}
			c.groups[group] = state
		}
		state.nextTry = now.Add(waitFor)
		state.backoff = nextBackoff(waitFor)
		return nil, errNotReady
	}
	if state == nil {
		state = &groupState{subs: map[string]string{}}
		c.groups[group] = state
	}
	state.client = client
	state.backoff = 0
	state.nextTry = time.Time{}
	return client, nil
}

func (c *Consumer) ensureSubscription(group, topic, tag string) error {
	norm := tag
	if norm == "" {
		norm = "*"
	}
	c.mu.Lock()
	state := c.groups[group]
	if state == nil || state.client == nil {
		c.mu.Unlock()
		return errNotReady
	}
	if prev, ok := state.subs[topic]; ok {
		c.mu.Unlock()
		if prev != norm {
			return errTagChanged
		}
		return nil
	}
	client := state.client
	c.mu.Unlock()
	if err := client.Subscribe(topic, norm); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := state.subs[topic]; ok && prev != norm {
		return errTagChanged
	}
	state.subs[topic] = norm
	return nil
}

func (c *Consumer) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, state := range c.groups {
		if state.client == nil {
			continue
		}
		if err := state.client.Close(ctx); err != nil && first == nil {
			first = err
		}
		state.client = nil
	}
	return first
}

func classify(ctx context.Context, err error) plugin.Result {
	if ctx.Err() != nil {
		return plugin.Fail("TIMEOUT", "timeout")
	}
	var invalid invalidArgumentError
	if errors.As(err, &invalid) {
		return plugin.Fail("INVALID_ARGUMENT", "rejected")
	}
	return plugin.Fail("UNAVAILABLE", "upstream unavailable")
}

var (
	errNotReady              = errors.New("consumer group not ready")
	errTagChanged            = invalidArgumentError{reason: "tag"}
	errReceiptAckUnsupported = errors.New("receipt ack unsupported")
)
