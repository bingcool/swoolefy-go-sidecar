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
	maxReceiveNum = 32
	// minInvisibleMs 是 proxy 允许的最小不可见时间（minInvisibleTimeMillsForRecv），
	// 小于它会返回 ILLEGAL_INVISIBLE_TIME。receive 的下限由 PHP 抬到 10 秒，这里只用于 changeInvisible。
	minInvisibleMs = 10 * 1000
	maxInvisibleMs = 5 * 60 * 1000
	maxSQLBytes    = 4096
)

const (
	filterTag   = "TAG"
	filterSQL92 = "SQL92"
)

// subscription 是一个消费组在一个 Topic 上的过滤条件。Tag 订阅全部时 Expression 为 "*"。
type subscription struct {
	Kind       string
	Expression string
}

// inbound 是已经从 SDK 视图里拷出来的消息。函数返回后不再保留原视图。
type inbound struct {
	MessageID       string
	Topic           string
	Body            []byte
	Tag             string
	Keys            []string
	Properties      map[string]string
	MessageGroup    string
	BornAt          time.Time // 生产端发送时刻，取不到时为零值
	DeliverAt       time.Time // 定时消息的计划投递时刻，非定时消息为零值
	ReceiptHandle   string
	DeliveryAttempt int
}

// groupClient 是一个消费组上的一次调用。Ack / ChangeInvisible 只用回执字段。
type groupClient interface {
	Receive(ctx context.Context, maxNum int32, invisible time.Duration) ([]inbound, error)
	Ack(ctx context.Context, receiptHandle, messageID, topic string) error
	// ChangeInvisible 返回 proxy 新签发的回执，旧回执随即失效。
	ChangeInvisible(ctx context.Context, receiptHandle, messageID, topic string, invisible time.Duration) (string, error)
	Subscribe(topic string, sub subscription) error
	Close(ctx context.Context) error
}

type clientFactory interface {
	Open(group string) (groupClient, error)
}

type receiveRequest struct {
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumerGroup"`
	ConsumerTag   string `json:"consumerTag"`
	// ConsumerSQL 是 SQL92 属性过滤表达式，例如 eventType IN ('created','cancelled')。
	// 非空时按 SQL92 订阅，ConsumerTag 只能是 "*" 或空；语法由 broker 校验，broker 需开启 enablePropertyFilter。
	ConsumerSQL string `json:"consumerSql,omitempty"`
	MaxNum      int32  `json:"maxNum"`
	InvisibleMs int64  `json:"invisibleMs"`
}

// receivedMessage 是 receive 返回给 PHP 的一条消息。
// keys / properties 始终输出 [] / {}，不输出 null：PHP DTO 的这两个字段是非空数组类型。
type receivedMessage struct {
	MessageID  string            `json:"messageId"`
	Topic      string            `json:"topic"`
	BodyBase64 string            `json:"bodyBase64"`
	Tag        string            `json:"tag"`
	Keys       []string          `json:"keys"`
	Properties map[string]string `json:"properties"`
	// MessageGroup 是顺序消息分组，普通消息为空串。
	MessageGroup string `json:"messageGroup"`
	// BornTimestampMs 是生产端发送时刻（Unix 毫秒），取不到时为 0。
	BornTimestampMs int64 `json:"bornTimestampMs"`
	// DeliveryTimestampMs 是定时消息的计划投递时刻（Unix 毫秒），非定时消息为 0。
	DeliveryTimestampMs int64  `json:"deliveryTimestampMs"`
	ReceiptHandle       string `json:"receiptHandle"`
	DeliveryAttempt     int    `json:"deliveryAttempt"`
}

type ackRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	ReceiptHandle string `json:"receiptHandle"`
	MessageID     string `json:"messageId"`
	Topic         string `json:"topic"`
}

// changeInvisibleRequest 是 rocketmq.consumer.changeInvisible 的 payload。
type changeInvisibleRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	MessageID     string `json:"messageId"`
	ReceiptHandle string `json:"receiptHandle"`
	// InvisibleMs 是从现在起再隐藏多久，10000–300000 毫秒。
	InvisibleMs int64 `json:"invisibleMs"`
}

type groupState struct {
	client  groupClient
	subs    map[string]subscription
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
	sub, reason := subscriptionOf(req.ConsumerTag, req.ConsumerSQL)
	if reason != "" {
		return plugin.Fail("INVALID_ARGUMENT", reason)
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
	if err = c.ensureSubscription(req.ConsumerGroup, req.Topic, sub); err != nil {
		var invalid invalidArgumentError
		if errors.As(err, &invalid) {
			return plugin.Fail("INVALID_ARGUMENT", invalid.reason)
		}
		// 非法 SQL、broker 未开 enablePropertyFilter 等是 400xx，重试无意义。
		if reason, ok := clientRejection(err); ok {
			return plugin.Fail("INVALID_ARGUMENT", reason)
		}
		return plugin.Fail("UNAVAILABLE", "subscription")
	}
	messages, err := client.Receive(ctx, req.MaxNum, time.Duration(req.InvisibleMs)*time.Millisecond)
	if err != nil {
		if ctx.Err() != nil || isEmptyReceive(err) {
			return plugin.OK(map[string]any{"messages": []any{}})
		}
		if sub.Kind == filterSQL92 && filterParseFailed(err) {
			return plugin.Fail("INVALID_ARGUMENT", "ILLEGAL_FILTER_EXPRESSION")
		}
		return classify(ctx, err)
	}
	out := make([]receivedMessage, 0, len(messages))
	for _, item := range messages {
		if item.Topic != "" && item.Topic != req.Topic {
			continue
		}
		keys := item.Keys
		if keys == nil {
			keys = []string{}
		}
		out = append(out, receivedMessage{
			MessageID:           item.MessageID,
			Topic:               item.Topic,
			BodyBase64:          base64.StdEncoding.EncodeToString(item.Body),
			Tag:                 item.Tag,
			Keys:                keys,
			Properties:          userProperties(item.Properties),
			MessageGroup:        item.MessageGroup,
			BornTimestampMs:     unixMilli(item.BornAt),
			DeliveryTimestampMs: unixMilli(item.DeliverAt),
			ReceiptHandle:       item.ReceiptHandle,
			DeliveryAttempt:     item.DeliveryAttempt,
		})
	}
	return plugin.OK(map[string]any{"messages": out})
}

// subscriptionOf 把请求里的 consumerTag / consumerSql 归一成订阅条件，不合法时返回原因。
// SQL 只检查长度和非空，语法交给 broker：sidecar 自己解析容易和 broker 的方言不一致。
func subscriptionOf(tag, sql string) (subscription, string) {
	if sql != "" {
		sql = strings.TrimSpace(sql)
		if sql == "" || len(sql) > maxSQLBytes {
			return subscription{}, "consumerSql"
		}
		if tag != "" && tag != "*" {
			return subscription{}, "consumerTag and consumerSql are exclusive"
		}
		return subscription{Kind: filterSQL92, Expression: sql}, ""
	}
	if tag == "" {
		tag = "*"
	}
	return subscription{Kind: filterTag, Expression: tag}, ""
}

// userProperties 去掉以 "__" 开头的 RocketMQ 系统属性，只把业务写入的属性交给 PHP。
func userProperties(props map[string]string) map[string]string {
	out := make(map[string]string, len(props))
	for key, value := range props {
		if strings.HasPrefix(key, "__") {
			continue
		}
		out[key] = value
	}
	return out
}

func unixMilli(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
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

// ChangeInvisible 重新设置一条已拉取消息的不可见时间，用于长任务续租，或让失败消息按指定时间重投。
// proxy 每次都会签发新的 receiptHandle，旧回执失效；返回给 PHP 后，后续 ack 必须用新回执。
// RocketMQ proxy 在 apache/rocketmq#9583 修复前（5.3.x 及更早），用新回执 ack 也可能不生效，
// 消息会在新的不可见时间到期后重投；这是服务端问题，sidecar 无法绕过。
func (c *Consumer) ChangeInvisible(ctx context.Context, raw json.RawMessage) plugin.Result {
	if !c.ready.Load() {
		return plugin.Fail("UNAVAILABLE", "consumer not ready")
	}
	var req changeInvisibleRequest
	if err := decodePayload(raw, &req); err != nil {
		return plugin.Fail("INVALID_ARGUMENT", "payload")
	}
	if req.ConsumerGroup == "" || req.ReceiptHandle == "" || req.MessageID == "" || req.Topic == "" {
		return plugin.Fail("INVALID_ARGUMENT", "changeInvisible")
	}
	if req.InvisibleMs < minInvisibleMs || req.InvisibleMs > maxInvisibleMs {
		return plugin.Fail("INVALID_ARGUMENT", "invisibleMs")
	}
	client, err := c.clientFor(req.ConsumerGroup)
	if err != nil {
		return plugin.Fail("UNAVAILABLE", "consumer group")
	}
	receipt, err := client.ChangeInvisible(ctx, req.ReceiptHandle, req.MessageID, req.Topic, time.Duration(req.InvisibleMs)*time.Millisecond)
	if err != nil {
		return classify(ctx, err)
	}
	if receipt == "" {
		return plugin.Fail("INTERNAL", "empty receipt handle")
	}
	return plugin.OK(map[string]any{"receiptHandle": receipt})
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
			state = &groupState{subs: map[string]subscription{}}
			c.groups[group] = state
		}
		state.nextTry = now.Add(waitFor)
		state.backoff = nextBackoff(waitFor)
		return nil, errNotReady
	}
	if state == nil {
		state = &groupState{subs: map[string]subscription{}}
		c.groups[group] = state
	}
	state.client = client
	state.backoff = 0
	state.nextTry = time.Time{}
	return client, nil
}

// ensureSubscription 让 group 在 topic 上按 sub 订阅。同一组 + Topic 只订阅一次，之后条件变了就拒绝：
// RocketMQ 的订阅关系属于消费组，拉取进行中换条件会让同组其他实例的过滤结果不确定。
func (c *Consumer) ensureSubscription(group, topic string, sub subscription) error {
	c.mu.Lock()
	state := c.groups[group]
	if state == nil || state.client == nil {
		c.mu.Unlock()
		return errNotReady
	}
	if prev, ok := state.subs[topic]; ok {
		c.mu.Unlock()
		if prev != sub {
			return errFilterChanged
		}
		return nil
	}
	client := state.client
	c.mu.Unlock()
	if err := client.Subscribe(topic, sub); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := state.subs[topic]; ok && prev != sub {
		return errFilterChanged
	}
	state.subs[topic] = sub
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
	// 回执过期、已被别的消费者拿走等是 400xx，重试无意义。
	if reason, ok := clientRejection(err); ok {
		return plugin.Fail("INVALID_ARGUMENT", reason)
	}
	return plugin.Fail("UNAVAILABLE", err.Error())
}

var (
	errNotReady              = errors.New("consumer group not ready")
	errFilterChanged         = invalidArgumentError{reason: "filter"}
	errReceiptAckUnsupported = errors.New("receipt ack unsupported")
)
