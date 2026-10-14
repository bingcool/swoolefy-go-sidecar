package rocketmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

// 发送参数的上限。和 PHP 侧 SendMessageRequestDto 的校验保持一致，sidecar 为准。
const (
	maxSendKeys          = 32
	maxSendKeyBytes      = 128
	maxSendProperties    = 32
	maxPropertyKeyBytes  = 64
	maxPropertyValBytes  = 1024
	maxPropertiesBytes   = 16 * 1024
	maxMessageGroupBytes = 64
	// maxDeliverAhead 只拦明显写错的时间。RocketMQ 服务端默认 timerMaxDelaySec 是 3 天，
	// 超过服务端上限由 proxy 拒绝。
	maxDeliverAhead = 40 * 24 * time.Hour
)

// outbound 是一条待发送消息，已经通过校验。
type outbound struct {
	Topic        string
	Body         []byte
	Tag          string
	Keys         []string
	Properties   map[string]string
	DeliverAt    time.Time // 零值表示立即投递
	MessageGroup string
}

// Sender 是一次发送。
type Sender interface {
	Send(ctx context.Context, msg outbound) (string, error)
	Close(ctx context.Context) error
}

// sendRequest 是 rocketmq.producer.send 的 payload。
type sendRequest struct {
	Topic      string `json:"topic"`
	BodyBase64 string `json:"bodyBase64"`
	// Tag 是消息分类，消费组按 Tag 订阅过滤。
	Tag string `json:"tag"`
	// Keys 是业务索引（订单号、用户号等），用于在控制台按 Key 查消息，不影响消费。
	Keys []string `json:"keys"`
	// Properties 是自定义属性，消费端原样读回。
	Properties map[string]string `json:"properties"`
	// DelayMs 是相对时长（毫秒），从 sidecar 收到请求起算；和 DeliverAtMs 互斥。
	DelayMs int64 `json:"delayMs"`
	// DeliverAtMs 是绝对投递时间（Unix 毫秒）；和 DelayMs 互斥。
	DeliverAtMs int64 `json:"deliverAtMs"`
	// MessageGroup 是顺序消息分组，只能发到 FIFO 类型的 Topic；不能和延时同时使用。
	MessageGroup string `json:"messageGroup"`
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
	now      func() time.Time
	backoff  time.Duration
}

func newProducer(cfg Config) *Producer {
	return &Producer{
		cfg: cfg,
		opener: func(ctx context.Context) (Sender, error) {
			return openSDKProducer(ctx, cfg)
		},
		sleep: sleepCtx,
		now:   time.Now,
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
	msg, field := buildOutbound(req, p.now())
	if field != "" {
		return plugin.Fail("INVALID_ARGUMENT", field)
	}
	messageID, err := p.sender.Send(ctx, msg)
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

// buildOutbound 校验请求并换算投递时间。第二个返回值非空时是不合法的字段名。
func buildOutbound(req sendRequest, now time.Time) (outbound, string) {
	body, err := base64.StdEncoding.DecodeString(req.BodyBase64)
	if err != nil || len(body) == 0 {
		return outbound{}, "bodyBase64"
	}
	keys, ok := normalizeKeys(req.Keys)
	if !ok {
		return outbound{}, "keys"
	}
	if !validProperties(req.Properties) {
		return outbound{}, "properties"
	}
	if req.DelayMs < 0 {
		return outbound{}, "delayMs"
	}
	if req.DelayMs > 0 && req.DeliverAtMs != 0 {
		return outbound{}, "delayMs and deliverAtMs are exclusive"
	}
	var deliverAt time.Time
	switch {
	case req.DelayMs > 0:
		deliverAt = now.Add(time.Duration(req.DelayMs) * time.Millisecond)
	case req.DeliverAtMs != 0:
		// 早于当前时间直接拒绝：按立即投递处理会把业务以为的「将来」变成「现在」。
		// 常见原因是传了秒级时间戳。
		deliverAt = time.UnixMilli(req.DeliverAtMs)
		if !deliverAt.After(now) || deliverAt.Sub(now) > maxDeliverAhead {
			return outbound{}, "deliverAtMs"
		}
	}
	if len(req.MessageGroup) > maxMessageGroupBytes || hasSpace(req.MessageGroup) {
		return outbound{}, "messageGroup"
	}
	// RocketMQ 5 的消息类型是互斥的：FIFO 消息不能同时是定时消息。
	if req.MessageGroup != "" && !deliverAt.IsZero() {
		return outbound{}, "messageGroup and delay are exclusive"
	}
	return outbound{
		Topic:        req.Topic,
		Body:         body,
		Tag:          req.Tag,
		Keys:         keys,
		Properties:   req.Properties,
		DeliverAt:    deliverAt,
		MessageGroup: req.MessageGroup,
	}, ""
}

// normalizeKeys 去掉重复的 Key，保留首次出现的顺序。
func normalizeKeys(keys []string) ([]string, bool) {
	if len(keys) > maxSendKeys {
		return nil, false
	}
	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" || len(key) > maxSendKeyBytes || hasSpace(key) {
			return nil, false
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out, true
}

// validProperties 拒绝以 "__" 开头的 Key：RocketMQ 的系统属性用这个前缀，混用会被覆盖或误读。
func validProperties(props map[string]string) bool {
	if len(props) > maxSendProperties {
		return false
	}
	total := 0
	for key, value := range props {
		if !validPropertyKey(key) || strings.HasPrefix(key, "__") || len(value) > maxPropertyValBytes {
			return false
		}
		total += len(key) + len(value)
	}
	return total <= maxPropertiesBytes
}

func validPropertyKey(key string) bool {
	if key == "" || len(key) > maxPropertyKeyBytes {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func hasSpace(value string) bool {
	return strings.IndexFunc(value, unicode.IsSpace) >= 0
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

// classifySend 区分「请求本身不合法」和「连接不可用」。
// proxy 返回 4xx 状态（如普通 Topic 收到带 messageGroup 的消息）是请求问题，重连也不会成功，
// 所以不重建 producer；只有其他错误才关闭连接，由 maintain() 重连。
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
	if reason, ok := clientRejection(err); ok {
		p.sendFail.Add(1)
		return plugin.Fail("INVALID_ARGUMENT", reason)
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
