package rocketmq

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

var fixedNow = time.UnixMilli(1792049100000)

func readyProducer(sender *fakeSender) *Producer {
	producer := newProducer(Config{AllowAll: true})
	producer.sender = sender
	producer.now = func() time.Time { return fixedNow }
	producer.ready.Store(true)
	return producer
}

func TestSendUsesRelativeDelay(t *testing.T) {
	sender := &fakeSender{id: "m1"}
	producer := readyProducer(sender)

	immediate := producer.Send(context.Background(), mustJSON(t, sendRequest{
		Topic: "order", BodyBase64: "aGk=", DelayMs: 0,
	}))
	if immediate.Code != "OK" || !sender.last.DeliverAt.IsZero() || !sender.called {
		t.Fatalf("%+v at=%s", immediate, sender.last.DeliverAt)
	}
	delayed := producer.Send(context.Background(), mustJSON(t, sendRequest{
		Topic: "order", BodyBase64: "aGk=", DelayMs: 1500,
	}))
	if delayed.Code != "OK" || !sender.last.DeliverAt.Equal(fixedNow.Add(1500*time.Millisecond)) {
		t.Fatalf("%+v at=%s", delayed, sender.last.DeliverAt)
	}
}

func TestSendDeliverAtAndDelayResolveToSameTime(t *testing.T) {
	sender := &fakeSender{id: "m1"}
	producer := readyProducer(sender)
	at := fixedNow.Add(30 * time.Minute)
	if got := producer.Send(context.Background(), mustJSON(t, sendRequest{
		Topic: "order", BodyBase64: "aGk=", DeliverAtMs: at.UnixMilli(),
	})); got.Code != "OK" || !sender.last.DeliverAt.Equal(at) {
		t.Fatalf("%+v at=%s", got, sender.last.DeliverAt)
	}
	if got := producer.Send(context.Background(), mustJSON(t, sendRequest{
		Topic: "order", BodyBase64: "aGk=", DelayMs: (30 * time.Minute).Milliseconds(),
	})); got.Code != "OK" || !sender.last.DeliverAt.Equal(at) {
		t.Fatalf("%+v at=%s", got, sender.last.DeliverAt)
	}
}

func TestSendPassesMetadata(t *testing.T) {
	sender := &fakeSender{id: "m1"}
	producer := readyProducer(sender)
	got := producer.Send(context.Background(), mustJSON(t, sendRequest{
		Topic:        "order",
		BodyBase64:   "aGk=",
		Tag:          "paid",
		Keys:         []string{"ORDER_1", "USER_2", "ORDER_1", "a,b"},
		Properties:   map[string]string{"traceId": "t-1", "event.version": "2"},
		MessageGroup: "ORDER_1",
	}))
	if got.Code != "OK" {
		t.Fatalf("%+v", got)
	}
	last := sender.last
	if last.Tag != "paid" || last.MessageGroup != "ORDER_1" || !last.DeliverAt.IsZero() {
		t.Fatalf("%+v", last)
	}
	if len(last.Keys) != 3 || last.Keys[0] != "ORDER_1" || last.Keys[1] != "USER_2" || last.Keys[2] != "a,b" {
		t.Fatalf("keys %v", last.Keys)
	}
	if last.Properties["traceId"] != "t-1" || last.Properties["event.version"] != "2" {
		t.Fatalf("props %v", last.Properties)
	}
}

func TestSendValidation(t *testing.T) {
	tooManyKeys := make([]string, maxSendKeys+1)
	for i := range tooManyKeys {
		tooManyKeys[i] = "k" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	tooManyProps := map[string]string{}
	for i := 0; i <= maxSendProperties; i++ {
		tooManyProps["p"+string(rune('a'+i%26))+string(rune('a'+i/26))] = "v"
	}
	future := fixedNow.Add(time.Minute).UnixMilli()
	cases := map[string]sendRequest{
		"keys too many":       {Keys: tooManyKeys},
		"key empty":           {Keys: []string{""}},
		"key with space":      {Keys: []string{"a b"}},
		"key too long":        {Keys: []string{strings.Repeat("k", maxSendKeyBytes+1)}},
		"props too many":      {Properties: tooManyProps},
		"prop key illegal":    {Properties: map[string]string{"a/b": "v"}},
		"prop key system":     {Properties: map[string]string{"__TAG": "v"}},
		"prop value too long": {Properties: map[string]string{"a": strings.Repeat("v", maxPropertyValBytes+1)}},
		"delay and deliverAt": {DelayMs: 1000, DeliverAtMs: future},
		"deliverAt in past":   {DeliverAtMs: fixedNow.Add(-time.Second).UnixMilli()},
		"deliverAt seconds":   {DeliverAtMs: fixedNow.Unix()},
		"deliverAt too far":   {DeliverAtMs: fixedNow.Add(maxDeliverAhead + time.Minute).UnixMilli()},
		"group with delay":    {MessageGroup: "g", DelayMs: 1000},
		"group with deliver":  {MessageGroup: "g", DeliverAtMs: future},
		"group too long":      {MessageGroup: strings.Repeat("g", maxMessageGroupBytes+1)},
		"negative delay":      {DelayMs: -1},
	}
	for name, req := range cases {
		sender := &fakeSender{id: "m1"}
		producer := readyProducer(sender)
		req.Topic = "order"
		req.BodyBase64 = "aGk="
		if got := producer.Send(context.Background(), mustJSON(t, req)); got.Code != "INVALID_ARGUMENT" || sender.called {
			t.Errorf("%s: code=%s called=%v", name, got.Code, sender.called)
		}
	}
}

func TestSendRejectsLegacyKeyField(t *testing.T) {
	producer := readyProducer(&fakeSender{id: "m1"})
	raw := []byte(`{"topic":"order","bodyBase64":"aGk=","key":"ORDER_1"}`)
	if got := producer.Send(context.Background(), raw); got.Code != "INVALID_ARGUMENT" {
		t.Fatal(got.Code)
	}
}

func TestSendLegacyPayloadStillWorks(t *testing.T) {
	sender := &fakeSender{id: "m1"}
	producer := readyProducer(sender)
	raw := []byte(`{"topic":"order","bodyBase64":"aGk=","tag":"demo","delayMs":0}`)
	if got := producer.Send(context.Background(), raw); got.Code != "OK" || len(sender.last.Keys) != 0 || sender.last.Tag != "demo" {
		t.Fatalf("%+v %+v", got, sender.last)
	}
}

func TestSendProxyRejectionDoesNotReconnect(t *testing.T) {
	sender := &fakeSender{err: &golang.ErrRpcStatus{Code: int32(v2.Code_MESSAGE_PROPERTY_CONFLICT_WITH_TYPE), Message: "fifo"}}
	producer := readyProducer(sender)
	got := producer.Send(context.Background(), mustJSON(t, sendRequest{Topic: "order", BodyBase64: "aGk=", MessageGroup: "g"}))
	if got.Code != "INVALID_ARGUMENT" || got.Message != "MESSAGE_PROPERTY_CONFLICT_WITH_TYPE" {
		t.Fatalf("%+v", got)
	}
	if !producer.ready.Load() || producer.sender == nil || sender.closed {
		t.Fatal("request rejection must keep the connection")
	}

	down := &fakeSender{err: &golang.ErrRpcStatus{Code: int32(v2.Code_TOO_MANY_REQUESTS)}}
	producer = readyProducer(down)
	if got = producer.Send(context.Background(), mustJSON(t, sendRequest{Topic: "order", BodyBase64: "aGk="})); got.Code != "UNAVAILABLE" {
		t.Fatalf("%+v", got)
	}
	if producer.ready.Load() || !down.closed {
		t.Fatal("non-request errors reconnect")
	}
}

func TestSendRejectsUnknownTopicAndUnreadiness(t *testing.T) {
	producer := newProducer(Config{Topics: []string{"order"}})
	if got := producer.Send(context.Background(), mustJSON(t, sendRequest{Topic: "order", BodyBase64: "aGk="})); got.Code != "UNAVAILABLE" {
		t.Fatal(got.Code)
	}
	producer.sender = &fakeSender{id: "m1"}
	producer.ready.Store(true)
	if got := producer.Send(context.Background(), mustJSON(t, sendRequest{Topic: "other", BodyBase64: "aGk="})); got.Code != "INVALID_ARGUMENT" {
		t.Fatal(got.Code)
	}
	if got := producer.Send(context.Background(), mustJSON(t, sendRequest{Topic: "order", BodyBase64: "%%%"})); got.Code != "INVALID_ARGUMENT" {
		t.Fatal(got.Code)
	}
}

func TestBackoffCapsAtThirtySeconds(t *testing.T) {
	cur := time.Duration(0)
	var seen []time.Duration
	for i := 0; i < 7; i++ {
		cur = nextBackoff(cur)
		seen = append(seen, cur)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("%v", seen)
		}
	}
}

func TestReceivePassesLimitsAndEmptyList(t *testing.T) {
	group := &fakeGroup{}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	result := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "order-consumer", ConsumerTag: "*", MaxNum: 2, InvisibleMs: 20000,
	}))
	if result.Code != "OK" || group.calls != 1 || group.maxNum != 2 || group.invisible != 20*time.Second {
		t.Fatalf("%+v calls=%d num=%d invisible=%s", result, group.calls, group.maxNum, group.invisible)
	}
	body := result.Payload.(map[string]any)
	messages, _ := body["messages"].([]receivedMessage)
	if len(messages) != 0 {
		t.Fatal(messages)
	}
	if group.acks != 0 {
		t.Fatal("receive must not ack")
	}
}

func TestSecondReceiveOnSameTopicIsOverloaded(t *testing.T) {
	group := &fakeGroup{block: make(chan struct{}), entered: make(chan struct{})}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	first := make(chan plugin.Result, 1)
	go func() {
		first <- consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
			Topic: "order", ConsumerGroup: "g", ConsumerTag: "*", MaxNum: 2, InvisibleMs: 1000,
		}))
	}()
	select {
	case <-group.entered:
	case <-time.After(time.Second):
		t.Fatal("first receive did not start")
	}
	second := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "g", ConsumerTag: "*", MaxNum: 2, InvisibleMs: 1000,
	}))
	if second.Code != "OVERLOADED" {
		t.Fatal(second.Code)
	}
	other := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "pay", ConsumerGroup: "g", ConsumerTag: "*", MaxNum: 2, InvisibleMs: 1000,
	}))
	if other.Code == "OVERLOADED" {
		t.Fatal("another topic must not share the latch")
	}
	close(group.block)
	<-first
}

func TestSubscriptionTagIsSticky(t *testing.T) {
	group := &fakeGroup{}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	ok := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "g", ConsumerTag: "", MaxNum: 1, InvisibleMs: 1000,
	}))
	all := subscription{Kind: filterTag, Expression: "*"}
	if ok.Code != "OK" || group.subs["order"] != all {
		t.Fatalf("%s %+v", ok.Code, group.subs)
	}
	bad := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "g", ConsumerTag: "paid", MaxNum: 1, InvisibleMs: 1000,
	}))
	if bad.Code != "INVALID_ARGUMENT" || bad.Message != "filter" || group.subs["order"] != all {
		t.Fatalf("%s %s %+v", bad.Code, bad.Message, group.subs)
	}
	toSQL := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "g", ConsumerSQL: "a = '1'", MaxNum: 1, InvisibleMs: 1000,
	}))
	if toSQL.Code != "INVALID_ARGUMENT" || toSQL.Message != "filter" {
		t.Fatalf("%s %s", toSQL.Code, toSQL.Message)
	}
}

func TestSQLSubscription(t *testing.T) {
	group := &fakeGroup{}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	ok := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order-events", ConsumerGroup: "stock", ConsumerTag: "*",
		ConsumerSQL: "  eventType IN ('created','cancelled')  ", MaxNum: 1, InvisibleMs: 1000,
	}))
	want := subscription{Kind: filterSQL92, Expression: "eventType IN ('created','cancelled')"}
	if ok.Code != "OK" || group.subs["order-events"] != want {
		t.Fatalf("%s %+v", ok.Code, group.subs)
	}
	same := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order-events", ConsumerGroup: "stock", ConsumerSQL: "eventType IN ('created','cancelled')", MaxNum: 1, InvisibleMs: 1000,
	}))
	if same.Code != "OK" || group.subscribes != 1 {
		t.Fatalf("%s subscribes=%d", same.Code, group.subscribes)
	}
}

func TestSQLSubscriptionValidation(t *testing.T) {
	cases := map[string]receiveRequest{
		"tag and sql":  {ConsumerTag: "paid", ConsumerSQL: "a = '1'"},
		"blank sql":    {ConsumerSQL: "   "},
		"sql too long": {ConsumerSQL: "a = '" + strings.Repeat("x", maxSQLBytes) + "'"},
	}
	for name, req := range cases {
		group := &fakeGroup{}
		consumer := newConsumer()
		consumer.ready.Store(true)
		consumer.factory = fakeFactory{client: group}
		req.Topic, req.ConsumerGroup, req.MaxNum, req.InvisibleMs = "t", "g", 1, 1000
		result := consumer.Receive(context.Background(), mustJSON(t, req))
		if result.Code != "INVALID_ARGUMENT" || group.subscribes != 0 {
			t.Fatalf("%s: %s %s subscribes=%d", name, result.Code, result.Message, group.subscribes)
		}
	}
}

func TestSubscribeRejectedByProxyIsInvalidArgument(t *testing.T) {
	group := &fakeGroup{subErr: &golang.ErrRpcStatus{Code: int32(v2.Code_ILLEGAL_FILTER_EXPRESSION), Message: "bad sql"}}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	result := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "t", ConsumerGroup: "g", ConsumerSQL: "a ==", MaxNum: 1, InvisibleMs: 1000,
	}))
	if result.Code != "INVALID_ARGUMENT" || result.Message != "ILLEGAL_FILTER_EXPRESSION" {
		t.Fatalf("%s %s", result.Code, result.Message)
	}
}

func TestSQLParseFailureOnReceiveIsInvalidArgument(t *testing.T) {
	parseErr := &golang.ErrRpcStatus{
		Code:    int32(v2.Code_INTERNAL_SERVER_ERROR),
		Message: "org.apache.rocketmq.proxy.common.ProxyException: parse the consumer's subscription failed",
	}
	for _, tc := range []struct {
		sql  string
		code string
	}{
		{"a ==", "INVALID_ARGUMENT"},
		// Tag 订阅遇到同样的错误不归为过滤表达式问题
		{"", "UNAVAILABLE"},
	} {
		consumer := newConsumer()
		consumer.ready.Store(true)
		consumer.factory = fakeFactory{client: &fakeGroup{err: parseErr}}
		result := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
			Topic: "t", ConsumerGroup: "g", ConsumerTag: "*", ConsumerSQL: tc.sql, MaxNum: 1, InvisibleMs: 1000,
		}))
		if result.Code != tc.code {
			t.Fatalf("sql=%q: %s %s", tc.sql, result.Code, result.Message)
		}
	}
}

func TestReceiveRequestOmitsEmptySQL(t *testing.T) {
	raw := mustJSON(t, receiveRequest{Topic: "t", ConsumerGroup: "g", ConsumerTag: "*", MaxNum: 1, InvisibleMs: 1000})
	if strings.Contains(string(raw), "consumerSql") {
		t.Fatal(string(raw))
	}
}

func TestCanceledReceiveIsEmptySuccess(t *testing.T) {
	group := &fakeGroup{err: context.DeadlineExceeded}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := consumer.Receive(ctx, mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "g", ConsumerTag: "*", MaxNum: 2, InvisibleMs: 1000,
	}))
	if result.Code != "OK" {
		t.Fatal(result.Code)
	}
	if group.acks != 0 {
		t.Fatal("canceled receive must not ack")
	}
}

func TestAckUsesReceiptFields(t *testing.T) {
	group := &fakeGroup{}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	result := consumer.Ack(context.Background(), mustJSON(t, ackRequest{
		ConsumerGroup: "g", ReceiptHandle: "rh", MessageID: "mid", Topic: "order",
	}))
	if result.Code != "OK" || group.acks != 1 || group.lastReceipt != "rh" || group.lastID != "mid" || group.lastTopic != "order" {
		t.Fatalf("%+v acks=%d", result, group.acks)
	}
}

func TestReceiveReturnsMetadata(t *testing.T) {
	born := fixedNow.Add(-time.Minute)
	deliver := fixedNow
	group := &fakeGroup{messages: []inbound{
		{
			MessageID: "m1", Topic: "order", Body: []byte("hi"), Tag: "paid",
			Keys:         []string{"ORDER_1", "a,b"},
			Properties:   map[string]string{"traceId": "t-1", "__SYS": "x"},
			MessageGroup: "ORDER_1", BornAt: born, DeliverAt: deliver,
			ReceiptHandle: "rh", DeliveryAttempt: 2,
		},
		{MessageID: "m2", Topic: "order", Body: []byte("x"), ReceiptHandle: "rh2", DeliveryAttempt: 1},
	}}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	result := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "g", ConsumerTag: "*", MaxNum: 2, InvisibleMs: 20000,
	}))
	if result.Code != "OK" {
		t.Fatal(result.Code)
	}
	raw, err := json.Marshal(result.Payload)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	if err = json.Unmarshal(raw, &body); err != nil || len(body.Messages) != 2 {
		t.Fatalf("%s %v", raw, err)
	}
	first := body.Messages[0]
	if _, ok := first["key"]; ok {
		t.Fatal("legacy key field must be gone")
	}
	keys := first["keys"].([]any)
	if len(keys) != 2 || keys[1] != "a,b" {
		t.Fatalf("keys %v", keys)
	}
	props := first["properties"].(map[string]any)
	if props["traceId"] != "t-1" || props["__SYS"] != nil {
		t.Fatalf("props %v", props)
	}
	if first["messageGroup"] != "ORDER_1" ||
		int64(first["bornTimestampMs"].(float64)) != born.UnixMilli() ||
		int64(first["deliveryTimestampMs"].(float64)) != deliver.UnixMilli() {
		t.Fatalf("%v", first)
	}
	second := body.Messages[1]
	if k, ok := second["keys"].([]any); !ok || len(k) != 0 {
		t.Fatalf("empty keys must be [] not null: %s", raw)
	}
	if p, ok := second["properties"].(map[string]any); !ok || len(p) != 0 {
		t.Fatalf("empty properties must be {} not null: %s", raw)
	}
	if second["bornTimestampMs"].(float64) != 0 || second["deliveryTimestampMs"].(float64) != 0 || second["messageGroup"] != "" {
		t.Fatalf("%v", second)
	}
}

func TestChangeInvisibleReturnsNewReceipt(t *testing.T) {
	group := &fakeGroup{newReceipt: "rh-2"}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	result := consumer.ChangeInvisible(context.Background(), mustJSON(t, changeInvisibleRequest{
		ConsumerGroup: "g", Topic: "order", MessageID: "mid", ReceiptHandle: "rh-1", InvisibleMs: 60000,
	}))
	if result.Code != "OK" || result.Payload.(map[string]any)["receiptHandle"] != "rh-2" {
		t.Fatalf("%+v", result)
	}
	if group.lastReceipt != "rh-1" || group.lastID != "mid" || group.lastTopic != "order" || group.invisible != time.Minute {
		t.Fatalf("%+v", group)
	}
}

func TestChangeInvisibleValidation(t *testing.T) {
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: &fakeGroup{newReceipt: "x"}}
	base := changeInvisibleRequest{ConsumerGroup: "g", Topic: "order", MessageID: "mid", ReceiptHandle: "rh", InvisibleMs: 20000}
	cases := map[string]func(r *changeInvisibleRequest){
		"no group":      func(r *changeInvisibleRequest) { r.ConsumerGroup = "" },
		"no receipt":    func(r *changeInvisibleRequest) { r.ReceiptHandle = "" },
		"no message id": func(r *changeInvisibleRequest) { r.MessageID = "" },
		"no topic":      func(r *changeInvisibleRequest) { r.Topic = "" },
		"too short":     func(r *changeInvisibleRequest) { r.InvisibleMs = minInvisibleMs - 1 },
		"too long":      func(r *changeInvisibleRequest) { r.InvisibleMs = maxInvisibleMs + 1 },
	}
	for name, mutate := range cases {
		req := base
		mutate(&req)
		if got := consumer.ChangeInvisible(context.Background(), mustJSON(t, req)); got.Code != "INVALID_ARGUMENT" {
			t.Errorf("%s: %s", name, got.Code)
		}
	}
}

func TestChangeInvisibleExpiredReceiptIsInvalidArgument(t *testing.T) {
	group := &fakeGroup{changeErr: &golang.ErrRpcStatus{Code: int32(v2.Code_INVALID_RECEIPT_HANDLE), Message: "expired"}}
	consumer := newConsumer()
	consumer.ready.Store(true)
	consumer.factory = fakeFactory{client: group}
	result := consumer.ChangeInvisible(context.Background(), mustJSON(t, changeInvisibleRequest{
		ConsumerGroup: "g", Topic: "order", MessageID: "mid", ReceiptHandle: "rh", InvisibleMs: 20000,
	}))
	if result.Code != "INVALID_ARGUMENT" || result.Message != "INVALID_RECEIPT_HANDLE" {
		t.Fatalf("%+v", result)
	}
}

func TestChangeInvisibleReceiptNeedsConsumer(t *testing.T) {
	if _, err := changeInvisibleReceipt(context.Background(), nil, "g", "", "rh", "mid", "order", time.Minute); err == nil {
		t.Fatal("nil consumer")
	}
}

func TestAckReceiptIsNotCached(t *testing.T) {
	err := ackReceipt(context.Background(), nil, "g", "", "rh", "mid", "order")
	if err == nil {
		t.Fatal("nil consumer")
	}
}

func TestAckReceiptReadsSDKClient(t *testing.T) {
	cfg := sdkConfig(Config{Endpoint: "127.0.0.1:1"})
	cfg.ConsumerGroup = "g"
	consumer, err := golang.NewSimpleConsumer(cfg, golang.WithSubscriptionExpressions(map[string]*golang.FilterExpression{
		"t": golang.SUB_ALL,
	}))
	if err != nil {
		t.Fatal(err)
	}
	cliVal := consumerField(consumer, "cli")
	if !cliVal.IsValid() || cliVal.IsNil() {
		t.Fatal("cli")
	}
	signer, ok := cliVal.Interface().(golang.Client)
	if !ok {
		t.Fatalf("signer %T", cliVal.Interface())
	}
	if signer.GetClientID() == "" {
		t.Fatal("empty client id")
	}
	endpointsVal := structField(cliVal, "accessPoint")
	endpoints, ok := endpointsVal.Interface().(*v2.Endpoints)
	if !ok || endpoints == nil || len(endpoints.GetAddresses()) == 0 {
		t.Fatalf("endpoints %#v ok=%v", endpoints, ok)
	}
}

func TestTopicsWildcard(t *testing.T) {
	allowAll, topics, err := parseTopics("*")
	if err != nil || !allowAll || topics != nil {
		t.Fatal(err, allowAll, topics)
	}
	if _, _, err = parseTopics("order,*"); err == nil {
		t.Fatal("mixed wildcard")
	}
	_, topics, err = parseTopics("order, pay")
	if err != nil || len(topics) != 2 || topics[1] != "pay" {
		t.Fatal(topics, err)
	}
}

func TestHealthFieldsFollowRegistration(t *testing.T) {
	item := New(Config{AllowAll: true, Endpoint: "127.0.0.1:8081"})
	item.producer.ready.Store(true)
	item.consumer.ready.Store(true)
	item.producer.sendOK.Store(3)
	dst := map[string]any{}
	item.Contribute(dst)
	if dst["producerReady"] != true || dst["consumerReady"] != true || dst["sendOk"] != uint64(3) {
		t.Fatal(dst)
	}
	caps, _ := dst["capabilities"].([]string)
	if len(caps) != len(Capabilities) || caps[0] != "send.keys" {
		t.Fatal(dst["capabilities"])
	}
}

type fakeSender struct {
	id     string
	err    error
	last   outbound
	called bool
	closed bool
}

func (f *fakeSender) Send(ctx context.Context, msg outbound) (string, error) {
	f.called = true
	f.last = msg
	if f.err != nil {
		return "", f.err
	}
	return f.id, nil
}

func (f *fakeSender) Close(ctx context.Context) error {
	f.closed = true
	return nil
}

type fakeFactory struct {
	client groupClient
}

func (f fakeFactory) Open(group string) (groupClient, error) { return f.client, nil }

type fakeGroup struct {
	mu          sync.Mutex
	maxNum      int32
	invisible   time.Duration
	calls       int
	acks        int
	subs        map[string]subscription
	subscribes  int
	subErr      error
	block       chan struct{}
	entered     chan struct{}
	err         error
	lastReceipt string
	lastID      string
	lastTopic   string
	messages    []inbound
	newReceipt  string
	changeErr   error
}

func (f *fakeGroup) Receive(ctx context.Context, maxNum int32, invisible time.Duration) ([]inbound, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.maxNum = maxNum
	f.invisible = invisible
	err := f.err
	f.mu.Unlock()
	if f.entered != nil && call == 1 {
		close(f.entered)
	}
	if f.block != nil && call == 1 {
		<-f.block
	}
	if err != nil {
		return nil, err
	}
	return f.messages, nil
}

func (f *fakeGroup) ChangeInvisible(ctx context.Context, receiptHandle, messageID, topic string, invisible time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastReceipt = receiptHandle
	f.lastID = messageID
	f.lastTopic = topic
	f.invisible = invisible
	if f.changeErr != nil {
		return "", f.changeErr
	}
	return f.newReceipt, nil
}

func (f *fakeGroup) Ack(ctx context.Context, receiptHandle, messageID, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks++
	f.lastReceipt = receiptHandle
	f.lastID = messageID
	f.lastTopic = topic
	return nil
}

func (f *fakeGroup) Subscribe(topic string, sub subscription) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribes++
	if f.subErr != nil {
		return f.subErr
	}
	if f.subs == nil {
		f.subs = map[string]subscription{}
	}
	f.subs[topic] = sub
	return nil
}

func (f *fakeGroup) Close(ctx context.Context) error { return nil }

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
