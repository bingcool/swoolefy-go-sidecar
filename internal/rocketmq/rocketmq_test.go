package rocketmq

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"

	"swoolefy-go-sidecar/internal/framework/plugin"
)

func TestSendUsesRelativeDelay(t *testing.T) {
	sender := &fakeSender{id: "m1"}
	producer := newProducer(Config{AllowAll: true})
	producer.sender = sender
	producer.ready.Store(true)

	immediate := producer.Send(context.Background(), mustJSON(t, sendRequest{
		Topic: "order", BodyBase64: "aGk=", DelayMs: 0,
	}))
	if immediate.Code != "OK" || sender.delay != 0 || !sender.called {
		t.Fatalf("%+v delay=%s", immediate, sender.delay)
	}
	delayed := producer.Send(context.Background(), mustJSON(t, sendRequest{
		Topic: "order", BodyBase64: "aGk=", DelayMs: 1500,
	}))
	if delayed.Code != "OK" || sender.delay != 1500*time.Millisecond {
		t.Fatalf("%+v delay=%s", delayed, sender.delay)
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
	if ok.Code != "OK" || group.subs["order"] != "*" {
		t.Fatalf("%s %+v", ok.Code, group.subs)
	}
	bad := consumer.Receive(context.Background(), mustJSON(t, receiveRequest{
		Topic: "order", ConsumerGroup: "g", ConsumerTag: "paid", MaxNum: 1, InvisibleMs: 1000,
	}))
	if bad.Code != "INVALID_ARGUMENT" || group.subs["order"] != "*" {
		t.Fatalf("%s %+v", bad.Code, group.subs)
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
}

type fakeSender struct {
	id     string
	delay  time.Duration
	called bool
}

func (f *fakeSender) Send(ctx context.Context, topic string, body []byte, tag, key string, delay time.Duration) (string, error) {
	f.called = true
	f.delay = delay
	return f.id, nil
}

func (f *fakeSender) Close(ctx context.Context) error { return nil }

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
	subs        map[string]string
	block       chan struct{}
	entered     chan struct{}
	err         error
	lastReceipt string
	lastID      string
	lastTopic   string
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
	return nil, nil
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

func (f *fakeGroup) Subscribe(topic, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subs == nil {
		f.subs = map[string]string{}
	}
	f.subs[topic] = tag
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
