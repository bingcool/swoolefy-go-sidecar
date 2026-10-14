package rocketmq

import (
	"context"
	"reflect"
	"time"
	"unsafe"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

// proxyRequestTimeout 是 ack / changeInvisible 单次 RPC 的超时。
const proxyRequestTimeout = 3 * time.Second

// proxyConn 是从 SimpleConsumer 里取出的、已经签好名的 proxy 连接。
type proxyConn struct {
	signer    golang.Client
	manager   golang.ClientManager
	endpoints *v2.Endpoints
}

// openProxyConn 用反射取 SimpleConsumer 内部的 cli / clientManager / accessPoint。
// v5.1.2 的 SimpleConsumer.Ack 和 ChangeInvisibleDuration 只接受包内的 *MessageView，
// sidecar 不保存视图（只把 receiptHandle 交给 PHP），所以直接用这条连接调 proxy 的 RPC。
// 升级 SDK 时要重新确认这三个字段名（TestAckReceiptReadsSDKClient 会失败提示）。
func openProxyConn(consumer golang.SimpleConsumer) (proxyConn, error) {
	if consumer == nil {
		return proxyConn{}, errNotReady
	}
	cliVal := consumerField(consumer, "cli")
	if !cliVal.IsValid() || cliVal.IsNil() {
		return proxyConn{}, errNotReady
	}
	signer, ok := cliVal.Interface().(golang.Client)
	if !ok {
		return proxyConn{}, errNotReady
	}
	manager, ok := structField(cliVal, "clientManager").Interface().(golang.ClientManager)
	if !ok || manager == nil {
		return proxyConn{}, errNotReady
	}
	endpoints, ok := structField(cliVal, "accessPoint").Interface().(*v2.Endpoints)
	if !ok || endpoints == nil {
		return proxyConn{}, errNotReady
	}
	return proxyConn{signer: signer, manager: manager, endpoints: endpoints}, nil
}

// ackReceipt 用 receiptHandle 调 Proxy 的 AckMessage。
func ackReceipt(ctx context.Context, consumer golang.SimpleConsumer, group, namespace, receiptHandle, messageID, topic string) error {
	if receiptHandle == "" || messageID == "" || topic == "" || group == "" {
		return errNotReady
	}
	conn, err := openProxyConn(consumer)
	if err != nil {
		return err
	}
	resp, err := conn.manager.AckMessage(conn.signer.Sign(ctx), conn.endpoints, &v2.AckMessageRequest{
		Group: &v2.Resource{Name: group, ResourceNamespace: namespace},
		Topic: &v2.Resource{Name: topic, ResourceNamespace: namespace},
		Entries: []*v2.AckMessageEntry{{
			MessageId:     messageID,
			ReceiptHandle: receiptHandle,
		}},
	}, proxyRequestTimeout)
	if err != nil {
		return err
	}
	return statusError(resp.GetStatus())
}

// changeInvisibleReceipt 用 receiptHandle 调 Proxy 的 ChangeInvisibleDuration，返回新回执。
// invisible 从 proxy 收到请求时起算，不是在原不可见时间上累加。
func changeInvisibleReceipt(ctx context.Context, consumer golang.SimpleConsumer, group, namespace, receiptHandle, messageID, topic string, invisible time.Duration) (string, error) {
	if receiptHandle == "" || messageID == "" || topic == "" || group == "" {
		return "", errNotReady
	}
	conn, err := openProxyConn(consumer)
	if err != nil {
		return "", err
	}
	resp, err := conn.manager.ChangeInvisibleDuration(conn.signer.Sign(ctx), conn.endpoints, &v2.ChangeInvisibleDurationRequest{
		Group:             &v2.Resource{Name: group, ResourceNamespace: namespace},
		Topic:             &v2.Resource{Name: topic, ResourceNamespace: namespace},
		ReceiptHandle:     receiptHandle,
		InvisibleDuration: durationpb.New(invisible),
		MessageId:         messageID,
	}, proxyRequestTimeout)
	if err != nil {
		return "", err
	}
	if err = statusError(resp.GetStatus()); err != nil {
		return "", err
	}
	return resp.GetReceiptHandle(), nil
}

func statusError(status *v2.Status) error {
	if status.GetCode() == v2.Code_OK {
		return nil
	}
	return &golang.ErrRpcStatus{
		Code:    int32(status.GetCode()),
		Message: status.GetMessage(),
	}
}

func consumerField(consumer golang.SimpleConsumer, name string) reflect.Value {
	if consumer == nil {
		return reflect.Value{}
	}
	// ValueOf on an interface parameter returns the concrete value.
	return structField(reflect.ValueOf(consumer), name)
}

func structField(value reflect.Value, name string) reflect.Value {
	if !value.IsValid() {
		return reflect.Value{}
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	field := value.FieldByName(name)
	if !field.IsValid() {
		return reflect.Value{}
	}
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
}
