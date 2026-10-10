package rocketmq

import (
	"context"
	"reflect"
	"time"
	"unsafe"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// ackReceipt 用 receiptHandle 调 Proxy 的 AckMessage。
// v5.1.2 的 SimpleConsumer.Ack 只接受包内的 *MessageView，这里不保存它，
// 而是复用这个 consumer 已经签好名的连接。
func ackReceipt(ctx context.Context, consumer golang.SimpleConsumer, group, namespace, receiptHandle, messageID, topic string) error {
	if consumer == nil || receiptHandle == "" || messageID == "" || topic == "" || group == "" {
		return errNotReady
	}
	cliVal := consumerField(consumer, "cli")
	if !cliVal.IsValid() || cliVal.IsNil() {
		return errNotReady
	}
	signer, ok := cliVal.Interface().(golang.Client)
	if !ok {
		return errNotReady
	}
	managerVal := structField(cliVal, "clientManager")
	manager, ok := managerVal.Interface().(golang.ClientManager)
	if !ok || manager == nil {
		return errNotReady
	}
	endpointsVal := structField(cliVal, "accessPoint")
	endpoints, ok := endpointsVal.Interface().(*v2.Endpoints)
	if !ok || endpoints == nil {
		return errNotReady
	}
	resp, err := manager.AckMessage(signer.Sign(ctx), endpoints, &v2.AckMessageRequest{
		Group: &v2.Resource{Name: group, ResourceNamespace: namespace},
		Topic: &v2.Resource{Name: topic, ResourceNamespace: namespace},
		Entries: []*v2.AckMessageEntry{{
			MessageId:     messageID,
			ReceiptHandle: receiptHandle,
		}},
	}, 3*time.Second)
	if err != nil {
		return err
	}
	if resp.GetStatus().GetCode() != v2.Code_OK {
		return &golang.ErrRpcStatus{
			Code:    int32(resp.GetStatus().GetCode()),
			Message: resp.GetStatus().GetMessage(),
		}
	}
	return nil
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
