package rocketmq

import (
	"context"

	golang "github.com/apache/rocketmq-clients/golang/v5"
)

// ackReceipt 是本包里唯一准备调用 SDK Ack 的位置。
//
// github.com/apache/rocketmq-clients/golang/v5 v5.1.2 的 SimpleConsumer.Ack
// 只接受 *MessageView。topic、messageId、endpoints 都不可从包外赋值，
// 也没有用 receiptHandle 构造 MessageView 的导出函数。
// 这里不保存上一轮的 MessageView。在 SDK 提供回执确认之前，确认调用返回固定错误。
func ackReceipt(ctx context.Context, consumer golang.SimpleConsumer, receiptHandle, messageID, topic string) error {
	_ = ctx
	_ = consumer
	_ = receiptHandle
	_ = messageID
	_ = topic
	return errReceiptAckUnsupported
}
