package rocketmq

import (
	"errors"
	"strings"

	golang "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
)

// clientRejection 判断 proxy 是否因为请求内容拒绝。返回值是状态码名，例如 ILLEGAL_MESSAGE_GROUP。
//
// 只认三段：400xx（参数不合法、消息类型和 Topic 不匹配、回执失效等）、
// 413xx（消息体过大）、431xx（属性过大）。这些错误换连接重试也不会成功。
// 401/403（鉴权）、404（Topic 或消费组不存在）、429（限流）不在其中，仍按不可用处理。
func clientRejection(err error) (string, bool) {
	var rpc *golang.ErrRpcStatus
	if !errors.As(err, &rpc) {
		return "", false
	}
	code := rpc.Code
	if !(code >= 40000 && code < 40100) && !(code >= 41300 && code < 41400) && !(code >= 43100 && code < 43200) {
		return "", false
	}
	name, ok := v2.Code_name[code]
	if !ok {
		name = "BAD_REQUEST"
	}
	return name, true
}

// filterParseFailed 判断 receive 失败是不是因为 SQL92 表达式解析不了。
// RocketMQ 5.3.2 的 proxy 不返回 ILLEGAL_FILTER_EXPRESSION，而是 INTERNAL_SERVER_ERROR 加这句文字；
// 不识别出来就会被当成 UNAVAILABLE，PHP 以为可以重试，实际上改配置之前永远失败。
func filterParseFailed(err error) bool {
	var rpc *golang.ErrRpcStatus
	if !errors.As(err, &rpc) {
		return false
	}
	if rpc.Code == int32(v2.Code_ILLEGAL_FILTER_EXPRESSION) {
		return true
	}
	return rpc.Code == int32(v2.Code_INTERNAL_SERVER_ERROR) && strings.Contains(rpc.Message, "parse the consumer's subscription")
}
