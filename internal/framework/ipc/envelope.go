package ipc

import "encoding/json"

// Request 与 PHP SidecarRequestDto::toArray() 的驼峰键一致。
type Request struct {
	RequestID string          `json:"requestId"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload"`
	TimeoutMs uint32          `json:"timeoutMs"`
}

// Response 的 payload 不加 omitempty，空对象也要写出来。
type Response struct {
	RequestID string `json:"requestId"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Payload   any    `json:"payload"`
}

func KnownCode(code string) bool {
	switch code {
	case "OK", "INVALID_ARGUMENT", "METHOD_NOT_FOUND", "UNAVAILABLE", "OVERLOADED", "TIMEOUT", "INTERNAL", "OUTCOME_UNKNOWN":
		return true
	default:
		return false
	}
}
