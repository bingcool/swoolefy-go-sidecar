package log

import "log"

// Access 只记录调用元数据，不记录 payload 和密钥。
func Access(sidecar, method, requestID, code string, durationMs int64) {
	log.Printf("sidecar=%s method=%s requestId=%s code=%s durationMs=%d", sidecar, method, requestID, code, durationMs)
}

// Error 记录不含正文的故障。message 必须是调用方写好的短句。
func Error(sidecar, message string) {
	log.Printf("sidecar=%s error=%s", sidecar, message)
}
