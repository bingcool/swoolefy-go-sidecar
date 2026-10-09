package rocketmq

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"swoolefy-go-sidecar/internal/framework/config"
)

// Config 是 RocketMQ 插件自己的配置。密钥不进入日志。
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	AllowAll  bool
	Topics    []string
}

func Load() (Config, error) {
	cfg := Config{
		Endpoint:  strings.TrimSpace(os.Getenv("SIDECAR_ROCKETMQ_PROXY_ENDPOINT")),
		AccessKey: os.Getenv("SIDECAR_ROCKETMQ_ACCESS_KEY"),
		SecretKey: os.Getenv("SIDECAR_ROCKETMQ_SECRET_KEY"),
	}
	if !validEndpoint(cfg.Endpoint) {
		return cfg, fmt.Errorf("SIDECAR_ROCKETMQ_PROXY_ENDPOINT")
	}
	allowAll, topics, err := parseTopics(os.Getenv("SIDECAR_ROCKETMQ_ALLOWED_TOPICS"))
	if err != nil {
		return cfg, err
	}
	cfg.AllowAll = allowAll
	cfg.Topics = topics
	return cfg, nil
}

func validEndpoint(endpoint string) bool {
	if endpoint == "" || strings.Contains(endpoint, "://") || strings.Contains(endpoint, "/") {
		return false
	}
	host, port, ok := strings.Cut(endpoint, ":")
	if !ok || host == "" || strings.Contains(port, ":") || port == "" {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n >= 1 && n <= 65535
}

func parseTopics(raw string) (bool, []string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "*" {
		return true, nil, nil
	}
	if trimmed == "" {
		return false, nil, nil
	}
	parts, err := config.SplitCSV(trimmed)
	if err != nil {
		return false, nil, err
	}
	for _, part := range parts {
		if strings.Contains(part, "*") {
			return false, nil, fmt.Errorf("SIDECAR_ROCKETMQ_ALLOWED_TOPICS")
		}
	}
	return false, parts, nil
}
