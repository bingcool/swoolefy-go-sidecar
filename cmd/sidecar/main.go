package main

import (
	"fmt"
	"os"

	"swoolefy-go-sidecar/internal/framework"
	"swoolefy-go-sidecar/internal/framework/config"
	"swoolefy-go-sidecar/internal/framework/plugin"
	"swoolefy-go-sidecar/internal/rocketmq"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "sidecar config: %v\n", err)
		os.Exit(1)
	}
	plugins, err := pluginsFrom(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sidecar plugin: %v\n", err)
		os.Exit(1)
	}
	if err = framework.Run(cfg, plugins); err != nil {
		fmt.Fprintf(os.Stderr, "sidecar: %v\n", err)
		os.Exit(1)
	}
}

func pluginsFrom(cfg config.Config) ([]plugin.Plugin, error) {
	var plugins []plugin.Plugin
	for _, name := range cfg.Plugins {
		switch name {
		case "rocketmq":
			item, err := rocketmq.NewFromEnv()
			if err != nil {
				return nil, err
			}
			plugins = append(plugins, item)
		default:
			return nil, fmt.Errorf("unknown plugin %s", name)
		}
	}
	return plugins, nil
}
