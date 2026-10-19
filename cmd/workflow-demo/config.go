package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	defaultDemoAddress     = "127.0.0.1:5090"
	defaultWorkFlowAPIURL  = "http://127.0.0.1:5080"
	defaultShutdownTimeout = 10 * time.Second
)

type config struct {
	HTTPAddress     string
	WorkFlowAPIURL  string
	ShutdownTimeout time.Duration
}

type environmentLookup func(string) (string, bool)

func loadConfig(lookup environmentLookup) (config, error) {
	configured := config{
		HTTPAddress:     valueOrDefault(lookup, "DEMO_HTTP_ADDR", defaultDemoAddress),
		WorkFlowAPIURL:  valueOrDefault(lookup, "WORKFLOW_API_URL", defaultWorkFlowAPIURL),
		ShutdownTimeout: defaultShutdownTimeout,
	}
	if value, exists := lookup("DEMO_SHUTDOWN_TIMEOUT"); exists && strings.TrimSpace(value) != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return config{}, fmt.Errorf("DEMO_SHUTDOWN_TIMEOUT must be a positive Go duration")
		}
		configured.ShutdownTimeout = parsed
	}
	parsedURL, err := url.Parse(configured.WorkFlowAPIURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" ||
		(parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return config{}, fmt.Errorf("WORKFLOW_API_URL must be an absolute HTTP(S) URL")
	}
	return configured, nil
}

func valueOrDefault(lookup environmentLookup, name, fallback string) string {
	if value, exists := lookup(name); exists && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}
