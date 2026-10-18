package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddress       = "127.0.0.1:5080"
	defaultDatabaseURL   = "postgres://workflow:workflow_dev@localhost:5432/workflow?sslmode=disable"
	defaultLeaseDuration = 30 * time.Second
	defaultRetryBase     = 5 * time.Second
	defaultRetryMaximum  = 5 * time.Minute
	defaultRecoveryEvery = 2 * time.Second
	defaultRecoveryBatch = 100
	defaultShutdownWait  = 10 * time.Second
)

type config struct {
	HTTPAddress       string
	DatabaseURL       string
	LeaseDuration     time.Duration
	RetryBase         time.Duration
	RetryMaximum      time.Duration
	RecoveryInterval  time.Duration
	RecoveryBatchSize int
	ShutdownTimeout   time.Duration
	Environment       string
	ServiceVersion    string
}

type environmentLookup func(string) (string, bool)

func loadConfig(lookup environmentLookup) (config, error) {
	configured := config{
		HTTPAddress:       valueOrDefault(lookup, "HTTP_ADDR", defaultAddress),
		DatabaseURL:       valueOrDefault(lookup, "DATABASE_URL", defaultDatabaseURL),
		Environment:       valueOrDefault(lookup, "WORKFLOW_ENVIRONMENT", "development"),
		ServiceVersion:    valueOrDefault(lookup, "WORKFLOW_VERSION", "dev"),
		LeaseDuration:     defaultLeaseDuration,
		RetryBase:         defaultRetryBase,
		RetryMaximum:      defaultRetryMaximum,
		RecoveryInterval:  defaultRecoveryEvery,
		RecoveryBatchSize: defaultRecoveryBatch,
		ShutdownTimeout:   defaultShutdownWait,
	}

	durationSettings := []struct {
		name        string
		destination *time.Duration
	}{
		{name: "LEASE_DURATION", destination: &configured.LeaseDuration},
		{name: "RETRY_BASE", destination: &configured.RetryBase},
		{name: "RETRY_MAXIMUM", destination: &configured.RetryMaximum},
		{name: "RECOVERY_INTERVAL", destination: &configured.RecoveryInterval},
		{name: "SHUTDOWN_TIMEOUT", destination: &configured.ShutdownTimeout},
	}
	for _, setting := range durationSettings {
		value, exists := lookup(setting.name)
		if !exists || strings.TrimSpace(value) == "" {
			continue
		}
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return config{}, fmt.Errorf("%s must be a positive Go duration", setting.name)
		}
		*setting.destination = parsed
	}

	if value, exists := lookup("RECOVERY_BATCH_SIZE"); exists && strings.TrimSpace(value) != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return config{}, fmt.Errorf("RECOVERY_BATCH_SIZE must be a positive integer")
		}
		configured.RecoveryBatchSize = parsed
	}
	if configured.RetryMaximum < configured.RetryBase {
		return config{}, fmt.Errorf("RETRY_MAXIMUM must be greater than or equal to RETRY_BASE")
	}
	return configured, nil
}

func valueOrDefault(lookup environmentLookup, name, fallback string) string {
	if value, exists := lookup(name); exists && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}
