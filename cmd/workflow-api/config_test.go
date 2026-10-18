package main

import (
	"testing"
	"time"
)

func TestLoadConfigUsesDefaults(t *testing.T) {
	configured, err := loadConfig(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if configured.HTTPAddress != defaultAddress || configured.LeaseDuration != defaultLeaseDuration {
		t.Fatalf("unexpected defaults: %+v", configured)
	}
}

func TestLoadConfigAppliesOperationalOverrides(t *testing.T) {
	values := map[string]string{
		"HTTP_ADDR":           "127.0.0.1:6080",
		"LEASE_DURATION":      "45s",
		"RETRY_BASE":          "2s",
		"RETRY_MAXIMUM":       "1m",
		"RECOVERY_INTERVAL":   "500ms",
		"RECOVERY_BATCH_SIZE": "25",
		"SHUTDOWN_TIMEOUT":    "20s",
	}
	configured, err := loadConfig(func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	})
	if err != nil {
		t.Fatal(err)
	}
	if configured.HTTPAddress != "127.0.0.1:6080" ||
		configured.LeaseDuration != 45*time.Second ||
		configured.RecoveryInterval != 500*time.Millisecond ||
		configured.RecoveryBatchSize != 25 ||
		configured.ShutdownTimeout != 20*time.Second {
		t.Fatalf("unexpected overrides: %+v", configured)
	}
}

func TestLoadConfigRejectsUnsafeValues(t *testing.T) {
	tests := map[string]map[string]string{
		"invalid duration": {"LEASE_DURATION": "soon"},
		"zero batch":       {"RECOVERY_BATCH_SIZE": "0"},
		"inverted retry":   {"RETRY_BASE": "1m", "RETRY_MAXIMUM": "1s"},
	}
	for name, values := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := loadConfig(func(name string) (string, bool) {
				value, exists := values[name]
				return value, exists
			})
			if err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}
