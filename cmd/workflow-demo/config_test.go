package main

import (
	"testing"
	"time"
)

func TestLoadConfigUsesLocalDefaults(t *testing.T) {
	configured, err := loadConfig(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if configured.HTTPAddress != defaultDemoAddress ||
		configured.WorkFlowAPIURL != defaultWorkFlowAPIURL ||
		configured.ShutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("unexpected defaults: %+v", configured)
	}
}

func TestLoadConfigAppliesOverrides(t *testing.T) {
	values := map[string]string{
		"DEMO_HTTP_ADDR":        "127.0.0.1:6090",
		"WORKFLOW_API_URL":      "http://127.0.0.1:6080",
		"DEMO_SHUTDOWN_TIMEOUT": "20s",
	}
	configured, err := loadConfig(func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	})
	if err != nil {
		t.Fatal(err)
	}
	if configured.HTTPAddress != values["DEMO_HTTP_ADDR"] ||
		configured.WorkFlowAPIURL != values["WORKFLOW_API_URL"] ||
		configured.ShutdownTimeout != 20*time.Second {
		t.Fatalf("unexpected overrides: %+v", configured)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	tests := map[string]map[string]string{
		"relative URL":     {"WORKFLOW_API_URL": "/workflow"},
		"unsupported URL":  {"WORKFLOW_API_URL": "ftp://localhost"},
		"invalid duration": {"DEMO_SHUTDOWN_TIMEOUT": "later"},
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
