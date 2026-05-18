package common

import "testing"

func TestK6GracefulStopUsesConfiguredValue(t *testing.T) {
	config := map[string]any{
		"k6_graceful_stop": "0s",
	}

	if got := K6GracefulStop(config); got != "0s" {
		t.Fatalf("K6GracefulStop() = %q, want %q", got, "0s")
	}
}

func TestK6GracefulStopDefaultsToK6Default(t *testing.T) {
	if got := K6GracefulStop(map[string]any{}); got != "30s" {
		t.Fatalf("K6GracefulStop() = %q, want %q", got, "30s")
	}
}
