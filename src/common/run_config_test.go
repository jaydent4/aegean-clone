package common

import "testing"

func TestK6RunPhaseStopPolicy(t *testing.T) {
	if DefaultK6WarmupGracefulStop != "30s" {
		t.Fatalf("DefaultK6WarmupGracefulStop = %q, want %q", DefaultK6WarmupGracefulStop, "30s")
	}
	if K6MeasuredGracefulStop != "0s" {
		t.Fatalf("K6MeasuredGracefulStop = %q, want %q", K6MeasuredGracefulStop, "0s")
	}
}

func TestK6WarmupGracefulStopUsesConfiguredValue(t *testing.T) {
	config := map[string]any{
		"warmup_graceful_stop": "45s",
	}

	if got := K6WarmupGracefulStop(config); got != "45s" {
		t.Fatalf("K6WarmupGracefulStop() = %q, want %q", got, "45s")
	}
}

func TestK6WarmupGracefulStopDefaultsToWarmupDefault(t *testing.T) {
	if got := K6WarmupGracefulStop(map[string]any{}); got != "30s" {
		t.Fatalf("K6WarmupGracefulStop() = %q, want %q", got, "30s")
	}
}
