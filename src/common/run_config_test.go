package common

import "testing"

func TestK6RunPhaseStopPolicy(t *testing.T) {
	if K6WarmupGracefulStop != "30s" {
		t.Fatalf("K6WarmupGracefulStop = %q, want %q", K6WarmupGracefulStop, "30s")
	}
	if K6MeasuredGracefulStop != "0s" {
		t.Fatalf("K6MeasuredGracefulStop = %q, want %q", K6MeasuredGracefulStop, "0s")
	}
}
