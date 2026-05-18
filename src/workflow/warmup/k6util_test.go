package warmup

import (
	"reflect"
	"testing"
)

func TestRunWarmupThenMeasuredDrainsBeforeMeasured(t *testing.T) {
	var events []string
	err := RunWarmupThenMeasured("1s", "45s", "2s", func(phase Phase) error {
		if phase.SuppressOutput {
			events = append(events, "run:"+phase.Duration+":"+phase.GracefulStop+":suppressed")
			return nil
		}
		events = append(events, "run:"+phase.Duration+":"+phase.GracefulStop+":measured")
		return nil
	}, func() error {
		events = append(events, "drain")
		return nil
	})
	if err != nil {
		t.Fatalf("RunWarmupThenMeasured returned error: %v", err)
	}

	want := []string{"run:1s:45s:suppressed", "drain", "run:2s:0s:measured"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestRunWarmupThenMeasuredSkipsDrainWithoutWarmup(t *testing.T) {
	var events []string
	err := RunWarmupThenMeasured("0s", "30s", "2s", func(phase Phase) error {
		events = append(events, "run:"+phase.Duration+":"+phase.GracefulStop)
		return nil
	}, func() error {
		events = append(events, "drain")
		return nil
	})
	if err != nil {
		t.Fatalf("RunWarmupThenMeasured returned error: %v", err)
	}

	want := []string{"run:2s:0s"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}
