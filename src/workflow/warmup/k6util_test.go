package warmup

import (
	"reflect"
	"testing"
)

func TestRunWarmupThenMeasuredDrainsBeforeMeasured(t *testing.T) {
	var events []string
	err := RunWarmupThenMeasured("1s", "2s", func(duration string, suppressOutput bool) error {
		if suppressOutput {
			events = append(events, "run:"+duration+":suppressed")
			return nil
		}
		events = append(events, "run:"+duration+":measured")
		return nil
	}, func() error {
		events = append(events, "drain")
		return nil
	})
	if err != nil {
		t.Fatalf("RunWarmupThenMeasured returned error: %v", err)
	}

	want := []string{"run:1s:suppressed", "drain", "run:2s:measured"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestRunWarmupThenMeasuredSkipsDrainWithoutWarmup(t *testing.T) {
	var events []string
	err := RunWarmupThenMeasured("0s", "2s", func(duration string, suppressOutput bool) error {
		events = append(events, "run:"+duration)
		return nil
	}, func() error {
		events = append(events, "drain")
		return nil
	})
	if err != nil {
		t.Fatalf("RunWarmupThenMeasured returned error: %v", err)
	}

	want := []string{"run:2s"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}
