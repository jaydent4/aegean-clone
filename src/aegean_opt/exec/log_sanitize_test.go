package exec

import "testing"

func TestTruncateLogStringCapsAtTwentyChars(t *testing.T) {
	value := truncateLogString("1234567890123456789012345")
	if value != "12345678901234567890" {
		t.Fatalf("expected 20-char truncated value, got %q", value)
	}
}

func TestTruncateLogStringMapLeavesOriginalUntouched(t *testing.T) {
	original := map[string]string{
		"large": "abcdefghijklmnopqrstuvwxyz",
		"small": "short",
	}

	truncated := truncateLogStringMap(original)

	if truncated["large"] != "abcdefghijklmnopqrst" {
		t.Fatalf("expected large value to be truncated, got %q", truncated["large"])
	}
	if truncated["small"] != "short" {
		t.Fatalf("expected small value to remain intact, got %q", truncated["small"])
	}
	if original["large"] != "abcdefghijklmnopqrstuvwxyz" {
		t.Fatalf("expected original map to remain untouched, got %q", original["large"])
	}
}

func TestTruncateLogValueTruncatesNestedOutputValues(t *testing.T) {
	outputs := []map[string]any{
		{
			"request_id": "r1",
			"read_value": "abcdefghijklmnopqrstuvwxyz",
			"writes": map[string]any{
				"x": "1234567890123456789012345",
			},
			"reads": []string{"abcdefghijklmnopqrstuvwxyz"},
		},
	}

	truncated := truncateLogValue(outputs).([]map[string]any)
	writes := truncated[0]["writes"].(map[string]any)
	reads := truncated[0]["reads"].([]string)

	if truncated[0]["read_value"] != "abcdefghijklmnopqrst" {
		t.Fatalf("expected read_value to be truncated, got %q", truncated[0]["read_value"])
	}
	if writes["x"] != "12345678901234567890" {
		t.Fatalf("expected nested write value to be truncated, got %q", writes["x"])
	}
	if reads[0] != "abcdefghijklmnopqrst" {
		t.Fatalf("expected nested read value to be truncated, got %q", reads[0])
	}
	if outputs[0]["read_value"] != "abcdefghijklmnopqrstuvwxyz" {
		t.Fatalf("expected original output to remain untouched, got %q", outputs[0]["read_value"])
	}
}
