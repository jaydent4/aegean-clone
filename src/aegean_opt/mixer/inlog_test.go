package mixer

import (
	"testing"
	"time"

	"aegean/aegean_opt/protocol"
)

func TestMixerInLogBuildsDeterministicDependencyPlan(t *testing.T) {
	out := make(chan map[string]any, 8)
	m := NewMixer("m1", out, map[string]any{
		"worker_count": 2,
	})

	m.HandleInLogRequestMessage(inlogRequest(1, 0, "r1", "write", map[string]any{"key": "A"}))
	m.HandleInLogRequestMessage(inlogRequest(1, 1, "r2", "write", map[string]any{"key": "B"}))
	m.HandleInLogRequestMessage(inlogRequest(1, 2, "r3", "read_write", map[string]any{
		"read_key":  "A",
		"write_key": "B",
	}))

	first := recvMixerMessage(t, out)
	second := recvMixerMessage(t, out)
	third := recvMixerMessage(t, out)

	if first[protocol.FieldWorker] != 0 {
		t.Fatalf("expected r1 on worker 0, got %v", first[protocol.FieldWorker])
	}
	if second[protocol.FieldWorker] != 1 {
		t.Fatalf("expected r2 on worker 1, got %v", second[protocol.FieldWorker])
	}
	deps, ok := third[protocol.FieldDependsOn].([]string)
	if !ok {
		t.Fatalf("expected dependency list, got %T", third[protocol.FieldDependsOn])
	}
	wantDeps := []string{"1:0", "1:1"}
	if len(deps) != len(wantDeps) {
		t.Fatalf("expected deps %v, got %v", wantDeps, deps)
	}
	for i := range wantDeps {
		if deps[i] != wantDeps[i] {
			t.Fatalf("expected deps %v, got %v", wantDeps, deps)
		}
	}
	if third[protocol.FieldWorker] != 1 {
		t.Fatalf("expected r3 assigned to highest-index dependency worker 1, got %v", third[protocol.FieldWorker])
	}
}

func inlogRequest(seq int, index int, requestID string, op string, opPayload map[string]any) map[string]any {
	request := map[string]any{
		"request_id": requestID,
		"op":         op,
		"op_payload": opPayload,
	}
	return map[string]any{
		protocol.FieldType:        protocol.MessageTypeInLogRequest,
		protocol.FieldSeqNum:      seq,
		protocol.FieldRequestIdx:  index,
		protocol.FieldRequestID:   requestID,
		protocol.FieldRequest:     request,
		protocol.FieldNDSeed:      int64(index + 10),
		protocol.FieldNDTimestamp: float64(index) + 0.5,
	}
}

func recvMixerMessage(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for mixer message")
		return nil
	}
}
