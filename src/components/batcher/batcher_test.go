package batcher

import (
	"testing"
	"time"

	"aegean/common"
)

func makeReq(id int) map[string]any {
	return map[string]any{"request_id": id}
}

// Batches flush immediately when reaching batchSize
func TestBatcherFlushOnSize(t *testing.T) {
	outCh := make(chan map[string]any, 1)
	b := NewBatcher("b", outCh, []string{"local"}, "local", true)
	b.batchSize = 2
	b.batchTimeout = 1 * time.Second

	b.HandleRequestMessage(makeReq(1))
	b.HandleRequestMessage(makeReq(2))

	select {
	case msg := <-outCh:
		if msg["type"] != "batch" {
			t.Fatalf("expected batch message, got %v", msg["type"])
		}
		if seq := common.GetInt(msg, "seq_num"); seq != 1 {
			t.Fatalf("expected seq_num 1, got %d", seq)
		}
		reqs, ok := msg["requests"].([]map[string]any)
		if !ok || len(reqs) != 2 {
			t.Fatalf("expected 2 requests, got %T len=%d", msg["requests"], len(reqs))
		}
		if msg["nd_seed"] == nil || msg["nd_timestamp"] == nil {
			t.Fatalf("expected nd fields present")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for batch")
	}
}

// Batches flush after timeout even if batchSize is not reached
func TestBatcherFlushOnTimeout(t *testing.T) {
	outCh := make(chan map[string]any, 1)
	b := NewBatcher("b", outCh, []string{"local"}, "local", true)
	b.batchSize = 10
	b.batchTimeout = 10 * time.Millisecond

	b.StartBatchFlusher()
	b.HandleRequestMessage(makeReq(1))

	select {
	case msg := <-outCh:
		if seq := common.GetInt(msg, "seq_num"); seq != 1 {
			t.Fatalf("expected seq_num 1, got %d", seq)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for timeout flush")
	}
}

// Non-primary batchers ignore incoming requests
func TestBatcherNonPrimaryIgnored(t *testing.T) {
	outCh := make(chan map[string]any, 1)
	b := NewBatcher("b", outCh, []string{"local"}, "local", false)
	b.batchSize = 1

	resp := b.HandleRequestMessage(makeReq(1))
	if resp["status"] != "ignored_non_primary" {
		t.Fatalf("expected ignored_non_primary, got %v", resp["status"])
	}

	select {
	case msg := <-outCh:
		t.Fatalf("unexpected batch emitted: %v", msg)
	case <-time.After(50 * time.Millisecond):
	}
}
