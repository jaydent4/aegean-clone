package shim

import (
	"net"
	"net/http"
	"testing"
	"time"

	"aegean/common"
)

type shimTestServer struct {
	server   *http.Server
	listener net.Listener
	received chan map[string]any
}

func startShimTestServer(t *testing.T) *shimTestServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:8000")
	if err != nil {
		t.Fatalf("failed to listen on 127.0.0.1:8000: %v", err)
	}

	ts := &shimTestServer{
		server:   &http.Server{},
		listener: listener,
		received: make(chan map[string]any, 64),
	}

	ts.server.Handler = common.MakeHandler(func(req map[string]any) map[string]any {
		ts.received <- req
		return map[string]any{"status": "ok"}
	})

	go func() {
		_ = ts.server.Serve(listener)
	}()

	return ts
}

func (ts *shimTestServer) close() {
	_ = ts.server.Close()
	_ = ts.listener.Close()
}

func expectShimMessage(t *testing.T, ch <-chan map[string]any, wantType string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(750 * time.Millisecond)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for message type %q", wantType)
		}
		select {
		case msg := <-ch:
			if wantType == "" {
				return msg
			}
			if got, _ := msg["type"].(string); got == wantType {
				return msg
			}
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for message type %q", wantType)
		}
	}
}

func expectNoShimMessage(t *testing.T, ch <-chan map[string]any, wait time.Duration) {
	t.Helper()
	select {
	case msg := <-ch:
		t.Fatalf("unexpected message received: %v", msg)
	case <-time.After(wait):
		return
	}
}

// Forwards a client request only after quorum of senders is reached
func TestShimForwardsOnQuorum(t *testing.T) {
	batcherCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, nil, nil, nil, true, 2)

	first := shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r1",
		"sender":     "clientA",
	})
	if first["status"] != "waiting_for_quorum" {
		t.Fatalf("expected waiting_for_quorum, got %v", first["status"])
	}
	expectNoShimMessage(t, batcherCh, 100*time.Millisecond)

	second := shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r1",
		"sender":     "clientB",
	})
	if second["status"] != "forwarded_to_mid_execs" {
		t.Fatalf("expected forwarded_to_mid_execs, got %v", second["status"])
	}

	msg := expectShimMessage(t, batcherCh, "")
	if msg["request_id"] != "r1" {
		t.Fatalf("expected forwarded request_id r1, got %v", msg["request_id"])
	}
}

// Duplicate senders do not advance quorum
func TestShimIgnoresDuplicateSenderUntilQuorum(t *testing.T) {
	batcherCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, nil, nil, nil, true, 2)

	first := shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r2",
		"sender":     "clientA",
	})
	if first["status"] != "waiting_for_quorum" {
		t.Fatalf("expected waiting_for_quorum, got %v", first["status"])
	}

	second := shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r2",
		"sender":     "clientA",
	})
	if second["status"] != "waiting_for_quorum" {
		t.Fatalf("expected waiting_for_quorum for duplicate, got %v", second["status"])
	}
	expectNoShimMessage(t, batcherCh, 100*time.Millisecond)

	third := shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r2",
		"sender":     "clientB",
	})
	if third["status"] != "forwarded_to_mid_execs" {
		t.Fatalf("expected forwarded_to_mid_execs, got %v", third["status"])
	}

	expectShimMessage(t, batcherCh, "request")
}

// Response from exec is broadcast to all configured clients
func TestShimBroadcastsResponseToClients(t *testing.T) {
	ts := startShimTestServer(t)
	defer ts.close()

	batcherCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, nil, []string{"127.0.0.1", "127.0.0.1"}, nil, true, 2)

	resp := shim.HandleOutgoingResponse(map[string]any{
		"type":       "response",
		"request_id": "r3",
		"response":   map[string]any{"status": "ok"},
	})
	if resp["status"] != "response_broadcast" {
		t.Fatalf("expected response_broadcast, got %v", resp["status"])
	}

	msg1 := expectShimMessage(t, ts.received, "response")
	msg2 := expectShimMessage(t, ts.received, "response")
	if msg1["request_id"] != "r3" || msg2["request_id"] != "r3" {
		t.Fatalf("expected response request_id r3, got %v and %v", msg1["request_id"], msg2["request_id"])
	}
}

// Missing type defaults to a client request and follows quorum forwarding
func TestShimDefaultsToRequestType(t *testing.T) {
	batcherCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, nil, nil, nil, true, 2)

	shim.HandleRequestMessage(map[string]any{
		"request_id": "r4",
		"sender":     "clientA",
	})
	shim.HandleRequestMessage(map[string]any{
		"request_id": "r4",
		"sender":     "clientB",
	})

	msg := expectShimMessage(t, batcherCh, "")
	if msg["request_id"] != "r4" {
		t.Fatalf("expected forwarded request_id r4, got %v", msg["request_id"])
	}
	if _, ok := msg["type"]; ok {
		t.Fatalf("expected forwarded payload without explicit type, got %v", msg["type"])
	}
}

// Different request IDs maintain independent quorums
func TestShimIndependentQuorumsPerRequestID(t *testing.T) {
	batcherCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, nil, nil, nil, true, 2)

	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r5",
		"sender":     "clientA",
	})
	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r6",
		"sender":     "clientA",
	})

	expectNoShimMessage(t, batcherCh, 100*time.Millisecond)

	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r6",
		"sender":     "clientB",
	})
	msg := expectShimMessage(t, batcherCh, "request")
	if msg["request_id"] != "r6" {
		t.Fatalf("expected forwarded request_id r6, got %v", msg["request_id"])
	}

	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r5",
		"sender":     "clientB",
	})
	msg2 := expectShimMessage(t, batcherCh, "request")
	if msg2["request_id"] != "r5" {
		t.Fatalf("expected forwarded request_id r5, got %v", msg2["request_id"])
	}
}

// Interleaved senders across requests still forward each at quorum
func TestShimInterleavedSendersAcrossRequests(t *testing.T) {
	batcherCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, nil, nil, nil, true, 2)

	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r7",
		"sender":     "clientA",
	})
	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r8",
		"sender":     "clientA",
	})
	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r7",
		"sender":     "clientB",
	})

	msg := expectShimMessage(t, batcherCh, "request")
	if msg["request_id"] != "r7" {
		t.Fatalf("expected forwarded request_id r7, got %v", msg["request_id"])
	}

	shim.HandleRequestMessage(map[string]any{
		"type":       "request",
		"request_id": "r8",
		"sender":     "clientB",
	})
	msg2 := expectShimMessage(t, batcherCh, "request")
	if msg2["request_id"] != "r8" {
		t.Fatalf("expected forwarded request_id r8, got %v", msg2["request_id"])
	}
}

// Incoming nested responses are quorum-gated in shim before forwarding to exec.
func TestShimForwardsNestedResponseAfterQuorum(t *testing.T) {
	batcherCh := make(chan map[string]any, 16)
	execCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, execCh, nil, nil, true, 2)

	first := shim.HandleIncomingResponse(map[string]any{
		"type":       "response",
		"request_id": "r9",
		"sender":     "node7",
		"response":   map[string]any{"status": "ok", "value": "a"},
	})
	if first["status"] != "waiting_for_quorum" {
		t.Fatalf("expected waiting_for_quorum, got %v", first["status"])
	}
	expectNoShimMessage(t, execCh, 100*time.Millisecond)

	second := shim.HandleIncomingResponse(map[string]any{
		"type":       "response",
		"request_id": "r9",
		"sender":     "node8",
		"response":   map[string]any{"status": "ok", "value": "a"},
	})
	if second["status"] != "forwarded_nested_response" {
		t.Fatalf("expected forwarded_nested_response, got %v", second["status"])
	}
	forwarded := expectShimMessage(t, execCh, "response")
	if forwarded["request_id"] != "r9" {
		t.Fatalf("expected forwarded request_id r9, got %v", forwarded["request_id"])
	}
	if got, _ := forwarded["shim_quorum_aggregated"].(bool); !got {
		t.Fatalf("expected shim_quorum_aggregated=true")
	}
}

// OHA mode does not wait for request quorum before processing.
func TestShimClientOHADoesNotWaitForRequestQuorum(t *testing.T) {
	batcherCh := make(chan map[string]any, 16)
	shim := NewShim("shim", batcherCh, nil, nil, nil, true, 2)

	resultCh := make(chan map[string]any, 1)
	go func() {
		resultCh <- shim.HandleRequestMessage(map[string]any{
			"type":       "request",
			"request_id": "r_no_quorum",
			"sender":     "clientA",
			"is_client_oha": true,
		})
	}()

	forwarded := expectShimMessage(t, batcherCh, "request")
	if forwarded["request_id"] != "r_no_quorum" {
		t.Fatalf("expected forwarded request_id r_no_quorum, got %v", forwarded["request_id"])
	}

	out := shim.HandleOutgoingResponse(map[string]any{
		"type":       "response",
		"request_id": "r_no_quorum",
		"is_client_oha": true,
		"response":   map[string]any{"status": "ok"},
	})
	if out["status"] != "response_returned_inline" {
		t.Fatalf("expected response_returned_inline, got %v", out["status"])
	}

	select {
	case resp := <-resultCh:
		if got, _ := resp["type"].(string); got != "response" {
			t.Fatalf("expected inline response type, got %v", resp["type"])
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatalf("timed out waiting for inline OHA response")
	}
}
