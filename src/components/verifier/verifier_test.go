package verifier

import (
	"net"
	"net/http"
	"testing"
	"time"

	"aegean/common"
)

type verifierTestServer struct {
	server   *http.Server
	listener net.Listener
	received chan map[string]any
}

func startVerifierTestServer(t *testing.T) *verifierTestServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:8000")
	if err != nil {
		t.Fatalf("failed to listen on 127.0.0.1:8000: %v", err)
	}

	ts := &verifierTestServer{
		server:   &http.Server{},
		listener: listener,
		received: make(chan map[string]any, 128),
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

func (ts *verifierTestServer) close() {
	_ = ts.server.Close()
	_ = ts.listener.Close()
}

func expectMessage(t *testing.T, ch <-chan map[string]any, predicate func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(750 * time.Millisecond)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for expected message")
		}
		select {
		case msg := <-ch:
			if predicate(msg) {
				return msg
			}
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for expected message")
		}
	}
}

func TestVerifierPreprepareThreshold(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("v1", []string{"v1", "v1"}, []string{"e1", "e2"}, execCh)

	first := v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "e1",
	})
	if first["status"] != "processed_or_buffered" {
		t.Fatalf("expected processed_or_buffered after first verify, got %v", first["status"])
	}

	second := v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "e2",
	})
	if second["status"] != "processed_or_buffered" {
		t.Fatalf("expected processed_or_buffered after threshold, got %v", second["status"])
	}
}

func TestVerifierPrepareCommitSendsVerifyResponse(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("127.0.0.1", []string{"127.0.0.1", "127.0.0.1"}, []string{"127.0.0.1"}, execCh)

	// Reach preprepare from exec verifies.
	v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "e1",
	})
	v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "e2",
	})

	// Reach prepared (phase quorum is 2 with u=1,r=0).
	v.HandlePrepareMessage(map[string]any{
		"type":        "prepare",
		"view":        1,
		"seq_num":     1,
		"token":       "tokA",
		"verifier_id": "v2",
	})

	// Reach committed.
	resp := v.HandleCommitMessage(map[string]any{
		"type":        "commit",
		"view":        1,
		"seq_num":     1,
		"token":       "tokA",
		"verifier_id": "v2",
	})
	if resp["status"] != "committed" {
		t.Fatalf("expected committed status, got %v", resp["status"])
	}

	msg := expectMessage(t, execCh, func(m map[string]any) bool {
		msgType, _ := m["type"].(string)
		return msgType == "verify_response"
	})
	if msg["token"] != "tokA" {
		t.Fatalf("expected token tokA, got %v", msg["token"])
	}
	if force, _ := msg["force_sequential"].(bool); force {
		t.Fatalf("expected force_sequential=false in happy path")
	}
}

func TestVerifierTimeoutTriggersViewChangeResponse(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("127.0.0.1", []string{"127.0.0.1", "127.0.0.1"}, []string{"127.0.0.1"}, execCh)
	v.viewChangeTimeout = 50 * time.Millisecond

	// One verify is below preprepare threshold, should timeout to no-agreement.
	resp := v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "e1",
	})
	if resp["status"] != "processed_or_buffered" {
		t.Fatalf("expected processed_or_buffered status, got %v", resp["status"])
	}
	time.Sleep(100 * time.Millisecond)
	// Simulate second verifier contributing a view-change report so the new-view
	// primary can build a quorum certificate.
	v.HandleViewChangeMessage(map[string]any{
		"type":         "view_change",
		"target_view":  2,
		"verifier_id":  "v2",
		"prepared_seq": 0,
		"token":        "",
	})

	msg := expectMessage(t, execCh, func(m map[string]any) bool {
		msgType, _ := m["type"].(string)
		if msgType != "verify_response" {
			return false
		}
		force, _ := m["force_sequential"].(bool)
		return force
	})
	newView := common.GetInt(msg, "view")
	if newView <= 1 {
		t.Fatalf("expected increased view after timeout, got %d", newView)
	}
}

func TestVerifierFastNoAgreementWhenAllExecVotesDiffer(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("127.0.0.1", []string{"127.0.0.1", "127.0.0.1"}, []string{"127.0.0.1"}, execCh)
	v.viewChangeTimeout = 5 * time.Second // Ensure test hits fast-path, not timeout.

	_ = v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "e1",
	})
	_ = v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokB",
		"prev_hash": "",
		"exec_id":   "e2",
	})
	resp := v.HandleVerifyMessage(map[string]any{
		"type":      "verify",
		"view":      1,
		"seq_num":   1,
		"token":     "tokC",
		"prev_hash": "",
		"exec_id":   "e3",
	})
	if resp["status"] != "processed_or_buffered" {
		t.Fatalf("expected processed_or_buffered, got %v", resp["status"])
	}

	// Simulate second verifier report so new-view certificate can be formed.
	v.HandleViewChangeMessage(map[string]any{
		"type":         "view_change",
		"target_view":  2,
		"verifier_id":  "v2",
		"prepared_seq": 0,
		"token":        "",
	})

	msg := expectMessage(t, execCh, func(m map[string]any) bool {
		msgType, _ := m["type"].(string)
		if msgType != "verify_response" {
			return false
		}
		force, _ := m["force_sequential"].(bool)
		return force
	})
	newView := common.GetInt(msg, "view")
	if newView <= 1 {
		t.Fatalf("expected increased view from fast no-agreement path, got %d", newView)
	}
}
