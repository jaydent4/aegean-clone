package nodes

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

func (ts *verifierTestServer) close() {
	_ = ts.server.Close()
	_ = ts.listener.Close()
}

func expectVerifierMessage(t *testing.T, ch <-chan map[string]any, wantDecision string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(750 * time.Millisecond)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for decision %q", wantDecision)
		}
		select {
		case msg := <-ch:
			if got, _ := msg["decision"].(string); got == wantDecision {
				return msg
			}
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for decision %q", wantDecision)
		}
	}
}

func expectVerifierMessageSeq(t *testing.T, ch <-chan map[string]any, wantDecision string, wantSeq int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(750 * time.Millisecond)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for decision %q seq %d", wantDecision, wantSeq)
		}
		select {
		case msg := <-ch:
			if gotDecision, _ := msg["decision"].(string); gotDecision == wantDecision {
				seqRaw, ok := msg["seq_num"]
				if !ok {
					continue
				}
				seqNum := 0
				switch v := seqRaw.(type) {
				case float64:
					seqNum = int(v)
				case int:
					seqNum = v
				}
				if seqNum == wantSeq {
					return msg
				}
			}
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for decision %q seq %d", wantDecision, wantSeq)
		}
	}
}

// Two matching exec tokens reach quorum and trigger commit responses to execs
func TestVerifierCommitOnQuorum(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)

	first := v.HandleMessage(map[string]any{
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	if first["status"] != "waiting" {
		t.Fatalf("expected waiting after first token, got %v", first["status"])
	}

	second := v.HandleMessage(map[string]any{
		"seq_num":   1,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec2",
	})
	if second["status"] != "committed" {
		t.Fatalf("expected committed after quorum, got %v", second["status"])
	}
	if v.committed[1] != "tokA" {
		t.Fatalf("expected committed token tokA, got %v", v.committed[1])
	}

	msg := expectVerifierMessage(t, ts.received, "commit")
	if msg["token"] != "tokA" {
		t.Fatalf("expected commit token tokA, got %v", msg["token"])
	}
}

// Divergent tokens from all execs trigger rollback
func TestVerifierRollbackOnDivergence(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)

	v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	resp := v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tokB",
		"prev_hash": "",
		"exec_id":   "exec2",
	})
	if resp["status"] != "rollback" {
		t.Fatalf("expected rollback status, got %v", resp["status"])
	}

	msg := expectVerifierMessage(t, ts.received, "rollback")
	if msg["seq_num"] != float64(2) && msg["seq_num"] != 2 {
		t.Fatalf("expected rollback seq_num 2, got %v", msg["seq_num"])
	}
}

// Prev-hash mismatch for seq>1 is rejected without recording tokens
func TestVerifierRejectsInvalidPrevHash(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1"}, "ver1", execCh)
	v.committed[1] = "good-prev"

	resp := v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tokA",
		"prev_hash": "bad-prev",
		"exec_id":   "exec1",
	})
	if resp["status"] != "invalid_prev_hash" {
		t.Fatalf("expected invalid_prev_hash, got %v", resp["status"])
	}
	if v.tokens[2] != nil {
		t.Fatalf("expected no token recorded for invalid prev_hash")
	}
}

// Requests for an already committed seq return the committed token
func TestVerifierAlreadyCommitted(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1"}, "ver1", execCh)
	v.committed[3] = "tokC"

	resp := v.HandleMessage(map[string]any{
		"seq_num":   3,
		"token":     "tokX",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	if resp["status"] != "already_committed" {
		t.Fatalf("expected already_committed, got %v", resp["status"])
	}
	if resp["token"] != "tokC" {
		t.Fatalf("expected token tokC, got %v", resp["token"])
	}
}

// Duplicate exec IDs do not increase quorum counts
func TestVerifierIgnoresDuplicateExecID(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)

	first := v.HandleMessage(map[string]any{
		"seq_num":   4,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	if first["status"] != "waiting" {
		t.Fatalf("expected waiting, got %v", first["status"])
	}

	second := v.HandleMessage(map[string]any{
		"seq_num":   4,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	if second["status"] != "waiting" {
		t.Fatalf("expected waiting after duplicate exec_id, got %v", second["status"])
	}
	if len(v.tokens[4]["tokA"]) != 1 {
		t.Fatalf("expected single exec_id recorded, got %d", len(v.tokens[4]["tokA"]))
	}
}

// Without quorum and without responses from all execs, verifier waits
func TestVerifierWaitsWithoutQuorum(t *testing.T) {
	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"e1", "e2", "e3"}, "ver1", execCh)

	resp := v.HandleMessage(map[string]any{
		"seq_num":   5,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "e1",
	})
	if resp["status"] != "waiting" {
		t.Fatalf("expected waiting, got %v", resp["status"])
	}
}

// Valid prev_hash for seq>1 proceeds and can commit on quorum
func TestVerifierAcceptsPrevHashWhenMatching(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)
	v.committed[1] = "prev-ok"

	v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tokA",
		"prev_hash": "prev-ok",
		"exec_id":   "exec1",
	})
	resp := v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tokA",
		"prev_hash": "prev-ok",
		"exec_id":   "exec2",
	})
	if resp["status"] != "committed" {
		t.Fatalf("expected committed with valid prev_hash, got %v", resp["status"])
	}
	expectVerifierMessage(t, ts.received, "commit")
}

// With u=1,r=0, a single matching token (execQuorum=2) commits after two distinct execs
func TestVerifierCommitsWithLowestQuorum(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1", "127.0.0.1"}, "ver1", execCh)
	v.HandleMessage(map[string]any{
		"seq_num":   6,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	resp := v.HandleMessage(map[string]any{
		"seq_num":   6,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec2",
	})
	if resp["status"] != "committed" {
		t.Fatalf("expected committed after execQuorum, got %v", resp["status"])
	}
	expectVerifierMessage(t, ts.received, "commit")
}

// Delayed arrivals for the same seq still commit once quorum is reached
func TestVerifierConcurrentSameSeqCommit(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)

	v.HandleMessage(map[string]any{
		"seq_num":   8,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	v.HandleMessage(map[string]any{
		"seq_num":   8,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec2",
	})

	msg := expectVerifierMessage(t, ts.received, "commit")
	if msg["token"] != "tokA" {
		t.Fatalf("expected commit token tokA, got %v", msg["token"])
	}

	// Late divergent token should be ignored as already committed
	late := v.HandleMessage(map[string]any{
		"seq_num":   8,
		"token":     "tokB",
		"prev_hash": "",
		"exec_id":   "exec3",
	})
	if late["status"] != "already_committed" {
		t.Fatalf("expected already_committed for late token, got %v", late["status"])
	}
}

// Interleaved seq processing commits in order when prev_hash matches
func TestVerifierInterleavedSequencesRespectPrevHash(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)

	// Seq 1 commit
	v.HandleMessage(map[string]any{
		"seq_num":   1,
		"token":     "tok1",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	v.HandleMessage(map[string]any{
		"seq_num":   1,
		"token":     "tok1",
		"prev_hash": "",
		"exec_id":   "exec2",
	})
	expectVerifierMessage(t, ts.received, "commit")

	// Wait until committed map is updated before sending seq 2 with prev_hash
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		if v.committed[1] == "tok1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for seq 1 commit to be recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Seq 2 commit with matching prev_hash
	v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tok2",
		"prev_hash": "tok1",
		"exec_id":   "exec1",
	})
	resp := v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tok2",
		"prev_hash": "tok1",
		"exec_id":   "exec2",
	})
	if resp["status"] != "committed" {
		t.Fatalf("expected committed for seq 2, got %v", resp["status"])
	}
	expectVerifierMessage(t, ts.received, "commit")
}

// A delayed second token still commits and emits a verify response
func TestVerifierDelayedSecondTokenCommits(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)

	first := v.HandleMessage(map[string]any{
		"seq_num":   9,
		"token":     "tokA",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	if first["status"] != "waiting" {
		t.Fatalf("expected waiting after first token, got %v", first["status"])
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		v.HandleMessage(map[string]any{
			"seq_num":   9,
			"token":     "tokA",
			"prev_hash": "",
			"exec_id":   "exec2",
		})
	}()

	msg := expectVerifierMessage(t, ts.received, "commit")
	if msg["token"] != "tokA" {
		t.Fatalf("expected commit token tokA, got %v", msg["token"])
	}
}

// Out-of-order seq with prev_hash is buffered until seq-1 commits
func TestVerifierBuffersUntilPrevCommitted(t *testing.T) {
	ts := startVerifierTestServer(t)
	defer ts.close()

	execCh := make(chan map[string]any, 16)
	v := NewVerifier("ver1", []string{"127.0.0.1", "127.0.0.1"}, "ver1", execCh)

	// Buffer seq 2 tokens that depend on seq 1
	resp1 := v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tok2",
		"prev_hash": "tok1",
		"exec_id":   "exec1",
	})
	if resp1["status"] != "buffered" {
		t.Fatalf("expected buffered for seq 2 before seq 1 commit, got %v", resp1["status"])
	}
	resp2 := v.HandleMessage(map[string]any{
		"seq_num":   2,
		"token":     "tok2",
		"prev_hash": "tok1",
		"exec_id":   "exec2",
	})
	if resp2["status"] != "buffered" {
		t.Fatalf("expected buffered for seq 2 before seq 1 commit, got %v", resp2["status"])
	}
	if v.tokens[2] != nil {
		t.Fatalf("expected seq 2 tokens not recorded while buffered")
	}

	// Commit seq 1, which should flush and commit seq 2
	v.HandleMessage(map[string]any{
		"seq_num":   1,
		"token":     "tok1",
		"prev_hash": "",
		"exec_id":   "exec1",
	})
	v.HandleMessage(map[string]any{
		"seq_num":   1,
		"token":     "tok1",
		"prev_hash": "",
		"exec_id":   "exec2",
	})

	expectVerifierMessageSeq(t, ts.received, "commit", 1)
	expectVerifierMessageSeq(t, ts.received, "commit", 2)
	if v.committed[2] != "tok2" {
		t.Fatalf("expected seq 2 committed to tok2, got %v", v.committed[2])
	}
}
