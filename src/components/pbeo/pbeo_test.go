package pbeo

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"aegean/components/eo"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type fakeConsensusBox struct {
	mu        sync.Mutex
	isLeader  bool
	leader    string
	slot      uint64
	onLearn   eo.LearnFunc
	proposals []eo.Entry
}

func (f *fakeConsensusBox) IsLeader() bool {
	return f.isLeader
}

func (f *fakeConsensusBox) Leader() (string, bool) {
	if f.leader == "" {
		return "", false
	}
	return f.leader, true
}

func (f *fakeConsensusBox) Propose(entry eo.Entry) error {
	f.mu.Lock()
	f.proposals = append(f.proposals, eo.Entry{
		RequestID: entry.RequestID,
		Response:  cloneMapAny(entry.Response),
	})
	f.slot++
	slot := f.slot
	onLearn := f.onLearn
	f.mu.Unlock()

	if onLearn != nil {
		onLearn(slot, entry)
	}
	return nil
}

func (f *fakeConsensusBox) HandleMessage(message raftpb.Message) error {
	return nil
}

func (f *fakeConsensusBox) Stop() {}

func newTestPBEO(t *testing.T, box *fakeConsensusBox, execute ExecuteRequestFunc, initState InitStateFunc, send SendFunc) *PBEO {
	t.Helper()
	if send == nil {
		send = func(peer string, payload map[string]any) error {
			return nil
		}
	}
	component, err := NewPBEO(Config{
		Name:      "node1",
		Peers:     []string{"node1", "node2", "node3"},
		Clients:   []string{"client"},
		Execute:   execute,
		InitState: initState,
		RunConfig: map[string]any{},
		Send:      send,
		BoxFactory: func(cfg eo.BoxConfig, onLearn eo.LearnFunc) (eo.ConsensusBox, error) {
			box.onLearn = onLearn
			return box, nil
		},
	})
	if err != nil {
		t.Fatalf("NewPBEO error: %v", err)
	}
	return component
}

func TestPBEOAppliesCommittedEntryOnce(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	component := newTestPBEO(t, box, func(tx *Txn, request map[string]any) map[string]any {
		t.Fatalf("execute should not be called")
		return nil
	}, func(runConfig map[string]any) map[string]string {
		return map[string]string{"x": "initial"}
	}, nil)
	defer component.Stop()

	learnPBEOEntry(component, 1, Entry{
		RequestID: "r1",
		Response:  map[string]any{"request_id": "r1", "status": "ok"},
		Writes:    map[string]string{"x": "first"},
	})
	learnPBEOEntry(component, 2, Entry{
		RequestID: "r1",
		Response:  map[string]any{"request_id": "r1", "status": "duplicate"},
		Writes:    map[string]string{"x": "duplicate"},
	})

	if got := component.ReadKV("x"); got != "first" {
		t.Fatalf("expected first committed write to win, got %q", got)
	}
	if got := component.ProcessedIndex(); got != 2 {
		t.Fatalf("expected duplicate slot to be processed, got %d", got)
	}
}

func TestPBEOSandboxWritesApplyOnlyAfterLogCommit(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	wrote := make(chan struct{})
	release := make(chan struct{})
	sent := make(chan map[string]any, 1)

	component := newTestPBEO(t, box, func(tx *Txn, request map[string]any) map[string]any {
		tx.WriteKV("x", "tentative")
		close(wrote)
		<-release
		return map[string]any{"request_id": request["request_id"], "status": "ok"}
	}, func(runConfig map[string]any) map[string]string {
		return map[string]string{"x": "initial"}
	}, func(peer string, payload map[string]any) error {
		sent <- cloneMapAny(payload)
		return nil
	})
	defer component.Stop()

	response := component.HandleRequest("r1", map[string]any{"request_id": "r1"})
	if response["status"] != "accepted" {
		t.Fatalf("expected request to be accepted, got %v", response["status"])
	}

	select {
	case <-wrote:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for sandbox write")
	}
	if got := component.ReadKV("x"); got != "initial" {
		t.Fatalf("sandbox write leaked before log commit: %q", got)
	}

	close(release)
	select {
	case payload := <-sent:
		if payload["request_id"] != "r1" {
			t.Fatalf("expected response for r1, got %v", payload["request_id"])
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for committed response")
	}
	if got := component.ReadKV("x"); got != "tentative" {
		t.Fatalf("expected committed write, got %q", got)
	}
}

func TestPBEORetriesStaleSpeculativeRead(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	var component *PBEO
	attempts := 0

	component = newTestPBEO(t, box, func(tx *Txn, request map[string]any) map[string]any {
		attempts++
		value := tx.ReadKV("x")
		if attempts == 1 {
			learnPBEOEntry(component, 1, Entry{
				RequestID: "other",
				Response:  map[string]any{"request_id": "other", "status": "ok"},
				Writes:    map[string]string{"x": "changed"},
			})
			box.mu.Lock()
			box.slot = 1
			box.mu.Unlock()
		}
		tx.WriteKV("y", value)
		return map[string]any{
			"request_id": request["request_id"],
			"status":     "ok",
			"value":      value,
		}
	}, func(runConfig map[string]any) map[string]string {
		return map[string]string{"x": "initial"}
	}, nil)
	defer component.Stop()

	response := component.HandleRequest("r1", map[string]any{"request_id": "r1"})
	if response["status"] != "accepted" {
		t.Fatalf("expected request to be accepted, got %v", response["status"])
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if component.ReadKV("y") == "changed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := component.ReadKV("y"); got != "changed" {
		t.Fatalf("expected retry to write value from latest snapshot, got %q", got)
	}
	if attempts < 2 {
		t.Fatalf("expected stale read retry, got %d attempt(s)", attempts)
	}
	if len(box.proposals) != 1 {
		t.Fatalf("expected one accepted proposal after retry, got %d", len(box.proposals))
	}
	proposed := decodeEntryPayload(box.proposals[0].RequestID, box.proposals[0].Response)
	if proposed.ReadVersions["x"] != 1 {
		t.Fatalf("expected retried proposal to read version 1, got %d", proposed.ReadVersions["x"])
	}
	if got := fmt.Sprint(proposed.Response["value"]); got != "changed" {
		t.Fatalf("expected response from retried execution, got %q", got)
	}
}

func learnPBEOEntry(component *PBEO, slot uint64, entry Entry) {
	component.log.Learn(slot, eo.Entry{
		RequestID: entry.RequestID,
		Response:  encodeEntryPayload(entry),
	})
}
