package pbeo

import (
	"sync"
	"testing"
	"time"

	"aegean/eo"
	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type fakeConsensusBox struct {
	mu        sync.Mutex
	isLeader  bool
	leader    string
	slot      uint64
	onLearn   LearnFunc
	proposals []Entry
}

func TestShouldTickRaftWithDisabledFollowerElections(t *testing.T) {
	tests := []struct {
		name    string
		status  raft.Status
		disable bool
		want    bool
	}{
		{
			name:    "normal raft keeps ticking followers",
			status:  raft.Status{BasicStatus: raft.BasicStatus{SoftState: raft.SoftState{Lead: 1, RaftState: raft.StateFollower}}},
			disable: false,
			want:    true,
		},
		{
			name:    "leader keeps ticking",
			status:  raft.Status{BasicStatus: raft.BasicStatus{SoftState: raft.SoftState{Lead: 1, RaftState: raft.StateLeader}}},
			disable: true,
			want:    true,
		},
		{
			name:    "followers tick until a leader is known",
			status:  raft.Status{BasicStatus: raft.BasicStatus{SoftState: raft.SoftState{Lead: 0, RaftState: raft.StateFollower}}},
			disable: true,
			want:    true,
		},
		{
			name:    "followers stop ticking after a leader is known",
			status:  raft.Status{BasicStatus: raft.BasicStatus{SoftState: raft.SoftState{Lead: 1, RaftState: raft.StateFollower}}},
			disable: true,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldTickRaft(tt.status, tt.disable); got != tt.want {
				t.Fatalf("shouldTickRaft() = %v, want %v", got, tt.want)
			}
		})
	}
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

func (f *fakeConsensusBox) Propose(entry Entry) error {
	f.mu.Lock()
	f.proposals = append(f.proposals, cloneEntry(entry))
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

type fakeEOConsensusBox struct {
	mu        sync.Mutex
	isLeader  bool
	leader    string
	slot      uint64
	onLearn   eo.LearnFunc
	proposals []eo.Entry
}

func (f *fakeEOConsensusBox) IsLeader() bool {
	return f.isLeader
}

func (f *fakeEOConsensusBox) Leader() (string, bool) {
	if f.leader == "" {
		return "", false
	}
	return f.leader, true
}

func (f *fakeEOConsensusBox) Propose(entry eo.Entry) error {
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

func (f *fakeEOConsensusBox) HandleMessage(message raftpb.Message) error {
	return nil
}

func (f *fakeEOConsensusBox) Stop() {}

func newTestPBEO(t *testing.T, box *fakeConsensusBox, execute ExecuteRequestFunc, initState InitStateFunc, send SendFunc) *PBEO {
	t.Helper()
	nestedBox := &fakeEOConsensusBox{isLeader: true, leader: "node1"}
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
		BoxFactory: func(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error) {
			box.onLearn = onLearn
			return box, nil
		},
		NestedBoxFactory: func(cfg eo.BoxConfig, onLearn eo.LearnFunc) (eo.ConsensusBox, error) {
			nestedBox.onLearn = onLearn
			return nestedBox, nil
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

	component.Learn(1, Entry{
		RequestID: "r1",
		Response:  map[string]any{"request_id": "r1", "status": "ok"},
		Writes:    map[string]string{"x": "first"},
	})
	component.Learn(2, Entry{
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

func TestPBEOCommitsPrimaryExecutionWithoutValidation(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	var component *PBEO
	attempts := 0

	component = newTestPBEO(t, box, func(tx *Txn, request map[string]any) map[string]any {
		attempts++
		value := tx.ReadKV("x")
		component.Learn(1, Entry{
			RequestID: "other",
			Response:  map[string]any{"request_id": "other", "status": "ok"},
			Writes:    map[string]string{"x": "changed"},
		})
		box.mu.Lock()
		box.slot = 1
		box.mu.Unlock()

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
		if component.ReadKV("y") == "initial" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := component.ReadKV("x"); got != "changed" {
		t.Fatalf("expected concurrent committed write to remain applied, got %q", got)
	}
	if got := component.ReadKV("y"); got != "initial" {
		t.Fatalf("expected original sandbox write to commit without retry, got %q", got)
	}
	if attempts != 1 {
		t.Fatalf("expected no validation retry, got %d attempt(s)", attempts)
	}
	if len(box.proposals) != 1 {
		t.Fatalf("expected one accepted proposal, got %d", len(box.proposals))
	}
	if got := box.proposals[0].Writes["y"]; got != "initial" {
		t.Fatalf("expected proposal to preserve original sandbox value, got %q", got)
	}
	if got := box.proposals[0].Response["value"]; got != "initial" {
		t.Fatalf("expected response from original execution, got %v", got)
	}
}

func TestPBEOReexecutesWhenNestedResponseArrivesBeforeBlockedRegistration(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	var component *PBEO
	attempts := make(chan int, 2)
	sent := make(chan map[string]any, 1)

	attempt := 0
	component = newTestPBEO(t, box, func(tx *Txn, request map[string]any) map[string]any {
		attempt++
		attempts <- attempt
		if attempt == 1 {
			if !component.BufferNestedResponse(map[string]any{
				"type":              "response",
				"request_id":        "r1/child",
				"parent_request_id": "r1",
				"sender":            "child",
			}) {
				t.Fatalf("expected early nested response to be buffered")
			}
			return map[string]any{"request_id": request["request_id"], "status": "blocked_for_nested_response"}
		}
		if _, ok := tx.GetNestedResponses("r1"); !ok {
			t.Fatalf("expected buffered nested response on retry")
		}
		return map[string]any{"request_id": request["request_id"], "status": "ok"}
	}, nil, func(peer string, payload map[string]any) error {
		sent <- cloneMapAny(payload)
		return nil
	})
	defer component.Stop()

	response := component.HandleRequest("r1", map[string]any{"request_id": "r1"})
	if response["status"] != "accepted" {
		t.Fatalf("expected request to be accepted, got %v", response["status"])
	}

	for want := 1; want <= 2; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for attempt %d", want)
		}
	}

	select {
	case payload := <-sent:
		if payload["request_id"] != "r1" {
			t.Fatalf("expected response for r1, got %v", payload["request_id"])
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for committed response")
	}
}
