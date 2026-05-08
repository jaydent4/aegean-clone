package eo

import (
	"fmt"
	"sync"
	"testing"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

type fakeConsensusBox struct {
	isLeader  bool
	leader    string
	proposals []Entry
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
	f.proposals = append(f.proposals, entry)
	return nil
}

func (f *fakeConsensusBox) HandleMessage(message raftpb.Message) error {
	return nil
}

func (f *fakeConsensusBox) Stop() {}

func newTestEO(t *testing.T, box ConsensusBox, execute ExecuteFunc, commit CommitFunc, forward ForwardFunc) *EO {
	t.Helper()

	component, err := NewEO(Config{
		Name:    "node1",
		Peers:   []string{"node1", "node2", "node3"},
		Execute: execute,
		Commit:  commit,
		Forward: forward,
		BoxFactory: func(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error) {
			return box, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEO error: %v", err)
	}
	return component
}

func TestEOHandleRequestLeaderDeduplicates(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	executions := 0

	component := newTestEO(t, box, func(requestID string, request map[string]any) (map[string]any, error) {
		executions++
		return map[string]any{"status": "ok", "request_id": requestID}, nil
	}, nil, nil)
	defer component.Stop()

	first := component.HandleRequestMessage(map[string]any{"request_id": "r1", "op": "write"})
	second := component.HandleRequestMessage(map[string]any{"request_id": "r1", "op": "write"})

	if first["status"] != "proposed" {
		t.Fatalf("expected first request to be proposed, got %v", first["status"])
	}
	if second["status"] != "duplicate_request" {
		t.Fatalf("expected duplicate request to be ignored, got %v", second["status"])
	}
	if executions != 1 {
		t.Fatalf("expected execute callback once, got %d", executions)
	}
	if len(box.proposals) != 1 {
		t.Fatalf("expected one proposal, got %d", len(box.proposals))
	}
	if box.proposals[0].RequestID != "r1" {
		t.Fatalf("expected request_id r1, got %q", box.proposals[0].RequestID)
	}
}

func TestEOHandleRequestFollowerForwardsToLeader(t *testing.T) {
	box := &fakeConsensusBox{isLeader: false, leader: "node2"}
	var forwardedLeader string
	var forwardedRequestID string
	var forwardedRequest map[string]any

	component := newTestEO(t, box, func(requestID string, request map[string]any) (map[string]any, error) {
		t.Fatalf("execute should not be called on follower")
		return nil, nil
	}, nil, func(leader string, requestID string, request map[string]any) error {
		forwardedLeader = leader
		forwardedRequestID = requestID
		forwardedRequest = request
		return nil
	})
	defer component.Stop()

	response := component.HandleRequestMessage(map[string]any{"request_id": "r2", "op": "write"})

	if response["status"] != "forwarded_to_leader" {
		t.Fatalf("expected forwarded_to_leader, got %v", response["status"])
	}
	if forwardedLeader != "node2" {
		t.Fatalf("expected leader node2, got %q", forwardedLeader)
	}
	if forwardedRequestID != "r2" {
		t.Fatalf("expected request_id r2, got %q", forwardedRequestID)
	}
	if forwardedRequest["request_id"] != "r2" {
		t.Fatalf("expected forwarded request_id r2, got %v", forwardedRequest["request_id"])
	}
}

func TestEOProcessWaitsForContiguousLearn(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	committed := make([]CommittedEntry, 0, 2)

	component := newTestEO(t, box, func(requestID string, request map[string]any) (map[string]any, error) {
		return map[string]any{"status": "ok"}, nil
	}, func(entry CommittedEntry) {
		committed = append(committed, entry)
	}, nil)
	defer component.Stop()

	component.Learn(2, Entry{RequestID: "r2", Response: map[string]any{"status": "two"}})
	if len(committed) != 0 {
		t.Fatalf("expected no committed entries yet, got %d", len(committed))
	}

	component.Learn(1, Entry{RequestID: "r1", Response: map[string]any{"status": "one"}})
	if len(committed) != 2 {
		t.Fatalf("expected two committed entries, got %d", len(committed))
	}
	if committed[0].Slot != 1 || committed[0].Entry.RequestID != "r1" {
		t.Fatalf("expected slot 1 / r1 first, got slot %d request %q", committed[0].Slot, committed[0].Entry.RequestID)
	}
	if committed[1].Slot != 2 || committed[1].Entry.RequestID != "r2" {
		t.Fatalf("expected slot 2 / r2 second, got slot %d request %q", committed[1].Slot, committed[1].Entry.RequestID)
	}
	if component.LearnedIndex() != 2 {
		t.Fatalf("expected learned index 2, got %d", component.LearnedIndex())
	}
	if component.ProcessedIndex() != 2 {
		t.Fatalf("expected processed index 2, got %d", component.ProcessedIndex())
	}
}

func TestEOProcessDeduplicatesRequestIDs(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	committed := make([]CommittedEntry, 0, 2)

	component := newTestEO(t, box, nil, func(entry CommittedEntry) {
		committed = append(committed, entry)
	}, nil)
	defer component.Stop()

	component.Learn(1, Entry{RequestID: "r1", Response: map[string]any{"status": "first"}})
	component.Learn(2, Entry{RequestID: "r1", Response: map[string]any{"status": "duplicate"}})
	component.Learn(3, Entry{RequestID: "r2", Response: map[string]any{"status": "second"}})

	if len(committed) != 2 {
		t.Fatalf("expected two deduplicated commits, got %d", len(committed))
	}
	if committed[0].Slot != 1 || committed[0].Entry.Response["status"] != "first" {
		t.Fatalf("expected first r1 occurrence from slot 1, got %+v", committed[0])
	}
	if committed[1].Slot != 3 || committed[1].Entry.RequestID != "r2" {
		t.Fatalf("expected r2 from slot 3, got %+v", committed[1])
	}
	if component.ProcessedIndex() != 3 {
		t.Fatalf("expected duplicate slot to be processed, got processed index %d", component.ProcessedIndex())
	}
}

func TestEOProcessSerializesCommitDelivery(t *testing.T) {
	box := &fakeConsensusBox{isLeader: true, leader: "node1"}
	startedSlot1 := make(chan struct{})
	releaseSlot1 := make(chan struct{})
	delivered := make(chan uint64, 2)

	component := newTestEO(t, box, nil, func(entry CommittedEntry) {
		if entry.Slot == 1 {
			close(startedSlot1)
			<-releaseSlot1
		}
		delivered <- entry.Slot
	}, nil)
	defer component.Stop()

	go component.Learn(1, Entry{RequestID: "r1", Response: map[string]any{"status": "one"}})

	select {
	case <-startedSlot1:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for slot 1 commit to start")
	}

	go component.Learn(2, Entry{RequestID: "r2", Response: map[string]any{"status": "two"}})

	select {
	case slot := <-delivered:
		t.Fatalf("slot %d was delivered before slot 1 commit completed", slot)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSlot1)

	requireDeliveredSlot := func(want uint64) {
		t.Helper()
		select {
		case slot := <-delivered:
			if slot != want {
				t.Fatalf("expected slot %d to deliver next, got %d", want, slot)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for slot %d delivery", want)
		}
	}
	requireDeliveredSlot(1)
	requireDeliveredSlot(2)
}

func TestEORaftClusterCommitsForwardedRequest(t *testing.T) {
	peers := []string{"node1", "node2", "node3"}
	committedByNode := make(map[string]chan CommittedEntry, len(peers))
	nodes := make(map[string]*EO, len(peers))
	var nodesMu sync.RWMutex

	for _, name := range peers {
		committedByNode[name] = make(chan CommittedEntry, 4)
	}

	for _, name := range peers {
		nodeName := name
		component, err := NewEO(Config{
			Name:  nodeName,
			Peers: peers,
			Execute: func(requestID string, request map[string]any) (map[string]any, error) {
				return map[string]any{
					"request_id": requestID,
					"status":     "ok",
					"value":      request["value"],
				}, nil
			},
			Commit: func(entry CommittedEntry) {
				committedByNode[nodeName] <- entry
			},
			Forward: func(leader string, requestID string, request map[string]any) error {
				nodesMu.RLock()
				target := nodes[leader]
				nodesMu.RUnlock()
				if target == nil {
					return fmt.Errorf("leader %s unavailable", leader)
				}
				response := target.HandleRequest(requestID, request)
				if response["status"] == "proposal_error" || response["status"] == "execution_error" {
					return fmt.Errorf("%v", response["error"])
				}
				return nil
			},
			SendRaft: func(peer string, message raftpb.Message) error {
				nodesMu.RLock()
				target := nodes[peer]
				nodesMu.RUnlock()
				if target == nil {
					return fmt.Errorf("peer %s unavailable", peer)
				}
				payload, err := EncodeRaftMessage(message)
				if err != nil {
					return err
				}
				response := target.HandleRaftMessage(payload)
				if response["status"] != "raft_message_accepted" {
					return fmt.Errorf("peer %s rejected raft message: %v", peer, response["status"])
				}
				return nil
			},
			TickInterval:  5 * time.Millisecond,
			ElectionTick:  6,
			HeartbeatTick: 1,
		})
		if err != nil {
			t.Fatalf("NewEO(%s) error: %v", nodeName, err)
		}

		nodesMu.Lock()
		nodes[nodeName] = component
		nodesMu.Unlock()
	}
	defer func() {
		for _, component := range nodes {
			component.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 3*time.Second)
	var follower *EO
	for name, component := range nodes {
		if name != leader {
			follower = component
			break
		}
	}
	if follower == nil {
		t.Fatalf("expected at least one follower")
	}

	response := follower.HandleRequest("r-cluster", map[string]any{
		"request_id": "r-cluster",
		"value":      "payload",
	})
	if response["status"] != "forwarded_to_leader" {
		t.Fatalf("expected forwarded_to_leader, got %v", response["status"])
	}

	for _, name := range peers {
		select {
		case committed := <-committedByNode[name]:
			if committed.Slot != 1 {
				t.Fatalf("%s expected slot 1, got %d", name, committed.Slot)
			}
			if committed.Entry.RequestID != "r-cluster" {
				t.Fatalf("%s expected request_id r-cluster, got %q", name, committed.Entry.RequestID)
			}
			if committed.Entry.Response["value"] != "payload" {
				t.Fatalf("%s expected payload response, got %v", name, committed.Entry.Response["value"])
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for committed entry on %s", name)
		}
	}
}

func waitForLeader(t *testing.T, nodes map[string]*EO, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for name, component := range nodes {
			if component.IsLeader() {
				return name
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader")
	return ""
}
