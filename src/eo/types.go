package eo

import (
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	// MessageTypeRaft identifies internal EO raft traffic on the wire.
	MessageTypeRaft = "eo_raft"
	raftMessageKey  = "raft_message"
)

// Entry is the ordered value replicated through the consensus box.
type Entry struct {
	RequestID string         `json:"request_id"`
	Response  map[string]any `json:"response"`
}

// CommittedEntry is delivered to the application once a learned slot becomes
// contiguous, deduplicated by request id, and safe to commit to the executor.
type CommittedEntry struct {
	Slot  uint64 `json:"slot"`
	Entry Entry  `json:"entry"`
}

// AppliedEntry is kept as a compatibility alias for older callers.
type AppliedEntry = CommittedEntry

// ExecuteFunc runs the request on the leader before the result is proposed.
type ExecuteFunc func(requestID string, request map[string]any) (map[string]any, error)

// CommitFunc receives deduplicated entries once EO processes them in slot order.
type CommitFunc func(entry CommittedEntry)

// ApplyFunc is kept as a compatibility alias for older callers.
type ApplyFunc = CommitFunc

// ForwardFunc forwards a client request to the current leader.
type ForwardFunc func(leader string, requestID string, request map[string]any) error

// SendRaftFunc ships a raft protocol message to a remote peer.
type SendRaftFunc func(peer string, message raftpb.Message) error

// LearnFunc is invoked by the consensus box whenever a replicated entry is learned.
type LearnFunc func(slot uint64, entry Entry)

// ConsensusBox is the ordered replication abstraction used by EO.
type ConsensusBox interface {
	IsLeader() bool
	Leader() (string, bool)
	Propose(entry Entry) error
	HandleMessage(message raftpb.Message) error
	Stop()
}

// BoxConfig controls the raft-backed consensus box.
type BoxConfig struct {
	Name            string
	Peers           []string
	SendRaft        SendRaftFunc
	TickInterval    time.Duration
	ElectionTick    int
	HeartbeatTick   int
	MaxInflightMsgs int
	MaxSizePerMsg   uint64
}

// BoxFactory allows tests to swap in a fake consensus implementation.
type BoxFactory func(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error)

// Config constructs an EO instance.
type Config struct {
	Name            string
	Peers           []string
	Execute         ExecuteFunc
	Commit          CommitFunc
	Apply           ApplyFunc
	Forward         ForwardFunc
	SendRaft        SendRaftFunc
	BoxFactory      BoxFactory
	TickInterval    time.Duration
	ElectionTick    int
	HeartbeatTick   int
	MaxInflightMsgs int
	MaxSizePerMsg   uint64
}
