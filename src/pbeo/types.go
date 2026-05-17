package pbeo

import (
	"time"

	"aegean/eo"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	// MessageTypeRaft identifies PBEO's passive state-update Raft traffic.
	MessageTypeRaft = "pbeo_raft"
	raftMessageKey  = "raft_message"
	raftMessagesKey = "raft_messages"
)

// Entry is the passive-replication update record committed through PBEO's Raft log.
// The leader computes Response and Writes in a sandbox; replicas apply Writes only
// after this entry appears in the committed log.
type Entry struct {
	RequestID string            `json:"request_id"`
	Response  map[string]any    `json:"response"`
	Writes    map[string]string `json:"writes,omitempty"`
}

type CommittedEntry struct {
	Slot  uint64 `json:"slot"`
	Entry Entry  `json:"entry"`
}

// ExecuteRequestFunc runs one application request in a sandboxed transaction.
type ExecuteRequestFunc func(tx *Txn, request map[string]any) map[string]any

// InitStateFunc returns the initial key/value state for a PBEO service.
type InitStateFunc func(runConfig map[string]any) map[string]string

// SendFunc sends an application message to another node.
type SendFunc func(peer string, payload map[string]any) error

// SendRaftFunc sends a raft protocol message to another PBEO replica.
type SendRaftFunc func(peer string, message raftpb.Message) error
type SendRaftBatchFunc func(peer string, messages []raftpb.Message) error

type LearnFunc func(slot uint64, entry Entry)

type ConsensusBox interface {
	IsLeader() bool
	Leader() (string, bool)
	Propose(entry Entry) error
	HandleMessage(message raftpb.Message) error
	Stop()
}

type BoxConfig struct {
	Name          string
	Peers         []string
	SendRaft      SendRaftFunc
	SendRaftBatch SendRaftBatchFunc
	TickInterval  time.Duration
	ElectionTick  int
	HeartbeatTick int
	// DisableFollowerElections keeps followers from campaigning after a leader
	// is known. It is intended only for controlled experiments that assume node
	// failures do not happen.
	DisableFollowerElections bool
	MaxInflightMsgs          int
	MaxSizePerMsg            uint64
	RaftSendBatchSize        int
}

type BoxFactory func(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error)

type Config struct {
	Name                       string
	Peers                      []string
	Clients                    []string
	Execute                    ExecuteRequestFunc
	InitState                  InitStateFunc
	RunConfig                  map[string]any
	Send                       SendFunc
	SendRaft                   SendRaftFunc
	SendRaftBatch              SendRaftBatchFunc
	SendNestedRaft             eo.SendRaftFunc
	SendNestedRaftBatch        eo.SendRaftBatchFunc
	BoxFactory                 BoxFactory
	NestedBoxFactory           eo.BoxFactory
	TickInterval               time.Duration
	ElectionTick               int
	HeartbeatTick              int
	DisableFollowerElections   bool
	EODisableFollowerElections bool
	MaxInflightMsgs            int
	MaxSizePerMsg              uint64
	RaftSendBatchSize          int
	EORaftSendBatchSize        int
}
