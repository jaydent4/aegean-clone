package pbeo

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

var errConsensusBoxStopped = errors.New("pbeo consensus box stopped")

type proposalRequest struct {
	entry  Entry
	result chan error
}

type raftConsensusBox struct {
	name    string
	selfID  uint64
	peerIDs map[string]uint64
	peers   map[uint64]string
	send    SendRaftFunc
	learn   LearnFunc

	leaderID    atomic.Uint64
	learnedSlot uint64

	proposals chan proposalRequest
	steps     chan raftpb.Message
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func newRaftConsensusBox(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error) {
	peerIDs, peers, selfID, err := buildPeerIDs(cfg.Name, cfg.Peers)
	if err != nil {
		return nil, err
	}
	if cfg.SendRaft == nil {
		return nil, fmt.Errorf("pbeo raft consensus box requires a SendRaft callback")
	}

	tickInterval := cfg.TickInterval
	if tickInterval <= 0 {
		tickInterval = 10 * time.Millisecond
	}
	electionTick := cfg.ElectionTick
	if electionTick <= 0 {
		electionTick = 10
	}
	heartbeatTick := cfg.HeartbeatTick
	if heartbeatTick <= 0 {
		heartbeatTick = 1
	}
	if heartbeatTick >= electionTick {
		return nil, fmt.Errorf("heartbeat tick must be smaller than election tick")
	}
	maxInflight := cfg.MaxInflightMsgs
	if maxInflight <= 0 {
		maxInflight = 256
	}
	maxSizePerMsg := cfg.MaxSizePerMsg
	if maxSizePerMsg == 0 {
		maxSizePerMsg = 1 << 20
	}

	storage := raft.NewMemoryStorage()
	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              selfID,
		ElectionTick:    electionTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflight,
	})
	if err != nil {
		return nil, err
	}

	bootstrapPeers := make([]raft.Peer, 0, len(peerIDs))
	peerNames := make([]string, 0, len(peerIDs))
	for peer := range peerIDs {
		peerNames = append(peerNames, peer)
	}
	sort.Strings(peerNames)
	for _, peer := range peerNames {
		bootstrapPeers = append(bootstrapPeers, raft.Peer{ID: peerIDs[peer]})
	}
	if err := rawNode.Bootstrap(bootstrapPeers); err != nil {
		return nil, err
	}

	box := &raftConsensusBox{
		name:      cfg.Name,
		selfID:    selfID,
		peerIDs:   peerIDs,
		peers:     peers,
		send:      cfg.SendRaft,
		learn:     onLearn,
		proposals: make(chan proposalRequest),
		steps:     make(chan raftpb.Message, 1024),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	go box.run(rawNode, storage, tickInterval)
	return box, nil
}

func (b *raftConsensusBox) IsLeader() bool {
	return b.leaderID.Load() == b.selfID && b.selfID != 0
}

func (b *raftConsensusBox) Leader() (string, bool) {
	leaderID := b.leaderID.Load()
	if leaderID == 0 {
		return "", false
	}
	peer, ok := b.peers[leaderID]
	return peer, ok
}

func (b *raftConsensusBox) Propose(entry Entry) error {
	result := make(chan error, 1)
	request := proposalRequest{entry: entry, result: result}

	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	case b.proposals <- request:
	}

	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	case err := <-result:
		return err
	}
}

func (b *raftConsensusBox) HandleMessage(message raftpb.Message) error {
	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	case b.steps <- message:
		return nil
	}
}

func (b *raftConsensusBox) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		<-b.doneCh
	})
}

func (b *raftConsensusBox) run(rawNode *raft.RawNode, storage *raft.MemoryStorage, tickInterval time.Duration) {
	defer close(b.doneCh)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	b.drainReady(rawNode, storage)

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			rawNode.Tick()
			b.drainReady(rawNode, storage)
		case request := <-b.proposals:
			data, err := json.Marshal(request.entry)
			if err == nil {
				err = rawNode.Propose(data)
				b.drainReady(rawNode, storage)
			}
			request.result <- err
		case message := <-b.steps:
			if err := rawNode.Step(message); err == nil {
				b.drainReady(rawNode, storage)
			}
		}
	}
}

func (b *raftConsensusBox) drainReady(rawNode *raft.RawNode, storage *raft.MemoryStorage) {
	for rawNode.HasReady() {
		ready := rawNode.Ready()

		if ready.SoftState != nil {
			b.leaderID.Store(ready.SoftState.Lead)
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			_ = storage.ApplySnapshot(ready.Snapshot)
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			_ = storage.SetHardState(ready.HardState)
		}
		if len(ready.Entries) > 0 {
			_ = storage.Append(ready.Entries)
		}

		for _, message := range ready.Messages {
			if message.To == b.selfID {
				_ = rawNode.Step(message)
				continue
			}
			peer, ok := b.peers[message.To]
			if !ok {
				continue
			}
			if err := b.send(peer, message); err != nil {
				rawNode.ReportUnreachable(message.To)
			}
		}

		for _, entry := range ready.CommittedEntries {
			b.learnCommittedEntry(rawNode, entry)
		}

		rawNode.Advance(ready)
	}
}

func (b *raftConsensusBox) learnCommittedEntry(rawNode *raft.RawNode, entry raftpb.Entry) {
	switch entry.Type {
	case raftpb.EntryConfChange:
		if len(entry.Data) == 0 {
			return
		}
		var change raftpb.ConfChange
		if err := change.Unmarshal(entry.Data); err == nil {
			rawNode.ApplyConfChange(change)
		}
	case raftpb.EntryConfChangeV2:
		if len(entry.Data) == 0 {
			return
		}
		var change raftpb.ConfChangeV2
		if err := change.Unmarshal(entry.Data); err == nil {
			rawNode.ApplyConfChange(change)
		}
	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			return
		}
		var value Entry
		if err := json.Unmarshal(entry.Data, &value); err != nil {
			return
		}
		b.learnedSlot++
		if b.learn != nil {
			b.learn(b.learnedSlot, value)
		}
	}
}

func buildPeerIDs(self string, peers []string) (map[string]uint64, map[uint64]string, uint64, error) {
	if self == "" {
		return nil, nil, 0, fmt.Errorf("pbeo raft consensus box requires a node name")
	}
	if len(peers) == 0 {
		return nil, nil, 0, fmt.Errorf("pbeo raft consensus box requires peers")
	}

	uniquePeers := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" {
			continue
		}
		uniquePeers[peer] = struct{}{}
	}
	if _, ok := uniquePeers[self]; !ok {
		uniquePeers[self] = struct{}{}
	}

	peerNames := make([]string, 0, len(uniquePeers))
	for peer := range uniquePeers {
		peerNames = append(peerNames, peer)
	}
	sort.Strings(peerNames)

	peerIDs := make(map[string]uint64, len(peerNames))
	reverse := make(map[uint64]string, len(peerNames))
	for index, peer := range peerNames {
		id := uint64(index + 1)
		peerIDs[peer] = id
		reverse[id] = peer
	}
	return peerIDs, reverse, peerIDs[self], nil
}
