package inlog

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"aegean/aegean_opt/protocol"
	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const raftMessageKey = "raft_message"

var errStopped = errors.New("inlog stopped")

type EntryType string

const (
	EntryTypeRequest      EntryType = "REQUEST"
	EntryTypeRequestBatch EntryType = "REQUEST_BATCH"
	EntryTypeBatchFormed  EntryType = "BATCH_FORMED"
)

type Entry struct {
	Type         EntryType        `json:"type"`
	SeqNum       int              `json:"seq_num"`
	RequestIdx   int              `json:"request_index,omitempty"`
	RequestID    string           `json:"request_id,omitempty"`
	Request      map[string]any   `json:"request,omitempty"`
	RequestIDs   []string         `json:"request_ids,omitempty"`
	Requests     []map[string]any `json:"requests,omitempty"`
	NDSeeds      []int64          `json:"nd_seeds,omitempty"`
	NDTimestamps []float64        `json:"nd_timestamps,omitempty"`
	NDSeed       int64            `json:"nd_seed,omitempty"`
	NDTimestamp  float64          `json:"nd_timestamp,omitempty"`
	Count        int              `json:"count,omitempty"`
}

type Config struct {
	Name              string
	Peers             []string
	SendRaft          func(peer string, payload map[string]any) error
	Commit            func(slot uint64, entry Entry)
	TickInterval      time.Duration
	ElectionTick      int
	HeartbeatTick     int
	CampaignOnStart   bool
	CampaignRetryTick int
}

type InLog struct {
	name     string
	selfID   uint64
	peerIDs  map[string]uint64
	peers    map[uint64]string
	sendRaft func(peer string, payload map[string]any) error
	commit   func(slot uint64, entry Entry)

	leaderID          atomic.Uint64
	slot              uint64
	campaignOnStart   bool
	campaignRetryTick int

	proposals   chan proposalRequest
	steps       chan raftpb.Message
	sendQueues  map[uint64]chan raftpb.Message
	unreachable chan uint64
	stopOnce    sync.Once
	stopCh      chan struct{}
	doneCh      chan struct{}

	mu                sync.Mutex
	committedRequests map[string]struct{}
}

type proposalRequest struct {
	entry  Entry
	result chan error
}

func New(cfg Config) (*InLog, error) {
	peerIDs, peers, selfID, err := buildPeerIDs(cfg.Name, cfg.Peers)
	if err != nil {
		return nil, err
	}
	if cfg.SendRaft == nil {
		return nil, fmt.Errorf("inlog requires SendRaft")
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
	campaignRetryTick := cfg.CampaignRetryTick
	if campaignRetryTick <= 0 {
		campaignRetryTick = 100
	}

	storage := raft.NewMemoryStorage()
	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              selfID,
		ElectionTick:    electionTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
	})
	if err != nil {
		return nil, err
	}

	peerNames := make([]string, 0, len(peerIDs))
	for peer := range peerIDs {
		peerNames = append(peerNames, peer)
	}
	sort.Strings(peerNames)
	raftPeers := make([]raft.Peer, 0, len(peerNames))
	for _, peer := range peerNames {
		raftPeers = append(raftPeers, raft.Peer{ID: peerIDs[peer]})
	}
	if err := rawNode.Bootstrap(raftPeers); err != nil {
		return nil, err
	}

	log := &InLog{
		name:              cfg.Name,
		selfID:            selfID,
		peerIDs:           peerIDs,
		peers:             peers,
		sendRaft:          cfg.SendRaft,
		commit:            cfg.Commit,
		campaignOnStart:   cfg.CampaignOnStart,
		campaignRetryTick: campaignRetryTick,
		proposals:         make(chan proposalRequest),
		steps:             make(chan raftpb.Message, 1024),
		sendQueues:        make(map[uint64]chan raftpb.Message),
		unreachable:       make(chan uint64, 1024),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
		committedRequests: make(map[string]struct{}),
	}
	for id := range peers {
		if id == selfID {
			continue
		}
		queue := make(chan raftpb.Message, 1024)
		log.sendQueues[id] = queue
		go log.runSendWorker(id, queue)
	}
	go log.run(rawNode, storage, tickInterval)
	return log, nil
}

func (l *InLog) IsLeader() bool {
	return l.leaderID.Load() == l.selfID && l.selfID != 0
}

func (l *InLog) Leader() (string, bool) {
	leaderID := l.leaderID.Load()
	if leaderID == 0 {
		return "", false
	}
	leader, ok := l.peers[leaderID]
	return leader, ok
}

func (l *InLog) Propose(entry Entry) error {
	result := make(chan error, 1)
	select {
	case <-l.doneCh:
		return errStopped
	case l.proposals <- proposalRequest{entry: entry, result: result}:
	}
	select {
	case <-l.doneCh:
		return errStopped
	case err := <-result:
		return err
	}
}

func (l *InLog) HandleRaftMessage(payload map[string]any) map[string]any {
	message, err := DecodeRaftMessage(payload)
	if err != nil {
		return map[string]any{"status": "invalid_inlog_raft", "error": err.Error()}
	}
	select {
	case <-l.doneCh:
		return map[string]any{"status": "inlog_stopped"}
	case l.steps <- message:
		return map[string]any{"status": "inlog_raft_accepted"}
	}
}

func (l *InLog) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopCh)
		<-l.doneCh
	})
}

func (l *InLog) run(rawNode *raft.RawNode, storage *raft.MemoryStorage, tickInterval time.Duration) {
	defer close(l.doneCh)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	l.drainReady(rawNode, storage)
	ticksSinceCampaign := l.campaignRetryTick
	if l.campaignOnStart {
		_ = rawNode.Campaign()
		ticksSinceCampaign = 0
		l.drainReady(rawNode, storage)
	}

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			rawNode.Tick()
			if l.campaignOnStart && !l.IsLeader() {
				ticksSinceCampaign++
				if ticksSinceCampaign >= l.campaignRetryTick {
					_ = rawNode.Campaign()
					ticksSinceCampaign = 0
				}
			}
			l.drainReady(rawNode, storage)
		case proposal := <-l.proposals:
			data, err := json.Marshal(proposal.entry)
			if err == nil {
				err = rawNode.Propose(data)
				l.drainReady(rawNode, storage)
			}
			proposal.result <- err
		case message := <-l.steps:
			if err := rawNode.Step(message); err == nil {
				l.drainReady(rawNode, storage)
			}
		case peerID := <-l.unreachable:
			rawNode.ReportUnreachable(peerID)
			l.drainReady(rawNode, storage)
		}
	}
}

func (l *InLog) drainReady(rawNode *raft.RawNode, storage *raft.MemoryStorage) {
	for rawNode.HasReady() {
		ready := rawNode.Ready()
		if ready.SoftState != nil {
			l.leaderID.Store(ready.SoftState.Lead)
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
			if message.To == l.selfID {
				_ = rawNode.Step(message)
				continue
			}
			queue := l.sendQueues[message.To]
			if queue == nil {
				continue
			}
			select {
			case <-l.stopCh:
				return
			case queue <- message:
			}
		}
		for _, entry := range ready.CommittedEntries {
			l.applyCommitted(rawNode, entry)
		}
		rawNode.Advance(ready)
	}
}

func (l *InLog) applyCommitted(rawNode *raft.RawNode, entry raftpb.Entry) {
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
		l.slot++
		if value.Type == EntryTypeRequest && value.RequestID != "" {
			l.mu.Lock()
			if _, duplicate := l.committedRequests[value.RequestID]; duplicate {
				l.mu.Unlock()
				return
			}
			l.committedRequests[value.RequestID] = struct{}{}
			l.mu.Unlock()
		}
		if l.commit != nil {
			l.commit(l.slot, value)
		}
	}
}

func (l *InLog) runSendWorker(peerID uint64, queue <-chan raftpb.Message) {
	peer := l.peers[peerID]
	for {
		select {
		case <-l.stopCh:
			return
		case message := <-queue:
			payload, err := EncodeRaftMessage(message)
			if err != nil {
				continue
			}
			if err := l.sendRaft(peer, payload); err != nil {
				select {
				case l.unreachable <- peerID:
				default:
				}
			}
		}
	}
}

func EncodeRaftMessage(message raftpb.Message) (map[string]any, error) {
	data, err := message.Marshal()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		protocol.FieldType: protocol.MessageTypeInLogRaft,
		raftMessageKey:     base64.StdEncoding.EncodeToString(data),
	}, nil
}

func DecodeRaftMessage(payload map[string]any) (raftpb.Message, error) {
	encoded, ok := payload[raftMessageKey].(string)
	if !ok || encoded == "" {
		return raftpb.Message{}, fmt.Errorf("missing %q", raftMessageKey)
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return raftpb.Message{}, err
	}
	var message raftpb.Message
	if err := message.Unmarshal(data); err != nil {
		return raftpb.Message{}, err
	}
	return message, nil
}

func buildPeerIDs(self string, peers []string) (map[string]uint64, map[uint64]string, uint64, error) {
	if self == "" {
		return nil, nil, 0, fmt.Errorf("inlog requires a node name")
	}
	if len(peers) == 0 {
		return nil, nil, 0, fmt.Errorf("inlog requires peers")
	}
	unique := make(map[string]struct{}, len(peers)+1)
	for _, peer := range peers {
		if peer != "" {
			unique[peer] = struct{}{}
		}
	}
	unique[self] = struct{}{}

	names := make([]string, 0, len(unique))
	for peer := range unique {
		names = append(names, peer)
	}
	sort.Strings(names)
	peerIDs := make(map[string]uint64, len(names))
	reverse := make(map[uint64]string, len(names))
	for index, peer := range names {
		id := uint64(index + 1)
		peerIDs[peer] = id
		reverse[id] = peer
	}
	return peerIDs, reverse, peerIDs[self], nil
}
