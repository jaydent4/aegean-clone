package shim

import (
	"aegean/common"
	"fmt"
	"sync"
	"sync/atomic"
)

type Shim struct {
	Name                 string
	BatcherCh            chan<- map[string]any
	ExecCh               chan<- map[string]any
	Clients              []string
	Peers                []string
	isPrimaryBatcher     bool
	requestQuorumHelper  *common.QuorumHelper
	responseQuorumHelper *common.QuorumHelper
	waitersMu            sync.Mutex
	waiters              map[string]chan map[string]any
	ohaRequestSeq        uint64
}

func NewShim(name string, batcherCh chan<- map[string]any, execCh chan<- map[string]any, clients []string, peers []string, isPrimaryBatcher bool, quorumSize int) *Shim {
	if batcherCh == nil {
		panic("shim component requires non-nil batcherCh")
	}
	shim := &Shim{
		Name:                 name,
		BatcherCh:            batcherCh,
		ExecCh:               execCh,
		Clients:              clients,
		Peers:                peers,
		isPrimaryBatcher:     isPrimaryBatcher,
		requestQuorumHelper:  common.NewQuorumHelper(quorumSize),
		responseQuorumHelper: common.NewQuorumHelper(quorumSize),
		waiters:              make(map[string]chan map[string]any),
	}
	return shim
}

func (s *Shim) HandleRequestMessage(payload map[string]any) map[string]any {

	msgType, _ := payload["type"].(string)
	if msgType == "" {
		msgType = "request"
	}

	isClientOHA, _ := payload["is_client_oha"].(bool)
	if isClientOHA {
		// OHA may replay lines from body files under load; assign a monotonic
		// request_id at the shim edge if one is not provided.
		if _, exists := payload["request_id"]; !exists {
			payload["request_id"] = atomic.AddUint64(&s.ohaRequestSeq, 1)
		}
	}

	// Handle incoming client request - wait for quorum then forward
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	requestKey := fmt.Sprintf("%v", requestID)

	if !s.isPrimaryBatcher && !isClientOHA {
		return map[string]any{"status": "ignored_non_primary"}
	}

	// If client is Oha: broadcast to peers, then return the actual response when everyone finishes
	if isClientOHA {
		var peerDone <-chan struct{}
		closed := make(chan struct{})
		close(closed)
		peerDone = closed
		if forwarded, _ := payload["oha_broadcasted"].(bool); !forwarded {
			peerDone = s.broadcastOHARequestAsync(payload)
		}

		s.waitersMu.Lock()
		if _, exists := s.waiters[requestKey]; exists {
			s.waitersMu.Unlock()
			return map[string]any{"status": "duplicate_oha_request_waiting_for_inflight_response", "request_id": requestID}
		}
		waiter := make(chan map[string]any, 1)
		s.waiters[requestKey] = waiter
		s.waitersMu.Unlock()
		if s.BatcherCh != nil {
			s.BatcherCh <- payload
		}
		localResponse := <-waiter
		<-peerDone
		return localResponse
	}

	if !s.requestQuorumHelper.Add(requestID, sender) {
		return map[string]any{"status": "waiting_for_quorum"}
	}
	if s.BatcherCh != nil {
		s.BatcherCh <- payload
	}
	return map[string]any{"status": "forwarded_to_mid_execs"}
}

func (s *Shim) HandleIncomingResponse(payload map[string]any) map[string]any {

	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if sender == "" {
		return map[string]any{"status": "error", "error": "missing sender"}
	}

	if !s.responseQuorumHelper.Add(requestID, sender) {
		return map[string]any{"status": "waiting_for_quorum", "request_id": requestID}
	}

	// This assumes nested responses are equivalent across backends
	payload["shim_quorum_aggregated"] = true

	if s.ExecCh != nil {
		s.ExecCh <- payload
		return map[string]any{"status": "forwarded_nested_response", "request_id": requestID}
	}
	return map[string]any{"status": "error", "error": "exec channel not configured", "request_id": requestID}
}

func (s *Shim) HandleOutgoingResponse(payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	responseData, _ := payload["response"].(map[string]any)
	sender := s.Name
	requestKey := fmt.Sprintf("%v", requestID)
	s.waitersMu.Lock()
	waiter, hasWaiter := s.waiters[requestKey]
	s.waitersMu.Unlock()

	fullResponse := map[string]any{
		"type":       "response",
		"request_id": requestID,
		"response":   responseData,
		"sender":     sender,
	}

	// Exec removes the "is_client_oha" field, but hasWaiter can also do the job
	if hasWaiter {
		s.waitersMu.Lock()
		delete(s.waiters, requestKey)
		s.waitersMu.Unlock()
		waiter <- fullResponse
		return map[string]any{"status": "response_returned_inline", "request_id": requestID}
	}

	// Handle response from exec - broadcast to all clients that sent the request
	// TODO: Or do we wait for a quorum, and then broadcast

	for _, client := range s.Clients {
		if _, err := common.SendMessage(client, 8000, fullResponse); err != nil {
			continue
		}
	}

	return map[string]any{"status": "response_broadcast", "recipients": s.Clients}
}

func (s *Shim) broadcastOHARequestAsync(payload map[string]any) <-chan struct{} {
	done := make(chan struct{})
	var wg sync.WaitGroup

	broadcasted := false
	for _, peer := range s.Peers {
		if peer == "" || peer == s.Name {
			continue
		}
		broadcasted = true
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()
			outgoing := make(map[string]any, len(payload)+3)
			for k, v := range payload {
				outgoing[k] = v
			}
			outgoing["type"] = "request"
			outgoing["sender"] = s.Name
			outgoing["oha_broadcasted"] = true
			outgoing["is_client_oha"] = true
			_, _ = common.SendMessage(peer, 8000, outgoing)
		}(peer)
	}

	if !broadcasted {
		close(done)
		return done
	}

	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}
