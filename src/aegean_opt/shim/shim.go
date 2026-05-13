package shim

import (
	"aegean/common"
	netx "aegean/net"
	"aegean/telemetry"
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
}

func NewShim(name string, batcherCh chan<- map[string]any, execCh chan<- map[string]any, clients []string, peers []string, isPrimaryBatcher bool, quorumSize int) *Shim {
	if batcherCh == nil {
		panic("shim component requires non-nil batcherCh")
	}
	shim := &Shim{
		Name:                name,
		BatcherCh:           batcherCh,
		ExecCh:              execCh,
		Clients:             clients,
		Peers:               peers,
		isPrimaryBatcher:    isPrimaryBatcher,
		requestQuorumHelper: common.NewQuorumHelper(quorumSize),
		// TODO: quorumSize should depend on size of nested service
		responseQuorumHelper: common.NewQuorumHelper(quorumSize),
	}
	return shim
}

func (s *Shim) HandleRequestMessage(payload map[string]any) map[string]any {
	// Handle incoming client request - wait for quorum then forward
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if !s.isPrimaryBatcher {
		return map[string]any{"status": "ignored_non_primary"}
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

	fullResponse := map[string]any{
		"type":                   "response",
		"request_id":             requestID,
		"response":               responseData,
		"sender":                 sender,
		"shim_quorum_aggregated": true,
	}
	if parentRequestID, ok := payload["parent_request_id"]; ok && parentRequestID != nil {
		fullResponse["parent_request_id"] = parentRequestID
	}
	telemetry.CopyContext(fullResponse, payload)

	// Handle response from exec - broadcast to all clients that sent the request
	// TODO: Or do we wait for a quorum, and then broadcast

	for _, client := range s.Clients {
		if _, err := netx.SendMessage(client, 8000, fullResponse); err != nil {
			continue
		}
	}

	return map[string]any{"status": "response_broadcast", "recipients": s.Clients}
}
