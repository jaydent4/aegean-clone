package shim

import (
	"aegean/common"
	netx "aegean/net"
	"aegean/telemetry"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	nestedWaitMu         sync.Mutex
	nestedWaitSpans      map[string]trace.Span
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
		nestedWaitSpans:      make(map[string]trace.Span),
	}
	return shim
}

func (s *Shim) HandleRequestMessage(payload map[string]any) map[string]any {
	// Handle incoming client request - wait for quorum then forward
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	baseAttrs := append(
		telemetry.AttrsFromPayload(payload),
		attribute.String("node.name", s.Name),
	)

	if !s.isPrimaryBatcher {
		_, span := telemetry.StartLocalSpanFromPayload(
			payload,
			"shim.request_admission",
			append(baseAttrs, attribute.String("shim.admission_status", "ignored_non_primary"))...,
		)
		span.End()
		return map[string]any{"status": "ignored_non_primary"}
	}

	if !s.requestQuorumHelper.Add(requestID, sender) {
		_, span := telemetry.StartLocalSpanFromPayload(
			payload,
			"shim.request_admission",
			append(baseAttrs, attribute.String("shim.admission_status", "waiting_for_quorum"))...,
		)
		span.End()
		return map[string]any{"status": "waiting_for_quorum"}
	}
	if s.BatcherCh != nil {
		s.BatcherCh <- payload
	}
	_, span := telemetry.StartLocalSpanFromPayload(
		payload,
		"shim.request_admission",
		append(baseAttrs, attribute.String("shim.admission_status", "forwarded_to_batcher"))...,
	)
	span.End()
	return map[string]any{"status": "forwarded_to_mid_execs"}
}

func (s *Shim) HandleIncomingResponse(payload map[string]any) map[string]any {

	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if sender == "" {
		return map[string]any{"status": "error", "error": "missing sender"}
	}

	s.startNestedQuorumWait(requestID, payload)

	if !s.responseQuorumHelper.Add(requestID, sender) {
		return map[string]any{"status": "waiting_for_quorum", "request_id": requestID}
	}

	s.finishNestedQuorumWait(requestID)

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
		"type":       "response",
		"request_id": requestID,
		"response":   responseData,
		"sender":     sender,
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

func (s *Shim) startNestedQuorumWait(requestID any, payload map[string]any) {
	key := fmt.Sprintf("%v", requestID)

	s.nestedWaitMu.Lock()
	defer s.nestedWaitMu.Unlock()
	if _, exists := s.nestedWaitSpans[key]; exists {
		return
	}

	_, span := telemetry.StartLocalSpanFromPayload(
		payload,
		"shim.nested_response_quorum_wait",
		append(
			telemetry.AttrsFromPayload(payload),
			attribute.String("node.name", s.Name),
		)...,
	)
	s.nestedWaitSpans[key] = span
}

func (s *Shim) finishNestedQuorumWait(requestID any) {
	key := fmt.Sprintf("%v", requestID)

	s.nestedWaitMu.Lock()
	span, exists := s.nestedWaitSpans[key]
	if exists {
		delete(s.nestedWaitSpans, key)
	}
	s.nestedWaitMu.Unlock()

	if exists && span != nil {
		span.End()
	}
}
