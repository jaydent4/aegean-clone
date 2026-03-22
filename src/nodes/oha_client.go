package nodes

import (
	"aegean/common"
	netx "aegean/net"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type OHAClient struct {
	*Client
	mu         sync.Mutex
	requestSeq uint64
	pending    map[string]*pendingOHARequest
	// TODO: QuorumHelper currently has no per-request cleanup/reset API
	finalResponseQuorum *common.QuorumHelper
}

type pendingOHARequest struct {
	doneCh chan map[string]any
}

func NewOHAClient(name, host string, port int, next []string, readyNodes []string, runConfig map[string]any, requestLogic func(c *Client)) *OHAClient {
	if requestLogic == nil {
		panic("oha client requires RequestLogic")
	}
	baseClient := NewClient(name, host, port, next, readyNodes, runConfig, requestLogic)
	quorumSize := len(next)/2 + 1
	client := &OHAClient{
		Client:              baseClient,
		pending:             make(map[string]*pendingOHARequest),
		finalResponseQuorum: common.NewQuorumHelper(quorumSize),
	}
	client.Node.HandleMessage = client.HandleMessage
	client.Node.HandleReady = client.HandleReady
	return client
}

func (c *OHAClient) Start() {
	go func() {
		c.WaitForNodesReady([]string{c.Name})
		c.RequestLogic(c.Client)
	}()
	c.Node.Start()
}

func (c *OHAClient) HandleMessage(payload map[string]any) map[string]any {
	msgType, _ := payload["type"].(string)
	if msgType == "response" {
		return c.handleResponse(payload)
	} else {
		return c.handleRequest(payload)
	}
}

func (c *OHAClient) handleRequest(payload map[string]any) map[string]any {
	requestID := atomic.AddUint64(&c.requestSeq, 1)
	requestKey := toKey(requestID)

	pendingReq := &pendingOHARequest{
		doneCh: make(chan map[string]any, 1),
	}
	c.mu.Lock()
	c.pending[requestKey] = pendingReq
	c.mu.Unlock()

	outgoing := make(map[string]any, len(payload)+4)
	for k, v := range payload {
		outgoing[k] = v
	}
	outgoing["type"] = "request"
	outgoing["request_id"] = requestID
	outgoing["sender"] = c.Name

	type sendResult struct {
		node     string
		response map[string]any
		err      error
	}

	results := make(chan sendResult, len(c.Next))
	for _, nextNode := range c.Next {
		go func(target string) {
			response, err := netx.SendMessage(target, 8000, outgoing)
			results <- sendResult{node: target, response: response, err: err}
		}(nextNode)
	}

	ackResponders := make(map[string]struct{}, len(c.Next))
	var lastError error

	for i := 0; i < len(c.Next); i++ {
		result := <-results
		if result.err != nil {
			lastError = result.err
			continue
		}

		sender, _ := result.response["sender"].(string)
		if sender == "" {
			sender = result.node
		}
		if _, seen := ackResponders[sender]; seen {
			continue
		}
		ackResponders[sender] = struct{}{}
	}

	if len(ackResponders) == 0 {
		c.mu.Lock()
		delete(c.pending, requestKey)
		c.mu.Unlock()
		log.Printf("warning: oha request dispatch failed for request_id=%v last_error=%v", requestID, lastError)
		return map[string]any{
			"status":     "error",
			"error":      "dispatch_failed",
			"request_id": requestID,
			"detail":     fmt.Sprintf("%v", lastError),
		}
	}

	return <-pendingReq.doneCh
}

func (c *OHAClient) handleResponse(payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if sender == "" {
		return map[string]any{"status": "error", "error": "missing sender"}
	}

	requestKey := toKey(requestID)

	c.mu.Lock()
	pendingReq, ok := c.pending[requestKey]
	if !ok {
		c.mu.Unlock()
		return map[string]any{"status": "already_completed"}
	}
	c.mu.Unlock()

	if c.finalResponseQuorum.Add(requestID, sender) {
		c.mu.Lock()
		delete(c.pending, requestKey)
		c.mu.Unlock()
		select {
		case pendingReq.doneCh <- payload:
		default:
		}
		return map[string]any{"status": "response_quorum_reached", "request_id": requestID}
	}

	return map[string]any{"status": "response_recorded", "request_id": requestID}
}

func (c *OHAClient) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}
