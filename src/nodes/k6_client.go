package nodes

import (
	"aegean/common"
	netx "aegean/net"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type K6Client struct {
	*Node
	Next              []string
	ReadyNodes        []string
	completedRequests map[string]struct{}
	mu                sync.Mutex
	cond              *sync.Cond
	requestSeq        uint64
	pending           map[string]*pendingK6Request
	// TODO: QuorumHelper currently has no per-request cleanup/reset API
	finalResponseQuorum *common.QuorumHelper
	RequestLogic        func(c *K6Client)
	RunConfig           map[string]any
}

type pendingK6Request struct {
	doneCh chan map[string]any
}

func NewK6Client(name, host string, port int, next []string, readyNodes []string, runConfig map[string]any, requestLogic func(c *K6Client)) *K6Client {
	if requestLogic == nil {
		panic("k6 client requires RequestLogic")
	}
	quorumSize := len(next)/2 + 1
	client := &K6Client{
		Node:                NewNode(name, host, port),
		Next:                next,
		ReadyNodes:          append([]string{}, readyNodes...),
		completedRequests:   make(map[string]struct{}),
		pending:             make(map[string]*pendingK6Request),
		finalResponseQuorum: common.NewQuorumHelper(quorumSize),
		RequestLogic:        requestLogic,
		RunConfig:           runConfig,
	}
	client.cond = sync.NewCond(&client.mu)
	client.Node.HandleMessage = client.HandleMessage
	client.Node.HandleReady = client.HandleReady
	return client
}

func (c *K6Client) Start() {
	go func() {
		c.WaitForNodesReady([]string{c.Name})
		c.RequestLogic(c)
	}()
	c.Node.Start()
}

func (c *K6Client) HandleMessage(payload map[string]any) map[string]any {
	msgType, _ := payload["type"].(string)
	if msgType == "response" {
		return c.handleResponse(payload)
	} else {
		return c.handleRequest(payload)
	}
}

func (c *K6Client) handleRequest(payload map[string]any) map[string]any {
	requestID := atomic.AddUint64(&c.requestSeq, 1)
	requestKey := fmt.Sprintf("%v", requestID)

	pendingReq := &pendingK6Request{
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
		log.Printf("warning: k6 request dispatch failed for request_id=%v last_error=%v", requestID, lastError)
		return map[string]any{
			"status":     "error",
			"error":      "dispatch_failed",
			"request_id": requestID,
			"detail":     fmt.Sprintf("%v", lastError),
		}
	}

	return <-pendingReq.doneCh
}

func (c *K6Client) handleResponse(payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if sender == "" {
		return map[string]any{"status": "error", "error": "missing sender"}
	}

	requestKey := fmt.Sprintf("%v", requestID)

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
		c.completedRequests[requestKey] = struct{}{}
		c.cond.Broadcast()
		c.mu.Unlock()
		select {
		case pendingReq.doneCh <- payload:
		default:
		}
		return map[string]any{"status": "response_quorum_reached", "request_id": requestID}
	}

	return map[string]any{"status": "response_recorded", "request_id": requestID}
}

func (c *K6Client) WaitForRequestCompletion(requestID any) {
	key := fmt.Sprintf("%v", requestID)

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if _, done := c.completedRequests[key]; done {
			return
		}
		c.cond.Wait()
	}
}

func (c *K6Client) WaitForNodesReady(nodeNames []string) {
	for {
		allReady := true
		for _, nodeName := range nodeNames {
			response, err := netx.SendMessageToPath(nodeName, 8000, "/ready", map[string]any{})
			if err != nil {
				allReady = false
				break
			}
			ready, _ := response["ready"].(bool)
			if !ready {
				allReady = false
				break
			}
		}

		if allReady {
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func (c *K6Client) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}
