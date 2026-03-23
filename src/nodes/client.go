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

type Client struct {
	*Node
	Next                []string
	ReadyNodes          []string
	completedRequests   map[string]struct{}
	pending             map[string]chan map[string]any
	mu                  sync.Mutex
	cond                *sync.Cond
	requestSeq          uint64
	finalResponseQuorum *common.QuorumHelper
	clientType          string
	RequestLogic        func(c *Client)
	RunConfig           map[string]any
}

func NewClient(name, host string, port int, next []string, readyNodes []string, runConfig map[string]any, clientType string, requestLogic func(c *Client)) *Client {
	if requestLogic == nil {
		panic("client requires RequestLogic")
	}
	client := &Client{
		Node:                NewNode(name, host, port),
		Next:                next,
		ReadyNodes:          append([]string{}, readyNodes...),
		completedRequests:   make(map[string]struct{}),
		pending:             make(map[string]chan map[string]any),
		finalResponseQuorum: common.NewQuorumHelper(len(next)/2 + 1),
		clientType:          clientType,
		RequestLogic:        requestLogic,
		RunConfig:           runConfig,
	}
	client.cond = sync.NewCond(&client.mu)
	client.Node.HandleMessage = client.HandleMessage
	client.Node.HandleReady = client.HandleReady
	return client
}

func (c *Client) Start() {
	go func() {
		c.WaitForNodesReady([]string{c.Name})
		c.RequestLogic(c)
	}()
	c.Node.Start()
}

func (c *Client) HandleMessage(payload map[string]any) map[string]any {
	msgType, _ := payload["type"].(string)
	if msgType == "response" {
		return c.handleResponse(payload)
	}
	return c.handleRequest(payload)
}

func (c *Client) handleRequest(payload map[string]any) map[string]any {
	requestID := atomic.AddUint64(&c.requestSeq, 1)
	requestKey := toKey(requestID)

	doneCh := make(chan map[string]any, 1)
	c.mu.Lock()
	c.pending[requestKey] = doneCh
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
		log.Printf("warning: %s request dispatch failed for request_id=%v last_error=%v", c.clientType, requestID, lastError)
		return map[string]any{
			"status":     "error",
			"error":      "dispatch_failed",
			"request_id": requestID,
			"detail":     fmt.Sprintf("%v", lastError),
		}
	}

	return <-doneCh
}

func (c *Client) handleResponse(payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)
	if sender == "" {
		return map[string]any{"status": "error", "error": "missing sender"}
	}

	requestKey := toKey(requestID)

	c.mu.Lock()
	doneCh, ok := c.pending[requestKey]
	if !ok {
		c.mu.Unlock()
		return map[string]any{"status": "already_completed"}
	}
	c.mu.Unlock()

	if c.finalResponseQuorum.Add(requestID, sender) {
		c.mu.Lock()
		delete(c.pending, requestKey)
		c.mu.Unlock()
		c.MarkRequestCompleted(requestID)
		select {
		case doneCh <- payload:
		default:
		}
		return map[string]any{"status": "response_quorum_reached", "request_id": requestID}
	}

	return map[string]any{"status": "response_recorded", "request_id": requestID}
}

func (c *Client) WaitForRequestCompletion(requestID any) {
	key := toKey(requestID)

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if _, done := c.completedRequests[key]; done {
			return
		}
		c.cond.Wait()
	}
}

func (c *Client) WaitForNodesReady(nodeNames []string) {
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

func (c *Client) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}

func (c *Client) MarkRequestCompleted(requestID any) {
	key := toKey(requestID)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.completedRequests[key] = struct{}{}
	c.cond.Broadcast()
}

func toKey(value any) string {
	return fmt.Sprintf("%v", value)
}
