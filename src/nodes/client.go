package nodes

import (
	"aegean/common"
	"fmt"
	"log"
	"sync"
	"time"
)

type Client struct {
	*Node
	Next              []string
	completedRequests map[string]struct{}
	mu                sync.Mutex
	cond              *sync.Cond
	RequestLogic      func(c *Client)

	// For /progress
	progress         float64
	workflowFinished bool
}

func NewClient(name, host string, port int, next []string, requestLogic func(c *Client)) *Client {
	if requestLogic == nil {
		log.Fatalf("client requires RequestLogic")
	}
	client := &Client{
		Node:              NewNode(name, host, port),
		Next:              next,
		completedRequests: make(map[string]struct{}),
		RequestLogic:      requestLogic,
	}
	client.cond = sync.NewCond(&client.mu)
	client.Node.HandleMessage = client.HandleMessage
	client.Node.HandleProgress = client.HandleProgress
	client.Node.HandleReady = client.HandleReady
	return client
}

func (c *Client) Start() {
	go func() {
		c.RequestLogic(c)
		c.mu.Lock()
		c.workflowFinished = true
		c.mu.Unlock()
	}()
	c.Node.Start()
}

func (c *Client) HandleMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", c.Name, payload)

	requestID := payload["request_id"]
	response, _ := payload["response"].(map[string]any)
	sender, _ := payload["sender"].(string)
	key := toKey(requestID)

	logger := GetClientLogger()

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, done := c.completedRequests[key]; done {
		log.Printf("Client %s: Ignoring duplicate response for %v", c.Name, requestID)
		logger.LogResponse(requestID, sender, payload, response)
		return map[string]any{"status": "already_completed"}
	}

	// In CFT mode with a single exec pipeline, one response is sufficient
	log.Printf("Client %s: Received response for request %v: %v", c.Name, requestID, response)
	c.completedRequests[key] = struct{}{}
	c.cond.Broadcast()
	// TODO: In full BFT mode, would wait for f+1 matching responses
	log.Printf("Client %s: Request %v completed with: %v", c.Name, requestID, response)

	logger.LogResponse(requestID, sender, payload, response)

	return map[string]any{"status": "response_received", "request_id": requestID}
}

func toKey(value any) string {
	return fmt.Sprintf("%v", value)
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
			response, err := common.SendMessageToPath(nodeName, 8000, "/ready", map[string]any{})
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

func (c *Client) IncrementProgress(delta float64) {
	c.mu.Lock()
	c.progress += delta
	c.mu.Unlock()
}

func (c *Client) HandleProgress(payload map[string]any) map[string]any {
	c.mu.Lock()
	progress := c.progress
	finished := c.workflowFinished
	c.mu.Unlock()

	return map[string]any{
		"progress": progress,
		"finished": finished,
	}
}

func (c *Client) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}
