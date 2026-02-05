package nodes

import (
	"fmt"
	"log"
	"sync"
	"time"

	"aegean/common"
)

type Client struct {
	*Node
	Next              []string
	completedRequests map[string]struct{}
	mu                sync.Mutex
}

func NewClient(name, host string, port int, next []string) *Client {
	client := &Client{
		Node:              NewNode(name, host, port),
		Next:              next,
		completedRequests: make(map[string]struct{}),
	}
	client.Node.HandleMessage = client.HandleMessage
	return client
}

func (c *Client) Start() {
	go c.clientWorkflow()
	c.Node.Start()
}

func (c *Client) clientWorkflow() {
	// Wait for other nodes to be turned on. TODO: improvable
	time.Sleep(2 * time.Second)

	logger := GetClientLogger()

	for requestID := 1; requestID <= 10; requestID++ {
		timestamp := float64(time.Now().UnixNano()) / 1e9

		request := map[string]any{
			"request_id": requestID,
			"timestamp":  timestamp,
			"sender":     c.Name,
			"op":         "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   0.1,
				"write_key":   "1",
				"write_value": "value_" + itoa(requestID),
				"read_key":    "1",
			},
		}

		expectedResult := map[string]any{
			"read_value": "value_" + itoa(requestID),
			"request_id": requestID,
			"status":     "ok",
		}
		log.Printf("Client %s sending request %d to %v", c.Name, requestID, c.Next)

		for _, nextNode := range c.Next {
			_, err := common.SendMessage(nextNode, 8000, request)
			if err != nil {
				log.Printf("Failed to send to %s: %v", nextNode, err)
				logger.LogRequest(requestID, nextNode, "error", request, expectedResult)
				continue
			}
			log.Printf("Ack from shim %s", nextNode)
			logger.LogRequest(requestID, nextNode, "ack", request, expectedResult)
		}
	}
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
	// TODO: In full BFT mode, would wait for f+1 matching responses
	log.Printf("Client %s: Request %v completed with: %v", c.Name, requestID, response)

	logger.LogResponse(requestID, sender, payload, response)

	return map[string]any{"status": "response_received", "request_id": requestID}
}

func itoa(value int) string {
	return fmt.Sprintf("%d", value)
}

func toKey(value any) string {
	return fmt.Sprintf("%v", value)
}
