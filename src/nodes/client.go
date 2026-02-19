package nodes

import (
	"aegean/common"
	"fmt"
	"sync"
	"time"
)

type Client struct {
	*Node
	Next              []string
	completedRequests map[string]struct{}
	TraceLogger       *common.TraceLogger
	mu                sync.Mutex
	cond              *sync.Cond
	RequestLogic      func(c *Client)

	// For /progress
	progress      float32
	TotalProgress float32
}

const clientTraceLogPath = "/tmp/client_result.jsonl"

func NewClient(name, host string, port int, next []string, requestLogic func(c *Client)) *Client {
	if requestLogic == nil {
		panic("client requires RequestLogic")
	}
	client := &Client{
		Node:              NewNode(name, host, port),
		Next:              next,
		completedRequests: make(map[string]struct{}),
		RequestLogic:      requestLogic,
	}
	traceLogger, err := common.NewTraceLogger(clientTraceLogPath)
	if err != nil {
		client.TraceLogger = common.NewNoopTraceLogger()
	} else {
		client.TraceLogger = traceLogger
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
	}()
	c.Node.Start()
}

func (c *Client) HandleMessage(payload map[string]any) map[string]any {

	requestID := payload["request_id"]
	response, _ := payload["response"].(map[string]any)
	sender, _ := payload["sender"].(string)
	key := toKey(requestID)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, done := c.completedRequests[key]; done {
		_ = c.TraceLogger.WriteTrace(map[string]any{
			"type":          "response",
			"request_id":    requestID,
			"receive_from":  sender,
			"payload":       payload,
			"actual_result": response,
			"timestamp":     time.Now().Format(time.RFC3339Nano),
		})
		return map[string]any{"status": "already_completed"}
	}

	// In CFT mode with a single exec pipeline, one response is sufficient
	c.completedRequests[key] = struct{}{}
	c.progress++
	c.cond.Broadcast()
	// TODO: In full BFT mode, would wait for f+1 matching responses

	_ = c.TraceLogger.WriteTrace(map[string]any{
		"type":          "response",
		"request_id":    requestID,
		"receive_from":  sender,
		"payload":       payload,
		"actual_result": response,
		"timestamp":     time.Now().Format(time.RFC3339Nano),
	})

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

func (c *Client) HandleProgress(payload map[string]any) map[string]any {
	c.mu.Lock()
	progress := c.progress
	totalProgress := c.TotalProgress
	c.mu.Unlock()

	return map[string]any{
		"progress": progress / totalProgress,
		"finished": progress == totalProgress,
	}
}

func (c *Client) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}
