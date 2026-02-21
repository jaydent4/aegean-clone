package nodes

import (
	"log"
	"sync"
)

type OHAClient struct {
	*Client
	mu       sync.Mutex
	finished bool
}

func NewOHAClient(name, host string, port int, next []string, requestLogic func(c *Client)) *OHAClient {
	if requestLogic == nil {
		panic("oha client requires RequestLogic")
	}
	baseClient := NewClient(name, host, port, next, requestLogic)
	client := &OHAClient{
		Client: baseClient,
	}
	client.Node.HandleMessage = client.HandleMessage
	client.Node.HandleProgress = client.HandleProgress
	client.Node.HandleReady = client.HandleReady
	return client
}

func (c *OHAClient) Start() {
	go func() {
		c.RequestLogic(c.Client)
		c.mu.Lock()
		c.finished = true
		c.mu.Unlock()
	}()
	c.Node.Start()
}

func (c *OHAClient) HandleMessage(payload map[string]any) map[string]any {
	log.Printf("warning: oha client should not receive any messages")
	return map[string]any{}
}

func (c *OHAClient) HandleProgress(payload map[string]any) map[string]any {
	c.mu.Lock()
	finished := c.finished
	c.mu.Unlock()

	progress := float32(0)
	if finished {
		progress = 1
	}

	return map[string]any{
		"progress":               progress,
		"finished":               finished,
		"disableProgressTimeout": true,
	}
}

func (c *OHAClient) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}
