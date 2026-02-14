package reqraceworkflow

import (
	"log"
	"time"

	"aegean/common"
	"aegean/nodes"
)

// ClientRequestLogicPlaceholder sends requests sequentially
func ClientRequestLogic(c *nodes.Client) {
	// Wait for other nodes to start.
	time.Sleep(2 * time.Second)

	for requestID := 1; requestID <= 100; requestID++ {
		request := map[string]any{
			"request_id": requestID,
			"timestamp":  float64(time.Now().UnixNano()) / 1e9,
			"sender":     c.Name,
			"op":         "default",
			"op_payload": map[string]any{},
		}

		log.Printf("Client %s sending request %d to %v", c.Name, requestID, c.Next)
		for _, nextNode := range c.Next {
			_, err := common.SendMessage(nextNode, 8000, request)
			if err != nil {
				log.Printf("Failed to send request %d to %s: %v", requestID, nextNode, err)
			}
		}

		c.WaitForRequestCompletion(requestID)
	}
}
