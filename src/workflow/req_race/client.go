package reqraceworkflow

import (
	"log"
	"time"

	"aegean/common"
	"aegean/nodes"
)

const totalRequests = 1000

var readyNodes = []string{
	"node2", "node3", "node4",
	"node5", "node6", "node7",
	"node8", "node9", "node10",
	"node11", "node12", "node13",
}

// ClientRequestLogicPlaceholder sends requests sequentially
func ClientRequestLogic(c *nodes.Client) {
	c.WaitForNodesReady(readyNodes)

	progressIncrement := 1.0 / float64(totalRequests)
	for requestID := 1; requestID <= totalRequests; requestID++ {
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
		c.IncrementProgress(progressIncrement)
	}
}
