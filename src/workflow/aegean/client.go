package aegeanworkflow

import (
	"log"
	"strconv"
	"time"

	"aegean/common"
	"aegean/nodes"
)

func ClientRequestLogic(c *nodes.Client) {
	// Wait for other nodes to be turned on. TODO: improvable
	time.Sleep(2 * time.Second)

	logger := nodes.GetClientLogger()

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
				"write_value": "value_" + strconv.Itoa(requestID),
				"read_key":    "1",
			},
		}

		expectedResult := map[string]any{
			"read_value": "value_" + strconv.Itoa(requestID),
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
