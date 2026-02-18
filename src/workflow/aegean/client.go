package aegeanworkflow

import (
	"log"
	"strconv"
	"time"

	"aegean/common"
	"aegean/nodes"
)

const (
	writeKeyMod   = 1000
	readKeyMod    = 1000
	totalRequests = 1000
)

var readyNodes = []string{"node2", "node3", "node4", "node5", "node6", "node7"}

func ClientRequestLogicPipelined(c *nodes.Client) {
	runClientRequestLogic(c, false)
}

func ClientRequestLogic(c *nodes.Client) {
	runClientRequestLogic(c, true)
}

func runClientRequestLogic(c *nodes.Client, waitForResponse bool) {
	c.WaitForNodesReady(readyNodes)

	logger := nodes.GetClientLogger()

	progressIncrement := 1.0 / float64(totalRequests)
	for requestID := 1; requestID <= totalRequests; requestID++ {
		timestamp := float64(time.Now().UnixNano()) / 1e9

		request := map[string]any{
			"request_id": requestID,
			"timestamp":  timestamp,
			"sender":     c.Name,
			"op":         "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   0.01,
				"write_key":   strconv.Itoa(requestID % writeKeyMod),
				"write_value": "value_" + strconv.Itoa(requestID),
				"read_key":    strconv.Itoa(requestID % readKeyMod),
			},
		}

		expectedResult := map[string]any{
			"read_value": expectedReadValue(requestID),
			"request_id": requestID,
			"status":     "ok",
		}
		log.Printf("Client %s sending request %d to %v", c.Name, requestID, c.Next)

		sent := false
		for _, nextNode := range c.Next {
			_, err := common.SendMessage(nextNode, 8000, request)
			if err != nil {
				log.Printf("Failed to send to %s: %v", nextNode, err)
				logger.LogRequest(requestID, nextNode, "error", request, expectedResult)
				continue
			}
			sent = true
			log.Printf("Ack from shim %s", nextNode)
			logger.LogRequest(requestID, nextNode, "ack", request, expectedResult)
		}

		if waitForResponse && sent {
			log.Printf("Client %s waiting for response to request %d", c.Name, requestID)
			c.WaitForRequestCompletion(requestID)
			c.IncrementProgress(progressIncrement)
		}
	}
}

func expectedReadValue(requestID int) string {
	readKey := requestID % readKeyMod
	for candidate := requestID; candidate >= 1; candidate-- {
		if candidate%writeKeyMod == readKey {
			return "value_" + strconv.Itoa(candidate)
		}
	}
	if readKey == 1 {
		return "111"
	}
	return ""
}
