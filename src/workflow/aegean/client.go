package aegeanworkflow

import (
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
	c.TotalProgress = float32(totalRequests * len(c.Next))

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

		sent := false
		for _, nextNode := range c.Next {
			_, err := common.SendMessage(nextNode, 8000, request)
			if err != nil {
				_ = c.TraceLogger.WriteTrace(map[string]any{
					"type":            "request",
					"request_id":      requestID,
					"send_to":         nextNode,
					"status_code":     "error",
					"payload":         request,
					"expected_result": expectedResult,
					"timestamp":       time.Now().Format(time.RFC3339Nano),
				})
				continue
			}
			sent = true
			_ = c.TraceLogger.WriteTrace(map[string]any{
				"type":            "request",
				"request_id":      requestID,
				"send_to":         nextNode,
				"status_code":     "ack",
				"payload":         request,
				"expected_result": expectedResult,
				"timestamp":       time.Now().Format(time.RFC3339Nano),
			})
		}

		if waitForResponse && sent {
			c.WaitForRequestCompletion(requestID)
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
	return ""
}
