package aegeanworkflow

import (
	"strconv"
	"strings"
	"time"

	"aegean/common"
	netx "aegean/net"
	"aegean/nodes"
)

func ClientRequestLogicPipelined(c *nodes.Client) {
	runClientRequestLogic(c, false)
}

func ClientRequestLogic(c *nodes.Client) {
	runClientRequestLogic(c, true)
}

func runClientRequestLogic(c *nodes.Client, waitForResponse bool) {
	numRequests := common.MustInt(c.RunConfig, "num_requests")
	spinTimeSeconds := common.MustFloat64(c.RunConfig, "spin_time_seconds")
	writeKeyMod := common.MustInt(c.RunConfig, "write_key_mod")
	readKeyMod := common.MustInt(c.RunConfig, "read_key_mod")
	valueLength := common.MustInt(c.RunConfig, "value_length")

	c.WaitForNodesReady(c.ReadyNodes)

	for requestID := 1; requestID <= numRequests; requestID++ {
		timestamp := float64(time.Now().UnixNano()) / 1e9

		request := map[string]any{
			"request_id": requestID,
			"timestamp":  timestamp,
			"sender":     c.Name,
			"op":         "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   spinTimeSeconds,
				"write_key":   strconv.Itoa(requestID % writeKeyMod),
				"write_value": makeLargeWriteValue(requestID, valueLength),
				"read_key":    strconv.Itoa(requestID % readKeyMod),
			},
		}

		expectedResult := map[string]any{
			"read_value": expectedReadValue(requestID, writeKeyMod, readKeyMod, valueLength),
			"request_id": requestID,
			"status":     "ok",
		}

		sent := false
		for _, nextNode := range c.Next {
			_, err := netx.SendMessage(nextNode, 8000, request)
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

func expectedReadValue(requestID int, writeKeyMod int, readKeyMod int, valueLength int) string {
	readKey := requestID % readKeyMod
	for candidate := requestID; candidate >= 1; candidate-- {
		if candidate%writeKeyMod == readKey {
			return makeLargeWriteValue(candidate, valueLength)
		}
	}
	return ""
}

func makeLargeWriteValue(requestID int, valueLength int) string {
	token := strconv.Itoa(requestID)
	repeat := valueLength/len(token) + 1
	return strings.Repeat(token, repeat)
}
