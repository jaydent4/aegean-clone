package reqraceworkflow

import (
	"time"

	"aegean/common"
	netx "aegean/net"
	"aegean/nodes"
)

// ClientRequestLogicPlaceholder sends requests sequentially
func ClientRequestLogic(c *nodes.Client) {
	totalRequests := common.MustInt(c.RunConfig, "num_requests")

	c.WaitForNodesReady(c.ReadyNodes)

	for requestID := 1; requestID <= totalRequests; requestID++ {
		request := map[string]any{
			"request_id": requestID,
			"timestamp":  float64(time.Now().UnixNano()) / 1e9,
			"sender":     c.Name,
			"op":         "default",
			"op_payload": map[string]any{},
		}
		for _, nextNode := range c.Next {
			_, err := netx.SendMessage(nextNode, 8000, request)
			if err != nil {
			}
		}

		c.WaitForRequestCompletion(requestID)
	}
}
