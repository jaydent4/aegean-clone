package socialworkflow

import (
	netx "aegean/net"
	"aegean/nodes"
)

func InitSocialGraphExternalService(es *nodes.ExternalService) {
	_ = es
	resetSocialGraphNonidemState()
}

func ExternalServiceSocialGraph(es *nodes.ExternalService, payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	response := socialGraphExternalResponse(es.RunConfig, payload)

	fullResponse := map[string]any{
		"type":                   "response",
		"request_id":             requestID,
		"response":               response,
		"sender":                 es.Name,
		"shim_quorum_aggregated": true,
	}
	if parentRequestID, ok := payload["parent_request_id"]; ok && parentRequestID != nil {
		fullResponse["parent_request_id"] = parentRequestID
	}

	if sender, _ := payload["sender"].(string); sender != "" {
		go func(target string, responsePayload map[string]any) {
			_, _ = netx.SendMessage(target, 8000, responsePayload)
		}(sender, fullResponse)
	}

	return map[string]any{"status": "accepted", "request_id": requestID}
}

func socialGraphExternalResponse(runConfig map[string]any, payload map[string]any) map[string]any {
	requestID := payload["request_id"]
	op, _ := payload["op"].(string)
	if op != "get_followers" && op != "ro_get_followers" {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unsupported op: " + op,
		}
	}

	userID := commonPayloadString(payload, "user_id")
	followers, nonidemVersion := socialGraphFollowersForRunConfig(runConfig, userID, nil)
	response := map[string]any{
		"request_id": requestID,
		"status":     "ok",
		"followers":  followers,
	}
	if nonidemVersion > 0 {
		response["social_graph_nonidem_version"] = nonidemVersion
	}
	if parentRequestID, ok := payload["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}
