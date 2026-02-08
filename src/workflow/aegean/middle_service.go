package aegeanworkflow

import (
	"log"
	"time"

	"aegean/common"
	"aegean/components/exec"
)

var responseQuorum = common.NewQuorumHelper(2)

func ExecuteRequest(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)

	// Execute a single request and return the response
	response := map[string]any{"request_id": requestID}

	switch op {
	case "spin_write_read":
		spinTime := common.GetFloat(opPayload, "spin_time")
		writeKey := common.GetString(opPayload, "write_key")
		writeValue := common.GetString(opPayload, "write_value")
		readKey := common.GetString(opPayload, "read_key")

		// Spin for the given time
		if spinTime > 0 {
			time.Sleep(time.Duration(spinTime * float64(time.Second)))
		}

		// Write to key
		e.WriteKV(writeKey, writeValue)
		// Read from key
		response["read_value"] = e.ReadKV(readKey)
		response["status"] = "ok"
	default:
		response["status"] = "error"
		response["error"] = "Unknown op: " + op
	}

	_ = ndSeed
	_ = ndTimestamp
	return response
}

func ExecuteRequestFanout(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	response := ExecuteRequest(e, request, ndSeed, ndTimestamp)

	fanoutTargets := []string{"node7", "node8", "node9"}
	var fanoutResponse map[string]any
	for _, target := range fanoutTargets {
		outgoing := map[string]any{
			"type":       "request",
			"request_id": request["request_id"],
			"timestamp":  request["timestamp"],
			"sender":     e.Name,
			"op":         request["op"],
			"op_payload": request["op_payload"],
		}
		resp, err := common.SendMessage(target, 8000, outgoing)
		if err != nil {
			log.Printf("Fanout from %s to %s failed: %v", e.Name, target, err)
			continue
		}
		if fanoutResponse == nil {
			fanoutResponse = resp
		}
	}

	if fanoutResponse != nil {
		return fanoutResponse
	}
	return response
}

func ResponseForwardToClients(e *exec.Exec, payload map[string]any) map[string]any {
	sender, _ := payload["sender"].(string)
	if !responseQuorum.Add(payload["request_id"], sender) {
		return map[string]any{"status": "waiting_for_quorum", "request_id": payload["request_id"]}
	}
	clientResponse := map[string]any{
		"type":       "response",
		"request_id": payload["request_id"],
		"response":   payload["response"],
		"sender":     sender,
	}

	clients := []string{"node1", "node2", "node3"}
	for _, client := range clients {
		if _, err := common.SendMessage(client, 8000, clientResponse); err != nil {
			log.Printf("Failed to send response to %s: %v", client, err)
		}
	}
	return map[string]any{"status": "response_broadcast", "recipients": clients}
}
