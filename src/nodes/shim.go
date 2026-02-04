// Package nodes contains node implementations.
// Translates: src_py/nodes/shim.py
package nodes

import (
	"log"

	"aegean/common"
)

type Shim struct {
	*Node
	Next         string
	Clients      []string
	quorumHelper *common.QuorumHelper
}

func NewShim(name, host string, port int, next string, clients []string) *Shim {
	shim := &Shim{
		Node:         NewNode(name, host, port),
		Next:         next,
		Clients:      clients,
		quorumHelper: common.NewQuorumHelper(2), // TODO: Replace hard-coded value with formula
	}
	shim.Node.HandleMessage = shim.HandleMessage
	return shim
}

func (s *Shim) Start() {
	s.Node.Start()
}

func (s *Shim) HandleMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", s.Name, payload)

	msgType, _ := payload["type"].(string)
	if msgType == "" {
		msgType = "request"
	}

	if msgType == "response" {
		requestID := payload["request_id"]
		responseData, _ := payload["response"].(map[string]any)

		// TODO: Or do we wait for a quorum, and then broadcast
		log.Printf("%s: Broadcasting response for request %v to %d clients: %v", s.Name, requestID, len(s.Clients), s.Clients)

		for _, client := range s.Clients {
			clientResponse := map[string]any{
				"type":       "response",
				"request_id": requestID,
				"response":   responseData,
			}
			if _, err := common.SendMessage(client, 8000, clientResponse); err != nil {
				log.Printf("%s: Failed to send response to %s: %v", s.Name, client, err)
				continue
			}
			log.Printf("%s: Sent response to client %s", s.Name, client)
		}

		return map[string]any{"status": "response_broadcast", "recipients": s.Clients}
	}

	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)

	if s.quorumHelper.Add(requestID, sender) {
		_, _ = common.SendMessage(s.Next, 8000, payload)
		return map[string]any{"status": "forwarded_to_mid_execs"}
	}
	return map[string]any{"status": "waiting_for_quorum"}
}
