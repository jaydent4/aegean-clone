package nodes

import (
	"log"

	"aegean/common"
)

type Shim struct {
	Name         string
	NextCh       chan<- map[string]any
	Clients      []string
	quorumHelper *common.QuorumHelper
}

func NewShim(name string, nextCh chan<- map[string]any, clients []string) *Shim {
	if nextCh == nil {
		log.Fatalf("shim component requires non-nil nextCh")
	}
	shim := &Shim{
		Name:         name,
		NextCh:       nextCh,
		Clients:      clients,
		quorumHelper: common.NewQuorumHelper(2), // TODO: Replace hard-coded value with formula
	}
	return shim
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

		// Handle response from exec - broadcast to all clients that sent the request
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

	// Handle incoming client request - wait for quorum then forward
	requestID := payload["request_id"]
	sender, _ := payload["sender"].(string)

	if s.quorumHelper.Add(requestID, sender) {
		if s.NextCh != nil {
			s.NextCh <- payload
		} else {
			log.Printf("%s: Next channel not set; dropping request %v", s.Name, requestID)
		}
		return map[string]any{"status": "forwarded_to_mid_execs"}
	}
	return map[string]any{"status": "waiting_for_quorum"}
}
