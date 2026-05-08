package nodes

import (
	"aegean/aegean/unreplicated"
	netx "aegean/net"
	"aegean/telemetry"
)

// UnreplicatedServer is a direct baseline with no overlap with the replicated component pipeline.
type UnreplicatedServer struct {
	*Node
	Engine         *unreplicated.Engine
	Clients        []string
	ExecuteRequest unreplicated.WorkflowFunc
}

func NewUnreplicatedServer(name, host string, port int, clients []string, executeRequest unreplicated.WorkflowFunc, initStateFn unreplicated.InitStateFunc, runConfig map[string]any) *UnreplicatedServer {
	if executeRequest == nil {
		panic("unreplicated server requires ExecuteRequest")
	}
	server := &UnreplicatedServer{
		Node:           NewNode(name, host, port),
		Engine:         unreplicated.NewEngine(name, runConfig, initStateFn),
		Clients:        append([]string{}, clients...),
		ExecuteRequest: executeRequest,
	}
	server.Node.HandleMessage = server.HandleMessage
	server.Node.HandleReady = server.HandleReady
	return server
}

func (s *UnreplicatedServer) Start() {
	s.Node.Start()
}

func (s *UnreplicatedServer) HandleMessage(payload map[string]any) map[string]any {
	msgType, _ := payload["type"].(string)
	if msgType == "" || msgType == "request" {
		return s.handleRequest(payload)
	}

	switch msgType {
	case "response":
		if !s.Engine.BufferNestedResponse(payload) {
			return map[string]any{"status": "error", "error": "missing request_id"}
		}
		return map[string]any{"status": "forwarded_nested_response", "request_id": payload["request_id"]}
	default:
		return map[string]any{"status": "error", "error": "unknown message type"}
	}
}

func (s *UnreplicatedServer) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{"ready": true}
}

func (s *UnreplicatedServer) handleRequest(payload map[string]any) map[string]any {
	request := make(map[string]any, len(payload))
	for key, value := range payload {
		request[key] = value
	}

	go func() {
		ndSeed := int64(0)
		ndTimestamp := 0.0
		requestID := request["request_id"]

		defer func() {
			s.Engine.ClearRequestContext(requestID)
			s.Engine.ClearNestedResponses(requestID)
		}()

		for {
			response := s.ExecuteRequest(s.Engine, request, ndSeed, ndTimestamp)
			if status, _ := response["status"].(string); status == "blocked_for_nested_response" {
				s.Engine.WaitForNestedResponse(requestID)
				continue
			}
			_ = sendDirectResponse(s.Name, s.Clients, request, response)
			return
		}
	}()
	return map[string]any{
		"status":     "accepted",
		"request_id": request["request_id"],
		"sender":     s.Name,
	}
}

func sendDirectResponse(name string, clients []string, request map[string]any, response map[string]any) map[string]any {
	requestID := request["request_id"]
	fullResponse := map[string]any{
		"type":                   "response",
		"request_id":             requestID,
		"response":               response,
		"sender":                 name,
		"shim_quorum_aggregated": true,
	}
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		fullResponse["parent_request_id"] = parentRequestID
	}
	telemetry.CopyContext(fullResponse, request)

	for _, client := range clients {
		_, _ = netx.SendMessage(client, 8000, fullResponse)
	}

	return map[string]any{"status": "responded", "request_id": requestID}
}
