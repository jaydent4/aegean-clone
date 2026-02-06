package nodes

import (
	"fmt"
)

// Server combines shim, mixer, exec, and verifier into one node.
type Server struct {
	*Node
	Shim     *Shim
	Mixer    *Mixer
	Exec     *Exec
	Verifier *Verifier

	shimToMixer     chan map[string]any
	mixerToExec     chan map[string]any
	execToVerifier  chan map[string]any
	verifierToExec  chan map[string]any
	execToShim      chan map[string]any
}

func NewServer(name, host string, port int, clients []string, verifiers []string, peers []string, execs []string) *Server {
	shimToMixer := make(chan map[string]any, 256)
	mixerToExec := make(chan map[string]any, 256)
	execToVerifier := make(chan map[string]any, 256)
	verifierToExec := make(chan map[string]any, 256)
	execToShim := make(chan map[string]any, 256)

	server := &Server{
		Node:            NewNode(name, host, port),
		shimToMixer:     shimToMixer,
		mixerToExec:     mixerToExec,
		execToVerifier:  execToVerifier,
		verifierToExec:  verifierToExec,
		execToShim:      execToShim,
	}

	server.Shim = NewShim(fmt.Sprintf("%s/shim", name), shimToMixer, clients)
	server.Mixer = NewMixer(fmt.Sprintf("%s/mixer", name), mixerToExec)
	server.Exec = NewExec(fmt.Sprintf("%s/exec", name), verifiers, peers, name, execToVerifier, execToShim)
	server.Exec.ExecID = name
	server.Verifier = NewVerifier(fmt.Sprintf("%s/verifier", name), execs, name, verifierToExec)

	server.Node.HandleMessage = server.HandleMessage
	return server
}

func (s *Server) Start() {
	s.Mixer.StartBatchFlusher()

	go func() {
		for msg := range s.shimToMixer {
			s.Mixer.HandleMessage(msg)
		}
	}()

	go func() {
		for msg := range s.mixerToExec {
			s.Exec.HandleMessage(msg)
		}
	}()

	go func() {
		for msg := range s.execToVerifier {
			s.Verifier.HandleMessage(msg)
		}
	}()

	go func() {
		for msg := range s.verifierToExec {
			s.Exec.HandleMessage(msg)
		}
	}()

	go func() {
		for msg := range s.execToShim {
			s.Shim.HandleMessage(msg)
		}
	}()

	s.Node.Start()
}

func (s *Server) HandleMessage(payload map[string]any) map[string]any {
	msgType, _ := payload["type"].(string)
	if msgType == "" || msgType == "request" {
		return s.Shim.HandleMessage(payload)
	}

	switch msgType {
	case "verify":
		return s.Verifier.HandleMessage(payload)
	case "verify_response":
		return s.Exec.HandleMessage(payload)
	case "state_transfer_request":
		return s.Exec.HandleMessage(payload)
	case "batch":
		return s.Exec.HandleMessage(payload)
	case "response":
		return s.Shim.HandleMessage(payload)
	default:
		return map[string]any{"status": "error", "error": "unknown message type"}
	}
}
