package nodes

import (
	"fmt"
)

// Server combines shim, mixer, exec, and verifier into one node
type Server struct {
	*Node
	Shim     *Shim
	Mixer    *Mixer
	Exec     *Exec
	Verifier *Verifier

	// Internal message buses between components
	shimToMixer    chan map[string]any
	mixerToExec    chan map[string]any
	execToVerifier chan map[string]any
	verifierToExec chan map[string]any
	execToShim     chan map[string]any
}

func NewServer(name, host string, port int, clients []string, verifiers []string, peers []string, execs []string) *Server {
	// Buffered channels to decouple component work
	shimToMixer := make(chan map[string]any, 256)
	mixerToExec := make(chan map[string]any, 256)
	execToVerifier := make(chan map[string]any, 256)
	verifierToExec := make(chan map[string]any, 256)
	execToShim := make(chan map[string]any, 256)

	server := &Server{
		Node:           NewNode(name, host, port),
		shimToMixer:    shimToMixer,
		mixerToExec:    mixerToExec,
		execToVerifier: execToVerifier,
		verifierToExec: verifierToExec,
		execToShim:     execToShim,
	}

	// Init each component
	server.Shim = NewShim(fmt.Sprintf("%s/shim", name), shimToMixer, clients)
	server.Mixer = NewMixer(fmt.Sprintf("%s/mixer", name), mixerToExec)
	server.Exec = NewExec(fmt.Sprintf("%s/exec", name), verifiers, peers, name, execToVerifier, execToShim)
	server.Exec.ExecID = name
	server.Verifier = NewVerifier(fmt.Sprintf("%s/verifier", name), execs, name, verifierToExec)

	server.Node.HandleMessage = server.HandleMessage
	return server
}

func (s *Server) Start() {
	// TODO: deprecate this with batcher
	// Mixer uses timer-driven flusher
	s.Mixer.StartBatchFlusher()

	// Wire component loops
	go func() {
		for msg := range s.shimToMixer {
			s.Mixer.HandleRequestMessage(msg)
		}
	}()

	go func() {
		for msg := range s.mixerToExec {
			s.Exec.HandleBatchMessage(msg)
		}
	}()

	go func() {
		for msg := range s.execToVerifier {
			s.Verifier.HandleVerifyMessage(msg)
		}
	}()

	go func() {
		for msg := range s.verifierToExec {
			s.Exec.HandleVerifyResponseMessage(msg)
		}
	}()

	go func() {
		for msg := range s.execToShim {
			s.Shim.HandleResponseMessage(msg)
		}
	}()

	s.Node.Start()
}

func (s *Server) HandleMessage(payload map[string]any) map[string]any {
	// Route by message type to the correct component
	msgType, _ := payload["type"].(string)
	if msgType == "" || msgType == "request" {
		return s.Shim.HandleRequestMessage(payload)
	}

	switch msgType {
	case "verify":
		return s.Verifier.HandleVerifyMessage(payload)
	case "verify_response":
		return s.Exec.HandleVerifyResponseMessage(payload)
	case "state_transfer_request":
		return s.Exec.HandleStateTransferRequestMessage(payload)
	default:
		return map[string]any{"status": "error", "error": "unknown message type"}
	}
}
