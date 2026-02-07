package nodes

import (
	"fmt"

	"aegean/components/exec"
	"aegean/components/verifier"
)

// Server combines shim, mixer, exec, and verifier into one node
type Server struct {
	*Node
	Shim             *Shim
	Batcher          *Batcher
	Mixer            *Mixer
	Exec             *exec.Exec
	Verifier         *verifier.Verifier
	isPrimaryBatcher bool

	// Internal message buses between components
	shimToBatcher  chan map[string]any
	batcherToMixer chan map[string]any
	mixerToExec    chan map[string]any
	execToVerifier chan map[string]any
	verifierToExec chan map[string]any
	execToShim     chan map[string]any
}

func NewServer(name, host string, port int, clients []string, verifiers []string, peers []string, execs []string, isPrimaryBatcher bool, executeRequest exec.ExecuteRequestFunc, responseHandler exec.ExecuteResponseFunc) *Server {
	// Buffered channels to decouple component work
	shimToBatcher := make(chan map[string]any, 256)
	batcherToMixer := make(chan map[string]any, 256)
	mixerToExec := make(chan map[string]any, 256)
	execToVerifier := make(chan map[string]any, 256)
	verifierToExec := make(chan map[string]any, 256)
	execToShim := make(chan map[string]any, 256)

	server := &Server{
		Node:             NewNode(name, host, port),
		shimToBatcher:    shimToBatcher,
		batcherToMixer:   batcherToMixer,
		mixerToExec:      mixerToExec,
		execToVerifier:   execToVerifier,
		verifierToExec:   verifierToExec,
		execToShim:       execToShim,
		isPrimaryBatcher: isPrimaryBatcher,
	}

	// Init each component
	server.Shim = NewShim(fmt.Sprintf("%s/shim", name), shimToBatcher, clients)
	server.Batcher = NewBatcher(fmt.Sprintf("%s/batcher", name), batcherToMixer, execs, name, isPrimaryBatcher)
	server.Mixer = NewMixer(fmt.Sprintf("%s/mixer", name), mixerToExec)
	server.Exec = exec.NewExec(fmt.Sprintf("%s/exec", name), verifiers, peers, name, execToVerifier, execToShim, executeRequest, responseHandler)
	server.Exec.ExecID = name
	server.Verifier = verifier.NewVerifier(fmt.Sprintf("%s/verifier", name), execs, name, verifierToExec)

	server.Node.HandleMessage = server.HandleMessage
	return server
}

func (s *Server) Start() {
	if s.isPrimaryBatcher {
		s.Batcher.StartBatchFlusher()
	}

	// Wire component loops
	go func() {
		for msg := range s.shimToBatcher {
			s.Batcher.HandleRequestMessage(msg)
		}
	}()

	go func() {
		for msg := range s.batcherToMixer {
			s.Mixer.HandleBatchMessage(msg)
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
		if !s.isPrimaryBatcher {
			return map[string]any{"status": "ignored_non_primary"}
		}
		return s.Shim.HandleRequestMessage(payload)
	}

	switch msgType {
	case "response":
		return s.Exec.HandleResponse(s.Exec, payload)
	case "batch":
		return s.Mixer.HandleBatchMessage(payload)
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
