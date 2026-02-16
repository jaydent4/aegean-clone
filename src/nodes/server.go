package nodes

import (
	"aegean/components/batcher"
	"aegean/components/exec"
	"aegean/components/mixer"
	"aegean/components/shim"
	"aegean/components/verifier"
	"math/rand/v2"
	"time"
)

// Server combines shim, mixer, exec, and verifier into one node
type Server struct {
	*Node
	Shim             *shim.Shim
	Batcher          *batcher.Batcher
	Mixer            *mixer.Mixer
	Exec             *exec.Exec
	Verifier         *verifier.Verifier
	isPrimaryBatcher bool

	// Internal message buses between components
	shimToBatcher  chan map[string]any
	batcherToMixer chan map[string]any
	mixerToExec    chan map[string]any
	shimToExec     chan map[string]any
	execToVerifier chan map[string]any
	verifierToExec chan map[string]any
	execToShim     chan map[string]any
}

func NewServer(name, host string, port int, clients []string, nodes []string, isPrimaryBatcher bool, shimQuorumSize int, verifyResponseQuorumSize int, execVerifyQuorumSize int, phaseQuorumSize int, expectedExecVotes int, executeRequest exec.ExecuteRequestFunc) *Server {
	// Buffered channels to decouple component work
	shimToBatcher := make(chan map[string]any, 256)
	batcherToMixer := make(chan map[string]any, 256)
	mixerToExec := make(chan map[string]any, 256)
	shimToExec := make(chan map[string]any, 256)
	execToVerifier := make(chan map[string]any, 256)
	verifierToExec := make(chan map[string]any, 256)
	execToShim := make(chan map[string]any, 256)

	server := &Server{
		Node:             NewNode(name, host, port),
		shimToBatcher:    shimToBatcher,
		batcherToMixer:   batcherToMixer,
		mixerToExec:      mixerToExec,
		shimToExec:       shimToExec,
		execToVerifier:   execToVerifier,
		verifierToExec:   verifierToExec,
		execToShim:       execToShim,
		isPrimaryBatcher: isPrimaryBatcher,
	}

	peers := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node != name {
			peers = append(peers, node)
		}
	}

	// Init each component
	server.Shim = shim.NewShim(name, shimToBatcher, shimToExec, clients, shimQuorumSize)
	server.Batcher = batcher.NewBatcher(name, batcherToMixer, nodes, isPrimaryBatcher)
	server.Mixer = mixer.NewMixer(name, mixerToExec)
	server.Exec = exec.NewExec(name, nodes, peers, execToVerifier, execToShim, verifyResponseQuorumSize, executeRequest)
	server.Verifier = verifier.NewVerifier(name, nodes, nodes, verifierToExec, execVerifyQuorumSize, phaseQuorumSize, expectedExecVotes)

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
		for msg := range s.shimToExec {
			_ = s.Exec.BufferNestedResponse(msg)
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
			s.Shim.HandleOutgoingResponse(msg)
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

	time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
	switch msgType {
	case "response":
		return s.Shim.HandleIncomingResponse(payload)
	case "batch":
		return s.Mixer.HandleBatchMessage(payload)
	case "verify":
		return s.Verifier.HandleVerifyMessage(payload)
	case "prepare":
		return s.Verifier.HandlePrepareMessage(payload)
	case "commit":
		return s.Verifier.HandleCommitMessage(payload)
	case "view_change":
		return s.Verifier.HandleViewChangeMessage(payload)
	case "new_view":
		return s.Verifier.HandleNewViewMessage(payload)
	case "verify_response":
		return s.Exec.HandleVerifyResponseMessage(payload)
	case "state_transfer_request":
		return s.Exec.HandleStateTransferRequestMessage(payload)
	default:
		return map[string]any{"status": "error", "error": "unknown message type"}
	}
}
