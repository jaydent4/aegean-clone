package nodes

import (
	"aegean/aegean/batcher"
	"aegean/aegean/exec"
	"aegean/aegean/mixer"
	"aegean/aegean/shim"
	"aegean/aegean/verifier"
	"aegean/common"
	"aegean/eo"
	netx "aegean/net"
	"math/rand/v2"
	"runtime"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	injectNetworkDelay = false
	injectChannelDelay = false
)

// Server combines shim, mixer, exec, and verifier into one node
type Server struct {
	*Node
	Shim             *shim.Shim
	Batcher          *batcher.Batcher
	Mixer            *mixer.Mixer
	Exec             *exec.Exec
	EO               *eo.EO
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

func NewServer(name, host string, port int, clients []string, nodes []string, isPrimaryBatcher bool, shimQuorumSize int, verifyResponseQuorumSize int, execVerifyQuorumSize int, phaseQuorumSize int, expectedExecVotes int, executeRequest exec.ExecuteRequestFunc, initStateFn exec.InitStateFunc, runConfig map[string]any) *Server {
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
	runtime.GOMAXPROCS(common.MustInt(runConfig, "gomaxprocs"))

	peers := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node != name {
			peers = append(peers, node)
		}
	}

	// Init each component
	server.Shim = shim.NewShim(name, shimToBatcher, shimToExec, clients, peers, isPrimaryBatcher, shimQuorumSize)
	server.Batcher = batcher.NewBatcher(name, batcherToMixer, nodes, isPrimaryBatcher, runConfig)
	server.Mixer = mixer.NewMixer(name, mixerToExec, runConfig)
	server.Exec = exec.NewExec(name, nodes, peers, execToVerifier, execToShim, verifyResponseQuorumSize, executeRequest, initStateFn, runConfig)
	server.Verifier = verifier.NewVerifier(name, nodes, nodes, verifierToExec, execVerifyQuorumSize, phaseQuorumSize, expectedExecVotes)
	if common.BoolOrDefault(runConfig, "nested_use_eo", false) {
		disableFollowerElections := common.BoolOrDefault(
			runConfig,
			"eo_disable_follower_elections",
			common.BoolOrDefault(runConfig, "raft_disable_follower_elections", false),
		)
		component, err := eo.NewEO(eo.Config{
			Name:  name,
			Peers: nodes,
			Commit: func(entry eo.CommittedEntry) {
				_ = server.Exec.BufferExactOnceNestedResponse(entry.Entry.Response)
			},
			SendRaft: func(peer string, message raftpb.Message) error {
				payload, err := eo.EncodeRaftMessage(message)
				if err != nil {
					return err
				}
				_, err = netx.SendMessage(peer, 8000, payload)
				return err
			},
			SendRaftBatch: func(peer string, messages []raftpb.Message) error {
				payload, err := eo.EncodeRaftMessages(messages)
				if err != nil {
					return err
				}
				_, err = netx.SendMessage(peer, 8000, payload)
				return err
			},
			TickInterval:             time.Duration(common.IntOrDefault(runConfig, "eo_tick_interval_ms", 10)) * time.Millisecond,
			ElectionTick:             common.IntOrDefault(runConfig, "eo_election_tick", 10),
			HeartbeatTick:            common.IntOrDefault(runConfig, "eo_heartbeat_tick", 1),
			DisableFollowerElections: disableFollowerElections,
			RaftSendBatchSize:        common.IntOrDefault(runConfig, "eo_raft_send_batch_size", 1),
		})
		if err != nil {
			panic(err)
		}
		server.EO = component
		if common.BoolOrDefault(runConfig, "use_eo_after_quorum", false) {
			server.Exec.SetNestedEO(exec.NewNestedEORequestQuorumGate(name, component))
		} else {
			server.Exec.SetNestedEO(component)
		}
	}

	server.Node.HandleMessage = server.HandleMessage
	server.Node.HandleReady = server.HandleReady
	return server
}

func (s *Server) Start() {
	if s.isPrimaryBatcher {
		s.Batcher.StartBatchFlusher()
	}

	// Wire component loops
	go func() {
		for msg := range s.shimToBatcher {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			s.Batcher.HandleRequestMessage(msg)
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.batcherToMixer {
			s.Mixer.HandleBatchMessage(msg)
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.mixerToExec {
			s.Exec.HandleBatchMessage(msg)
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.shimToExec {
			_ = s.Exec.BufferNestedResponse(msg)
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.execToVerifier {
			s.Verifier.HandleVerifyMessage(msg)
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.verifierToExec {
			s.Exec.HandleVerifyResponseMessage(msg)
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
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
		return s.Shim.HandleRequestMessage(payload)
	}

	if injectNetworkDelay {
		time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
	}
	switch msgType {
	case "response":
		if response, handled := s.Exec.HandleNestedResponseMessage(payload); handled {
			return response
		}
		return s.Shim.HandleIncomingResponse(payload)
	case eo.MessageTypeRaft:
		if s.EO == nil {
			return map[string]any{"status": "error", "error": "eo not configured"}
		}
		return s.EO.HandleRaftMessage(payload)
	case exec.MessageTypeEONestedRequest:
		return s.Exec.HandleNestedEORequestMessage(payload)
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

func (s *Server) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": s.Exec.NestedEOReady(),
	}
}
