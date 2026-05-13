package nodes

import (
	optbatcher "aegean/aegean_opt/batcher"
	optexec "aegean/aegean_opt/exec"
	optmixer "aegean/aegean_opt/mixer"
	"aegean/aegean_opt/protocol"
	optshim "aegean/aegean_opt/shim"
	optverifier "aegean/aegean_opt/verifier"
	"aegean/common"
	"aegean/eo"
	netx "aegean/net"
	"math/rand/v2"
	"runtime"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

// OptServer wires the experimental aegean_opt component tree without changing
// the original Aegean server path.
type OptServer struct {
	*Node
	Shim             *optshim.Shim
	Batcher          *optbatcher.Batcher
	Mixer            *optmixer.Mixer
	Exec             *optexec.Exec
	EO               *eo.EO
	Verifier         *optverifier.Verifier
	isPrimaryBatcher bool

	shimToBatcher  chan map[string]any
	batcherToMixer chan map[string]any
	mixerToExec    chan map[string]any
	shimToExec     chan map[string]any
	execToVerifier chan map[string]any
	verifierToExec chan map[string]any
	execToShim     chan map[string]any
}

func NewOptServer(name, host string, port int, clients []string, nodes []string, isPrimaryBatcher bool, shimQuorumSize int, verifyResponseQuorumSize int, execVerifyQuorumSize int, phaseQuorumSize int, expectedExecVotes int, executeRequest optexec.ExecuteRequestFunc, initStateFn optexec.InitStateFunc, runConfig map[string]any) *OptServer {
	shimToBatcher := make(chan map[string]any, 256)
	batcherToMixer := make(chan map[string]any, 256)
	mixerToExec := make(chan map[string]any, 256)
	shimToExec := make(chan map[string]any, 256)
	execToVerifier := make(chan map[string]any, 256)
	verifierToExec := make(chan map[string]any, 256)
	execToShim := make(chan map[string]any, 256)

	server := &OptServer{
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

	server.Shim = optshim.NewShim(name, shimToBatcher, shimToExec, clients, peers, isPrimaryBatcher, shimQuorumSize)
	server.Batcher = optbatcher.NewBatcher(name, batcherToMixer, nodes, isPrimaryBatcher, runConfig)
	server.Mixer = optmixer.NewMixer(name, mixerToExec, runConfig)
	server.Exec = optexec.NewExec(name, nodes, peers, execToVerifier, execToShim, verifyResponseQuorumSize, executeRequest, initStateFn, runConfig)
	server.Verifier = optverifier.NewVerifier(name, nodes, nodes, verifierToExec, execVerifyQuorumSize, phaseQuorumSize, expectedExecVotes)
	if common.BoolOrDefault(runConfig, "nested_use_eo", false) {
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
			TickInterval:  time.Duration(common.IntOrDefault(runConfig, "eo_tick_interval_ms", 10)) * time.Millisecond,
			ElectionTick:  common.IntOrDefault(runConfig, "eo_election_tick", 10),
			HeartbeatTick: common.IntOrDefault(runConfig, "eo_heartbeat_tick", 1),
		})
		if err != nil {
			panic(err)
		}
		server.EO = component
		if common.BoolOrDefault(runConfig, "use_eo_after_quorum", false) {
			server.Exec.SetNestedEO(optexec.NewNestedEORequestQuorumGate(name, component))
		} else {
			server.Exec.SetNestedEO(component)
		}
	}

	server.Node.HandleMessage = server.HandleMessage
	server.Node.HandleReady = server.HandleReady
	return server
}

func (s *OptServer) Start() {
	if s.isPrimaryBatcher {
		s.Batcher.StartBatchFlusher()
	}

	go func() {
		for msg := range s.shimToBatcher {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			s.Batcher.HandleRequestMessage(msg)
		}
	}()

	go func() {
		for msg := range s.batcherToMixer {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			s.handleOptMixerInput(msg)
		}
	}()

	go func() {
		for msg := range s.mixerToExec {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			s.handleOptExecInput(msg)
		}
	}()

	go func() {
		for msg := range s.shimToExec {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			_ = s.Exec.BufferNestedResponse(msg)
		}
	}()

	go func() {
		for msg := range s.execToVerifier {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			s.Verifier.HandleVerifyMessage(msg)
		}
	}()

	go func() {
		for msg := range s.verifierToExec {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			s.Exec.HandleVerifyResponseMessage(msg)
		}
	}()

	go func() {
		for msg := range s.execToShim {
			if injectChannelDelay {
				time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
			}
			s.Shim.HandleOutgoingResponse(msg)
		}
	}()

	s.Node.Start()
}

func (s *OptServer) handleOptMixerInput(msg map[string]any) {
	switch msg[protocol.FieldType] {
	case protocol.MessageTypeInLogRequest:
		s.Mixer.HandleInLogRequestMessage(msg)
	case protocol.MessageTypeInLogBatchFormed:
		s.Mixer.HandleInLogBatchFormedMessage(msg)
	default:
		s.Mixer.HandleBatchMessage(msg)
	}
}

func (s *OptServer) handleOptExecInput(msg map[string]any) {
	switch msg[protocol.FieldType] {
	case protocol.MessageTypeScheduledRequest:
		s.Exec.HandleScheduledRequestMessage(msg)
	case protocol.MessageTypeVerificationWindowClosed:
		s.Exec.HandleVerificationWindowClosedMessage(msg)
	default:
		s.Exec.HandleBatchMessage(msg)
	}
}

func (s *OptServer) HandleMessage(payload map[string]any) map[string]any {
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
	case protocol.MessageTypeInLogRaft:
		return s.Batcher.HandleInLogRaftMessage(payload)
	case protocol.MessageTypeEONestedRequest:
		return s.Exec.HandleNestedEORequestMessage(payload)
	case "batch":
		return s.Mixer.HandleBatchMessage(payload)
	case protocol.MessageTypeInLogRequest:
		return s.Mixer.HandleInLogRequestMessage(payload)
	case protocol.MessageTypeInLogBatchFormed:
		return s.Mixer.HandleInLogBatchFormedMessage(payload)
	case protocol.MessageTypeScheduledRequest:
		return s.Exec.HandleScheduledRequestMessage(payload)
	case protocol.MessageTypeVerificationWindowClosed:
		return s.Exec.HandleVerificationWindowClosedMessage(payload)
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

func (s *OptServer) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": s.Exec.NestedEOReady() && s.Batcher.InLogReady(),
	}
}
