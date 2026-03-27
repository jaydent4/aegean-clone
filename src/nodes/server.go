package nodes

import (
	"aegean/common"
	"aegean/components/batcher"
	"aegean/components/exec"
	"aegean/components/mixer"
	"aegean/components/shim"
	"aegean/components/verifier"
	"aegean/telemetry"
	"math/rand/v2"
	"runtime"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	server.Mixer = mixer.NewMixer(name, mixerToExec)
	server.Exec = exec.NewExec(name, nodes, peers, execToVerifier, execToShim, verifyResponseQuorumSize, executeRequest, initStateFn, runConfig)
	server.Verifier = verifier.NewVerifier(name, nodes, nodes, verifierToExec, execVerifyQuorumSize, phaseQuorumSize, expectedExecVotes)

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
			doWithPayloadSpan(msg, "server.shim_to_batcher", func() struct{} {
				s.Batcher.HandleRequestMessage(msg)
				return struct{}{}
			}, attribute.String("node.name", s.Name), attribute.String("component", "batcher"), attribute.String("path", "shim_to_batcher"))
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.batcherToMixer {
			doWithPayloadSpan(msg, "server.batcher_to_mixer", func() struct{} {
				s.Mixer.HandleBatchMessage(msg)
				return struct{}{}
			}, attribute.String("node.name", s.Name), attribute.String("component", "mixer"), attribute.String("path", "batcher_to_mixer"))
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.mixerToExec {
			doWithPayloadSpan(msg, "server.mixer_to_exec", func() struct{} {
				s.Exec.HandleBatchMessage(msg)
				return struct{}{}
			}, attribute.String("node.name", s.Name), attribute.String("component", "exec"), attribute.String("path", "mixer_to_exec"))
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.shimToExec {
			doWithPayloadSpan(msg, "server.shim_to_exec", func() struct{} {
				_ = s.Exec.BufferNestedResponse(msg)
				return struct{}{}
			}, attribute.String("node.name", s.Name), attribute.String("component", "exec"), attribute.String("path", "shim_to_exec"))
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.execToVerifier {
			doWithPayloadSpan(msg, "server.exec_to_verifier", func() struct{} {
				s.Verifier.HandleVerifyMessage(msg)
				return struct{}{}
			}, attribute.String("node.name", s.Name), attribute.String("component", "verifier"), attribute.String("path", "exec_to_verifier"))
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.verifierToExec {
			doWithPayloadSpan(msg, "server.verifier_to_exec", func() struct{} {
				s.Exec.HandleVerifyResponseMessage(msg)
				return struct{}{}
			}, attribute.String("node.name", s.Name), attribute.String("component", "exec"), attribute.String("path", "verifier_to_exec"))
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.execToShim {
			if waitSpanAny, ok := msg[exec.ResponseEmitWaitSpanKey()]; ok {
				if waitSpan, ok := waitSpanAny.(trace.Span); ok && waitSpan != nil {
					waitSpan.End()
				}
				delete(msg, exec.ResponseEmitWaitSpanKey())
			}
			doWithPayloadSpan(msg, "server.exec_to_shim", func() struct{} {
				s.Shim.HandleOutgoingResponse(msg)
				return struct{}{}
			}, attribute.String("node.name", s.Name), attribute.String("component", "shim"), attribute.String("path", "exec_to_shim"))
		}
	}()

	s.Node.Start()
}

func (s *Server) HandleMessage(payload map[string]any) map[string]any {
	// Route by message type to the correct component
	msgType, _ := payload["type"].(string)
	if msgType == "" || msgType == "request" {
		return doWithPayloadSpan(payload, "server.handle_request", func() map[string]any {
			return s.Shim.HandleRequestMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "shim"), attribute.String("msg_type", "request"))
	}

	if injectNetworkDelay {
		time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
	}
	switch msgType {
	case "response":
		return doWithPayloadSpan(payload, "server.handle_response", func() map[string]any {
			return s.Shim.HandleIncomingResponse(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "shim"), attribute.String("msg_type", "response"))
	case "batch":
		return doWithPayloadSpan(payload, "server.handle_batch", func() map[string]any {
			return s.Mixer.HandleBatchMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "mixer"), attribute.String("msg_type", "batch"))
	case "verify":
		return doWithPayloadSpan(payload, "server.handle_verify", func() map[string]any {
			return s.Verifier.HandleVerifyMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "verifier"), attribute.String("msg_type", "verify"))
	case "prepare":
		return doWithPayloadSpan(payload, "server.handle_prepare", func() map[string]any {
			return s.Verifier.HandlePrepareMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "verifier"), attribute.String("msg_type", "prepare"))
	case "commit":
		return doWithPayloadSpan(payload, "server.handle_commit", func() map[string]any {
			return s.Verifier.HandleCommitMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "verifier"), attribute.String("msg_type", "commit"))
	case "view_change":
		return doWithPayloadSpan(payload, "server.handle_view_change", func() map[string]any {
			return s.Verifier.HandleViewChangeMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "verifier"), attribute.String("msg_type", "view_change"))
	case "new_view":
		return doWithPayloadSpan(payload, "server.handle_new_view", func() map[string]any {
			return s.Verifier.HandleNewViewMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "verifier"), attribute.String("msg_type", "new_view"))
	case "verify_response":
		return doWithPayloadSpan(payload, "server.handle_verify_response", func() map[string]any {
			return s.Exec.HandleVerifyResponseMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "exec"), attribute.String("msg_type", "verify_response"))
	case "state_transfer_request":
		return doWithPayloadSpan(payload, "server.handle_state_transfer_request", func() map[string]any {
			return s.Exec.HandleStateTransferRequestMessage(payload)
		}, attribute.String("node.name", s.Name), attribute.String("component", "exec"), attribute.String("msg_type", "state_transfer_request"))
	default:
		return map[string]any{"status": "error", "error": "unknown message type"}
	}
}

func (s *Server) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}

func doWithPayloadSpan[T any](payload map[string]any, name string, fn func() T, attrs ...attribute.KeyValue) T {
	attrs = append(attrs, telemetry.AttrsFromPayload(payload)...)
	_, span := telemetry.StartLocalSpanFromPayload(payload, name, attrs...)
	defer span.End()
	return fn()
}
