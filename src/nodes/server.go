package nodes

import (
	"aegean/common"
	"aegean/components/batcher"
	"aegean/components/exec"
	"aegean/components/mixer"
	"aegean/components/shim"
	"aegean/components/verifier"
	"context"
	"math/rand/v2"
	"runtime"
	"runtime/pprof"
	"time"
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
	server.Node.EnablePprof = common.BoolOrDefault(runConfig, "pprof_enabled", true)
	server.Node.BlockProfileRate = common.IntOrDefault(runConfig, "pprof_block_profile_rate", 1)
	server.Node.MutexProfileFraction = common.IntOrDefault(runConfig, "pprof_mutex_profile_fraction", 1)
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
			doWithPprofLabels(func() struct{} {
				s.Batcher.HandleRequestMessage(msg)
				return struct{}{}
			}, "node", s.Name, "component", "batcher", "path", "shim_to_batcher")
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.batcherToMixer {
			doWithPprofLabels(func() struct{} {
				s.Mixer.HandleBatchMessage(msg)
				return struct{}{}
			}, "node", s.Name, "component", "mixer", "path", "batcher_to_mixer")
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.mixerToExec {
			doWithPprofLabels(func() struct{} {
				s.Exec.HandleBatchMessage(msg)
				return struct{}{}
			}, "node", s.Name, "component", "exec", "path", "mixer_to_exec")
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.shimToExec {
			doWithPprofLabels(func() struct{} {
				_ = s.Exec.BufferNestedResponse(msg)
				return struct{}{}
			}, "node", s.Name, "component", "exec", "path", "shim_to_exec")
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.execToVerifier {
			doWithPprofLabels(func() struct{} {
				s.Verifier.HandleVerifyMessage(msg)
				return struct{}{}
			}, "node", s.Name, "component", "verifier", "path", "exec_to_verifier")
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.verifierToExec {
			doWithPprofLabels(func() struct{} {
				s.Exec.HandleVerifyResponseMessage(msg)
				return struct{}{}
			}, "node", s.Name, "component", "exec", "path", "verifier_to_exec")
		}
	}()

	go func() {
		if injectChannelDelay {
			time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
		}
		for msg := range s.execToShim {
			doWithPprofLabels(func() struct{} {
				s.Shim.HandleOutgoingResponse(msg)
				return struct{}{}
			}, "node", s.Name, "component", "shim", "path", "exec_to_shim")
		}
	}()

	s.Node.Start()
}

func (s *Server) HandleMessage(payload map[string]any) map[string]any {
	// Route by message type to the correct component
	msgType, _ := payload["type"].(string)
	if msgType == "" || msgType == "request" {
		return doWithPprofLabels(func() map[string]any {
			return s.Shim.HandleRequestMessage(payload)
		}, "node", s.Name, "component", "shim", "msg_type", "request")
	}

	if injectNetworkDelay {
		time.Sleep(time.Duration(rand.Float64() * 0.01 * float64(time.Second)))
	}
	switch msgType {
	case "response":
		return doWithPprofLabels(func() map[string]any {
			return s.Shim.HandleIncomingResponse(payload)
		}, "node", s.Name, "component", "shim", "msg_type", "response")
	case "batch":
		return doWithPprofLabels(func() map[string]any {
			return s.Mixer.HandleBatchMessage(payload)
		}, "node", s.Name, "component", "mixer", "msg_type", "batch")
	case "verify":
		return doWithPprofLabels(func() map[string]any {
			return s.Verifier.HandleVerifyMessage(payload)
		}, "node", s.Name, "component", "verifier", "msg_type", "verify")
	case "prepare":
		return doWithPprofLabels(func() map[string]any {
			return s.Verifier.HandlePrepareMessage(payload)
		}, "node", s.Name, "component", "verifier", "msg_type", "prepare")
	case "commit":
		return doWithPprofLabels(func() map[string]any {
			return s.Verifier.HandleCommitMessage(payload)
		}, "node", s.Name, "component", "verifier", "msg_type", "commit")
	case "view_change":
		return doWithPprofLabels(func() map[string]any {
			return s.Verifier.HandleViewChangeMessage(payload)
		}, "node", s.Name, "component", "verifier", "msg_type", "view_change")
	case "new_view":
		return doWithPprofLabels(func() map[string]any {
			return s.Verifier.HandleNewViewMessage(payload)
		}, "node", s.Name, "component", "verifier", "msg_type", "new_view")
	case "verify_response":
		return doWithPprofLabels(func() map[string]any {
			return s.Exec.HandleVerifyResponseMessage(payload)
		}, "node", s.Name, "component", "exec", "msg_type", "verify_response")
	case "state_transfer_request":
		return doWithPprofLabels(func() map[string]any {
			return s.Exec.HandleStateTransferRequestMessage(payload)
		}, "node", s.Name, "component", "exec", "msg_type", "state_transfer_request")
	default:
		return map[string]any{"status": "error", "error": "unknown message type"}
	}
}

func (s *Server) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}

func doWithPprofLabels[T any](fn func() T, labels ...string) T {
	var result T
	pprof.Do(context.Background(), pprof.Labels(labels...), func(context.Context) {
		result = fn()
	})
	return result
}
