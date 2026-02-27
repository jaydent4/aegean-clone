package nodes

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"

	"aegean/common"
)

// Node is a base class that handles HTTP requests
type Node struct {
	Name   string
	Host   string
	Port   int
	server *http.Server
	// EnablePprof exposes net/http pprof handlers under /debug/pprof/.
	EnablePprof bool
	// BlockProfileRate controls runtime block profiling.
	// See runtime.SetBlockProfileRate.
	BlockProfileRate int
	// MutexProfileFraction controls runtime mutex profiling.
	// See runtime.SetMutexProfileFraction.
	MutexProfileFraction int

	// HandleMessage must be set by embedding types to process requests.
	HandleMessage common.MessageHandler
	// HandleProgress is mounted on /progress and mirrors HandleMessage semantics.
	HandleProgress common.MessageHandler
	// HandleReady is mounted on /ready and mirrors HandleMessage semantics.
	HandleReady common.MessageHandler
}

func NewNode(name, host string, port int) *Node {
	return &Node{
		Name: name,
		Host: host,
		Port: port,
	}
}

// Start the node and process HTTP requests
func (n *Node) Start() {
	if n.server != nil {
		return
	}

	if n.HandleMessage == nil {
		panic(fmt.Sprintf("Node %s: HandleMessage not set", n.Name))
	}

	addr := fmt.Sprintf("%s:%d", n.Host, n.Port)

	mux := http.NewServeMux()
	mux.Handle("/", common.MakeHandler(n.HandleMessage))
	if n.HandleProgress != nil {
		mux.Handle("/progress", common.MakeHandler(n.HandleProgress))
	}
	if n.HandleReady != nil {
		mux.Handle("/ready", common.MakeHandler(n.HandleReady))
	}
	if n.EnablePprof {
		if n.BlockProfileRate > 0 {
			runtime.SetBlockProfileRate(n.BlockProfileRate)
		}
		if n.MutexProfileFraction > 0 {
			runtime.SetMutexProfileFraction(n.MutexProfileFraction)
		}
		n.registerPprofHandlers(mux)
	}

	n.server = &http.Server{Addr: addr, Handler: mux}
	if err := n.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(fmt.Sprintf("Node %s failed: %v", n.Name, err))
	}
}

func (n *Node) registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}
