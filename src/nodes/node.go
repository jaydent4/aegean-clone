package nodes

import (
	"fmt"
	"log"
	"net/http"

	"aegean/common"
)

// Node is a base class that handles HTTP requests
type Node struct {
	Name   string
	Host   string
	Port   int
	server *http.Server

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
		log.Printf("Node %s already running", n.Name)
		return
	}

	if n.HandleMessage == nil {
		log.Fatalf("Node %s: HandleMessage not set", n.Name)
	}

	addr := fmt.Sprintf("%s:%d", n.Host, n.Port)
	log.Printf("Starting node %s on %s", n.Name, addr)

	mux := http.NewServeMux()
	mux.Handle("/", common.MakeHandler(n.HandleMessage))
	if n.HandleProgress != nil {
		mux.Handle("/progress", common.MakeHandler(n.HandleProgress))
	}
	if n.HandleReady != nil {
		mux.Handle("/ready", common.MakeHandler(n.HandleReady))
	}

	n.server = &http.Server{Addr: addr, Handler: mux}
	if err := n.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Node %s failed: %v", n.Name, err)
	}
}
