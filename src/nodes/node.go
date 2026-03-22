package nodes

import (
	"fmt"
	"net"

	netx "aegean/net"
)

// Node is a base class that handles HTTP requests
type Node struct {
	Name   string
	Host   string
	Port   int
	server net.Listener

	// HandleMessage must be set by embedding types to process requests.
	HandleMessage netx.MessageHandler
	// HandleReady is mounted on /ready and mirrors HandleMessage semantics.
	HandleReady netx.MessageHandler
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
	handlers := map[string]netx.MessageHandler{
		"/": n.HandleMessage,
	}
	if n.HandleReady != nil {
		handlers["/ready"] = n.HandleReady
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("Node %s failed to listen: %v", n.Name, err))
	}
	n.server = listener

	if err := netx.ServeTCP(listener, handlers); err != nil {
		panic(fmt.Sprintf("Node %s failed: %v", n.Name, err))
	}
}
