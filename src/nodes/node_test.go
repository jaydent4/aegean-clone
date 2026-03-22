package nodes

import (
	"net"
	"testing"
	"time"

	netx "aegean/net"
)

func TestNewNode(t *testing.T) {
	node := NewNode("test", "localhost", 8080)

	if node.Name != "test" {
		t.Errorf("expected Name=test, got %s", node.Name)
	}
	if node.Host != "localhost" {
		t.Errorf("expected Host=localhost, got %s", node.Host)
	}
	if node.Port != 8080 {
		t.Errorf("expected Port=8080, got %d", node.Port)
	}
	if node.server != nil {
		t.Error("expected server to be nil initially")
	}
}

func TestNodeStart(t *testing.T) {
	// Some available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	node := NewNode("test", "127.0.0.1", port)
	node.HandleMessage = func(req map[string]any) map[string]any {
		return map[string]any{"echo": req["msg"]}
	}

	// Start server in background
	go node.Start()
	time.Sleep(50 * time.Millisecond)

	// Send a request
	result, err := netx.SendMessage("127.0.0.1", port, map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	if result["echo"] != "hello" {
		t.Errorf("expected echo=hello, got %v", result["echo"])
	}
}
