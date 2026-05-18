package nodes

import (
	"testing"
	"time"
)

func TestClientStatusHandlerReflectsRequestLogicState(t *testing.T) {
	client := NewClient(
		"client",
		"127.0.0.1",
		8000,
		nil,
		nil,
		map[string]any{},
		"test_client",
		func(c *Client) {},
	)

	if client.Node.HandleStatus == nil {
		t.Fatal("expected /status handler to be registered")
	}

	status := client.HandleStatus(map[string]any{})
	if started, _ := status["request_logic_started"].(bool); started {
		t.Fatalf("expected request_logic_started=false, got %v", status)
	}
	if completed, _ := status["request_logic_completed"].(bool); completed {
		t.Fatalf("expected request_logic_completed=false, got %v", status)
	}

	client.requestLogicStarted.Store(true)
	client.requestLogicCompleted.Store(true)

	status = client.HandleStatus(map[string]any{})
	if started, _ := status["request_logic_started"].(bool); !started {
		t.Fatalf("expected request_logic_started=true, got %v", status)
	}
	if completed, _ := status["request_logic_completed"].(bool); !completed {
		t.Fatalf("expected request_logic_completed=true, got %v", status)
	}
}

func TestClientDrainPendingRequestsReturnsAfterPendingClears(t *testing.T) {
	client := NewClient(
		"client",
		"127.0.0.1",
		8000,
		nil,
		nil,
		map[string]any{},
		"test_client",
		func(c *Client) {},
	)
	client.pending["1"] = make(chan map[string]any, 1)

	done := make(chan error, 1)
	go func() {
		done <- client.DrainPendingRequests(time.Second)
	}()

	time.Sleep(20 * time.Millisecond)
	client.mu.Lock()
	delete(client.pending, "1")
	client.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected drain to succeed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pending drain")
	}
}

func TestClientDrainPendingRequestsTimesOut(t *testing.T) {
	client := NewClient(
		"client",
		"127.0.0.1",
		8000,
		nil,
		nil,
		map[string]any{},
		"test_client",
		func(c *Client) {},
	)
	client.pending["1"] = make(chan map[string]any, 1)

	if err := client.DrainPendingRequests(10 * time.Millisecond); err == nil {
		t.Fatal("expected pending drain timeout")
	}
}
