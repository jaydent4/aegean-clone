package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// SendMessage sends a HTTP POST request to the specified host and port with retry logic
func SendMessage(host string, port int, payload any, opts ...any) (map[string]any, error) {
	return SendMessageToPath(host, port, "/", payload, opts...)
}

// SendMessageToPath sends an HTTP POST request to the specified host, port, and path with retry logic.
func SendMessageToPath(host string, port int, path string, payload any, opts ...any) (map[string]any, error) {
	retries := 3
	timeout := 5 * time.Second
	if len(opts) >= 1 {
		retries = opts[0].(int)
	}
	if len(opts) >= 2 {
		timeout = opts[1].(time.Duration)
	}

	if path == "" {
		path = "/"
	}
	if path[0] != '/' {
		path = "/" + path
	}
	url := fmt.Sprintf("http://%s:%d%s", host, port, path)
	var lastErr error

	client := &http.Client{Timeout: timeout}

	for attempt := 0; attempt < retries; attempt++ {
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}

		resp, err := client.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}

		var result map[string]any
		if resp.ContentLength != 0 {
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				resp.Body.Close()
				lastErr = err
				continue
			}
		} else {
			result = make(map[string]any)
		}
		resp.Body.Close()
		return result, nil
	}

	log.Printf("All %d attempts failed for %s:%d", retries, host, port)
	return nil, lastErr
}

// MessageHandler is a function that processes a request and returns a response
type MessageHandler func(request map[string]any) map[string]any

// MakeHandler creates an HTTP handler that processes HTTP POST requests
func MakeHandler(messageHandler MessageHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request map[string]any
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}
		} else {
			request = make(map[string]any)
		}

		response := messageHandler(request)

		body, err := json.Marshal(response)
		if err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}
}
