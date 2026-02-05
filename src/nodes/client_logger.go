package nodes

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

const clientLogPath = "/tmp/client_result.jsonl"

type ClientLogger struct {
	file *os.File
	mu   sync.Mutex
}

type RequestLogEntry struct {
	Type           string         `json:"type"`
	RequestID      any            `json:"request_id"`
	SendTo         string         `json:"send_to"`
	StatusCode     string         `json:"status_code"`
	Payload        map[string]any `json:"payload"`
	ExpectedResult string         `json:"expected_result"`
	Timestamp      string         `json:"timestamp"`
}

type ResponseLogEntry struct {
	Type         string         `json:"type"`
	RequestID    any            `json:"request_id"`
	ReceiveFrom  string         `json:"receive_from"`
	Payload      map[string]any `json:"payload"`
	ActualResult map[string]any `json:"actual_result"`
	Timestamp    string         `json:"timestamp"`
}

var (
	logger     *ClientLogger
	loggerOnce sync.Once
)

// GetClientLogger returns the singleton client logger instance
func GetClientLogger() *ClientLogger {
	loggerOnce.Do(func() {
		logger = &ClientLogger{}
		logger.init()
	})
	return logger
}

func (l *ClientLogger) init() {
	file, err := os.OpenFile(clientLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	l.file = file
}

// LogRequest logs an outgoing request
func (l *ClientLogger) LogRequest(requestID any, sendTo string, statusCode string, payload map[string]any, expectedResult string) {
	if l.file == nil {
		return
	}

	entry := RequestLogEntry{
		Type:           "request",
		RequestID:      requestID,
		SendTo:         sendTo,
		StatusCode:     statusCode,
		Payload:        payload,
		ExpectedResult: expectedResult,
		Timestamp:      time.Now().Format(time.RFC3339Nano),
	}

	l.writeEntry(entry)
}

// LogResponse logs an incoming response
func (l *ClientLogger) LogResponse(requestID any, receiveFrom string, payload map[string]any, actualResult map[string]any) {
	if l.file == nil {
		return
	}

	entry := ResponseLogEntry{
		Type:         "response",
		RequestID:    requestID,
		ReceiveFrom:  receiveFrom,
		Payload:      payload,
		ActualResult: actualResult,
		Timestamp:    time.Now().Format(time.RFC3339Nano),
	}

	l.writeEntry(entry)
}

func (l *ClientLogger) writeEntry(entry any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	l.file.Write(data)
	l.file.Write([]byte("\n"))
}

// Close closes the log file
func (l *ClientLogger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
}
