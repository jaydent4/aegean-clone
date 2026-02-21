package aegeanworkflow

import (
	"aegean/nodes"
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const (
	numRequests        = 10
	ohaTargetURL       = "http://node2:8000/"
	ohaTargetNode      = "node2"
	ohaBodyPath        = "/tmp/oha-requests.ndjson"
	ohaRequestTimeout  = "5s"
	ohaCommandDeadline = 1 * time.Minute
)

func OhaClientRequestLogic(c *nodes.Client) {
	c.WaitForNodesReady([]string{ohaTargetNode})

	bodyFile, err := os.Create(ohaBodyPath)
	if err != nil {
		log.Printf("failed to create temp request file: %v", err)
		return
	}
	defer os.Remove(ohaBodyPath)

	writer := bufio.NewWriter(bodyFile)
	for requestID := 1; requestID <= numRequests; requestID++ {
		timestamp := float64(time.Now().UnixNano()) / 1e9
		request := map[string]any{
			"request_id": requestID,
			"timestamp":  timestamp,
			"sender":     c.Name,
			"op":         "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   0.01,
				"write_key":   strconv.Itoa(requestID % writeKeyMod),
				"write_value": "value_" + strconv.Itoa(requestID),
				"read_key":    strconv.Itoa(requestID % readKeyMod),
			},
			"is_client_oha": true,
		}
		line, err := json.Marshal(request)
		if err != nil {
			log.Printf("failed to marshal oha request %d: %v", requestID, err)
			return
		}
		if _, err := writer.Write(line); err != nil {
			log.Printf("failed to write oha request %d: %v", requestID, err)
			return
		}
		if err := writer.WriteByte('\n'); err != nil {
			log.Printf("failed to write oha newline %d: %v", requestID, err)
			return
		}
	}
	if err := writer.Flush(); err != nil {
		log.Printf("failed to flush oha request file: %v", err)
		return
	}
	if err := bodyFile.Close(); err != nil {
		log.Printf("failed to close oha request file: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), ohaCommandDeadline)
	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		"oha",
		"-n", strconv.Itoa(numRequests),
		"-m", "POST",
		"-H", "Content-Type: application/json",
		"-t", ohaRequestTimeout,
		"-Z", ohaBodyPath,
		ohaTargetURL,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("oha client request logic timed out after %s", ohaCommandDeadline)
			return
		}
		log.Printf("oha client request logic failed: %v", err)
	}
}
