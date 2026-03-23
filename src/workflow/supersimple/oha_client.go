package supersimpleworkflow

import (
	"aegean/common"
	"aegean/nodes"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const ohaBodyPath = "/tmp/oha-requests.ndjson"

func OhaClientRequestLogic(c *nodes.OHAClient) {
	numRequests := common.MustInt(c.RunConfig, "num_requests")
	ohaRequestTimeout := common.MustString(c.RunConfig, "request_timeout")
	ohaCommandDeadlineSeconds := common.MustInt(c.RunConfig, "command_deadline_seconds")
	ohaCommandDeadline := time.Duration(ohaCommandDeadlineSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	ohaTargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	bodyFile, err := os.Create(ohaBodyPath)
	if err != nil {
		log.Printf("failed to create temp request file: %v", err)
		return
	}
	defer os.Remove(ohaBodyPath)

	writer := bufio.NewWriter(bodyFile)
	for requestIdx := 1; requestIdx <= numRequests; requestIdx++ {
		request := map[string]any{
			"timestamp":  float64(time.Now().UnixNano()) / 1e9,
			"sender":     c.Name,
			"op":         "default",
			"op_payload": map[string]any{},
		}
		line, err := json.Marshal(request)
		if err != nil {
			log.Printf("failed to marshal oha request %d: %v", requestIdx, err)
			return
		}
		if _, err := writer.Write(line); err != nil {
			log.Printf("failed to write oha request %d: %v", requestIdx, err)
			return
		}
		if err := writer.WriteByte('\n'); err != nil {
			log.Printf("failed to write oha newline %d: %v", requestIdx, err)
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
