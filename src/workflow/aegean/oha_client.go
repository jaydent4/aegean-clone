package aegeanworkflow

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
	"strings"
	"time"
)

const ohaBodyPath = "/tmp/oha-requests.ndjson"

func OhaClientRequestLogic(c *nodes.Client) {
	numRequests := common.MustInt(c.RunConfig, "num_requests")
	spinTimeSeconds := common.MustFloat64(c.RunConfig, "spin_time_seconds")
	ohaRequestTimeout := common.MustString(c.RunConfig, "request_timeout")
	ohaCommandDeadlineSeconds := common.MustInt(c.RunConfig, "command_deadline_seconds")
	writeKeyMod := common.MustInt(c.RunConfig, "write_key_mod")
	readKeyMod := common.MustInt(c.RunConfig, "read_key_mod")
	valueLength := common.MustInt(c.RunConfig, "value_length")
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
		timestamp := float64(time.Now().UnixNano()) / 1e9
		request := map[string]any{
			"timestamp": timestamp,
			"sender":    c.Name,
			"op":        "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   spinTimeSeconds,
				"write_key":   strconv.Itoa(requestIdx % writeKeyMod),
				"write_value": makeLargeWriteValue(requestIdx, valueLength),
				"read_key":    strconv.Itoa(requestIdx % readKeyMod),
			},
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

func makeLargeWriteValue(requestID int, valueLength int) string {
	token := strconv.Itoa(requestID)
	repeat := valueLength/len(token) + 1
	return strings.Repeat(token, repeat)
}
