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

const ohaRequestCount = 10000
const ohaBodyPath = "/tmp/oha-requests.ndjson"

func OhaClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	spinTimeSeconds := common.MustFloat64(c.RunConfig, "spin_time_seconds")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	writeKeyMod := common.MustInt(c.RunConfig, "write_key_mod")
	readKeyMod := common.MustInt(c.RunConfig, "read_key_mod")
	valueLength := common.MustInt(c.RunConfig, "value_length")
	ohaCommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	ohaTargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runOha(duration, ohaCommandDeadline, ohaTargetURL, func(requestIdx int) map[string]any {
		return map[string]any{
			"timestamp": float64(time.Now().UnixNano()) / 1e9,
			"sender":    c.Name,
			"op":        "spin_write_read",
			"op_payload": map[string]any{
				"spin_time":   spinTimeSeconds,
				"write_key":   strconv.Itoa(requestIdx % writeKeyMod),
				"write_value": makeLargeWriteValue(requestIdx, valueLength),
				"read_key":    strconv.Itoa(requestIdx % readKeyMod),
			},
		}
	}); err != nil {
		if err == context.DeadlineExceeded {
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

func runOha(duration string, deadline time.Duration, targetURL string, buildRequest func(int) map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	if err := writeRequests(ohaBodyPath, ohaRequestCount, buildRequest); err != nil {
		return err
	}
	defer os.Remove(ohaBodyPath)

	cmd := exec.CommandContext(
		ctx,
		"oha",
		"-z", duration,
		"-m", "POST",
		"-H", "Content-Type: application/json",
		"-Z", ohaBodyPath,
		targetURL,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return ctx.Err()
		}
		return fmt.Errorf("run oha: %w", err)
	}

	return nil
}

func writeRequests(bodyPath string, requestCount int, buildRequest func(int) map[string]any) error {
	bodyFile, err := os.Create(bodyPath)
	if err != nil {
		return fmt.Errorf("create request file: %w", err)
	}
	defer bodyFile.Close()

	writer := bufio.NewWriter(bodyFile)
	for requestIdx := 1; requestIdx <= requestCount; requestIdx++ {
		line, err := json.Marshal(buildRequest(requestIdx))
		if err != nil {
			return err
		}
		if _, err := writer.Write(line); err != nil {
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			return err
		}
	}
	return writer.Flush()
}
