package writeworkflow

import (
	"aegean/common"
	"aegean/nodes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func K6OpenClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6QPS := common.MustInt(c.RunConfig, "k6_qps")
	k6PreAllocatedVUs := common.MustInt(c.RunConfig, "k6_pre_allocated_vus")
	k6MaxVUs := common.MustInt(c.RunConfig, "k6_max_vus")
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6Open(writeK6OpenRunConfig{
		rate:            k6QPS,
		duration:        duration,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		deadline:        k6CommandDeadline,
		scriptPath:      "workflow/write/k6_open_client.js",
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("write k6 client request logic timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("write k6 client request logic failed: %v", err)
	}
}

type writeK6OpenRunConfig struct {
	rate            int
	duration        string
	preAllocatedVUs int
	maxVUs          int
	targetURL       string
	deadline        time.Duration
	scriptPath      string
}

func runK6Open(config writeK6OpenRunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	cmd := exec.CommandContext(ctx, "k6",
		"run",
		"-e", "WRITE_TARGET_URL="+config.targetURL,
		"-e", "WRITE_RATE="+strconv.Itoa(config.rate),
		"-e", "WRITE_DURATION="+config.duration,
		"-e", "WRITE_PRE_ALLOCATED_VUS="+strconv.Itoa(config.preAllocatedVUs),
		"-e", "WRITE_MAX_VUS="+strconv.Itoa(config.maxVUs),
		config.scriptPath,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return ctx.Err()
		}
		return fmt.Errorf("run k6: %w", err)
	}

	return nil
}
