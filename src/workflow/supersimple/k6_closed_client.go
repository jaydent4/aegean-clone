package supersimpleworkflow

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

func K6ClosedClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6VUs := common.MustInt(c.RunConfig, "k6_vus")
	k6GracefulStop := common.K6GracefulStop(c.RunConfig)
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6(k6RunConfig{
		duration:     duration,
		targetURL:    k6TargetURL,
		deadline:     k6CommandDeadline,
		sender:       c.Name,
		scriptPath:   "workflow/supersimple/k6_closed_client.js",
		gracefulStop: k6GracefulStop,
		extraEnv: []string{
			"SUPERSIMPLE_VUS=" + strconv.Itoa(k6VUs),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("k6 closed client request logic timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("k6 closed client request logic failed: %v", err)
	}
}

type k6RunConfig struct {
	duration     string
	targetURL    string
	deadline     time.Duration
	sender       string
	scriptPath   string
	gracefulStop string
	extraEnv     []string
}

func runK6(config k6RunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	args := []string{
		"run",
		"-e", "TARGET_URL=" + config.targetURL,
		"-e", "SENDER=" + config.sender,
		"-e", "DURATION=" + config.duration,
		"-e", "GRACEFUL_STOP=" + config.gracefulStop,
	}
	for _, envVar := range config.extraEnv {
		args = append(args, "-e", envVar)
	}
	args = append(args, config.scriptPath)

	cmd := exec.CommandContext(ctx, "k6", args...)
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
