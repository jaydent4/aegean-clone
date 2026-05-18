package externalsrvworkflow

import (
	"aegean/common"
	"aegean/nodes"
	"aegean/workflow/warmup"
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
	k6GracefulStop := common.K6MeasuredGracefulStop
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6(k6RunConfig{
		duration:     duration,
		targetURL:    k6TargetURL,
		deadline:     k6CommandDeadline,
		sender:       c.Name,
		scriptPath:   "workflow/external_srv/k6_closed_client.js",
		gracefulStop: k6GracefulStop,
		extraEnv: []string{
			"EXTERNAL_SRV_VUS=" + strconv.Itoa(k6VUs),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("k6 closed client request logic timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("k6 closed client request logic failed: %v", err)
	}
}

func K6OpenClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6QPS := common.MustInt(c.RunConfig, "k6_qps")
	k6PreAllocatedVUs := common.K6PreAllocatedVUs(c.RunConfig, k6QPS)
	k6MaxVUs := common.MustInt(c.RunConfig, "k6_max_vus")
	k6GracefulStop := common.K6MeasuredGracefulStop
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	baseConfig := k6OpenRunConfig{
		rate:            k6QPS,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		deadline:        k6CommandDeadline,
		sender:          c.Name,
		scriptPath:      "workflow/external_srv/k6_open_client.js",
		gracefulStop:    k6GracefulStop,
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, duration, func(phase warmup.Phase) error {
		config := baseConfig
		config.duration = phase.Duration
		config.gracefulStop = phase.GracefulStop
		config.suppressOutput = phase.SuppressOutput
		return runK6Open(config)
	}, func() error {
		return c.DrainPendingRequests(k6CommandDeadline)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("k6 open client request logic timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("k6 open client request logic failed: %v", err)
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

type k6OpenRunConfig struct {
	rate            int
	duration        string
	preAllocatedVUs int
	maxVUs          int
	targetURL       string
	deadline        time.Duration
	sender          string
	scriptPath      string
	gracefulStop    string
	extraEnv        []string
	suppressOutput  bool
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

func runK6Open(config k6OpenRunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	args := []string{
		"run",
		"-e", "EXTERNAL_SRV_TARGET_URL=" + config.targetURL,
		"-e", "EXTERNAL_SRV_SENDER=" + config.sender,
		"-e", "EXTERNAL_SRV_RATE=" + strconv.Itoa(config.rate),
		"-e", "EXTERNAL_SRV_DURATION=" + config.duration,
		"-e", "EXTERNAL_SRV_GRACEFUL_STOP=" + config.gracefulStop,
		"-e", "EXTERNAL_SRV_PRE_ALLOCATED_VUS=" + strconv.Itoa(config.preAllocatedVUs),
		"-e", "EXTERNAL_SRV_MAX_VUS=" + strconv.Itoa(config.maxVUs),
	}
	for _, envVar := range config.extraEnv {
		args = append(args, "-e", envVar)
	}
	args = append(args, config.scriptPath)

	return warmup.Run(ctx, args, config.suppressOutput)
}
