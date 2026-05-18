package responseworkflow

import (
	"aegean/common"
	"aegean/nodes"
	"aegean/workflow/warmup"
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

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

	baseConfig := responseK6OpenRunConfig{
		rate:            k6QPS,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		deadline:        k6CommandDeadline,
		sender:          c.Name,
		scriptPath:      "workflow/response/k6_open_client.js",
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
			log.Printf("response k6 client request logic timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("response k6 client request logic failed: %v", err)
	}
}

type responseK6OpenRunConfig struct {
	rate            int
	duration        string
	preAllocatedVUs int
	maxVUs          int
	targetURL       string
	deadline        time.Duration
	sender          string
	scriptPath      string
	gracefulStop    string
	suppressOutput  bool
}

func runK6Open(config responseK6OpenRunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	args := []string{
		"run",
		"-e", "RESPONSE_TARGET_URL=" + config.targetURL,
		"-e", "RESPONSE_SENDER=" + config.sender,
		"-e", "RESPONSE_RATE=" + strconv.Itoa(config.rate),
		"-e", "RESPONSE_DURATION=" + config.duration,
		"-e", "RESPONSE_GRACEFUL_STOP=" + config.gracefulStop,
		"-e", "RESPONSE_PRE_ALLOCATED_VUS=" + strconv.Itoa(config.preAllocatedVUs),
		"-e", "RESPONSE_MAX_VUS=" + strconv.Itoa(config.maxVUs),
		config.scriptPath,
	}

	return warmup.Run(ctx, args, config.suppressOutput)
}
