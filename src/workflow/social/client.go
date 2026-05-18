package socialworkflow

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
	userCount := common.MustInt(c.RunConfig, "social_user_count")
	postTextLength := common.IntOrDefault(c.RunConfig, "social_post_text_length", 64)
	k6GracefulStop := common.K6MeasuredGracefulStop
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6(k6RunConfig{
		duration:     duration,
		targetURL:    k6TargetURL,
		deadline:     k6CommandDeadline,
		sender:       c.Name,
		scriptPath:   "workflow/social/k6_closed_client.js",
		gracefulStop: k6GracefulStop,
		extraEnv: []string{
			"SOCIAL_VUS=" + strconv.Itoa(k6VUs),
			"SOCIAL_USER_COUNT=" + strconv.Itoa(userCount),
			"SOCIAL_POST_TEXT_LENGTH=" + strconv.Itoa(postTextLength),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("social k6 closed client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("social k6 closed client failed: %v", err)
	}
}

func K6OpenClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	warmupGracefulStop := common.K6WarmupGracefulStop(c.RunConfig)
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	userCount := common.MustInt(c.RunConfig, "social_user_count")
	postTextLength := common.IntOrDefault(c.RunConfig, "social_post_text_length", 64)
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
		scriptPath:      "workflow/social/k6_open_client.js",
		gracefulStop:    k6GracefulStop,
		extraEnv: []string{
			"SOCIAL_USER_COUNT=" + strconv.Itoa(userCount),
			"SOCIAL_POST_TEXT_LENGTH=" + strconv.Itoa(postTextLength),
		},
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, warmupGracefulStop, duration, func(phase warmup.Phase) error {
		config := baseConfig
		config.duration = phase.Duration
		config.gracefulStop = phase.GracefulStop
		config.suppressOutput = phase.SuppressOutput
		return runK6Open(config)
	}, func() error {
		return c.DrainPendingRequests(k6CommandDeadline)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("social k6 open client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("social k6 open client failed: %v", err)
	}
}

func K6ClosedReadUserTimelineClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6VUs := common.MustInt(c.RunConfig, "k6_vus")
	userCount := common.MustInt(c.RunConfig, "social_user_count")
	k6GracefulStop := common.K6MeasuredGracefulStop
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6(k6RunConfig{
		duration:     duration,
		targetURL:    k6TargetURL,
		deadline:     k6CommandDeadline,
		sender:       c.Name,
		scriptPath:   "workflow/social/k6_closed_read_user_timeline.js",
		gracefulStop: k6GracefulStop,
		extraEnv: []string{
			"SOCIAL_VUS=" + strconv.Itoa(k6VUs),
			"SOCIAL_USER_COUNT=" + strconv.Itoa(userCount),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("social k6 closed read user timeline client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("social k6 closed read user timeline client failed: %v", err)
	}
}

func K6OpenReadUserTimelineClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	warmupGracefulStop := common.K6WarmupGracefulStop(c.RunConfig)
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	userCount := common.MustInt(c.RunConfig, "social_user_count")
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
		scriptPath:      "workflow/social/k6_open_read_user_timeline.js",
		gracefulStop:    k6GracefulStop,
		extraEnv: []string{
			"SOCIAL_USER_COUNT=" + strconv.Itoa(userCount),
		},
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, warmupGracefulStop, duration, func(phase warmup.Phase) error {
		config := baseConfig
		config.duration = phase.Duration
		config.gracefulStop = phase.GracefulStop
		config.suppressOutput = phase.SuppressOutput
		return runK6Open(config)
	}, func() error {
		return c.DrainPendingRequests(k6CommandDeadline)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("social k6 open read user timeline client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("social k6 open read user timeline client failed: %v", err)
	}
}

func K6ClosedReadHomeTimelineClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6VUs := common.MustInt(c.RunConfig, "k6_vus")
	userCount := common.MustInt(c.RunConfig, "social_user_count")
	k6GracefulStop := common.K6MeasuredGracefulStop
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6(k6RunConfig{
		duration:     duration,
		targetURL:    k6TargetURL,
		deadline:     k6CommandDeadline,
		sender:       c.Name,
		scriptPath:   "workflow/social/k6_closed_read_home_timeline.js",
		gracefulStop: k6GracefulStop,
		extraEnv: []string{
			"SOCIAL_VUS=" + strconv.Itoa(k6VUs),
			"SOCIAL_USER_COUNT=" + strconv.Itoa(userCount),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("social k6 closed read home timeline client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("social k6 closed read home timeline client failed: %v", err)
	}
}

func K6OpenReadHomeTimelineClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	warmupGracefulStop := common.K6WarmupGracefulStop(c.RunConfig)
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	userCount := common.MustInt(c.RunConfig, "social_user_count")
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
		scriptPath:      "workflow/social/k6_open_read_home_timeline.js",
		gracefulStop:    k6GracefulStop,
		extraEnv: []string{
			"SOCIAL_USER_COUNT=" + strconv.Itoa(userCount),
		},
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, warmupGracefulStop, duration, func(phase warmup.Phase) error {
		config := baseConfig
		config.duration = phase.Duration
		config.gracefulStop = phase.GracefulStop
		config.suppressOutput = phase.SuppressOutput
		return runK6Open(config)
	}, func() error {
		return c.DrainPendingRequests(k6CommandDeadline)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("social k6 open read home timeline client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("social k6 open read home timeline client failed: %v", err)
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
		"-e", "SOCIAL_TARGET_URL=" + config.targetURL,
		"-e", "SOCIAL_SENDER=" + config.sender,
		"-e", "SOCIAL_RATE=" + strconv.Itoa(config.rate),
		"-e", "SOCIAL_DURATION=" + config.duration,
		"-e", "SOCIAL_GRACEFUL_STOP=" + config.gracefulStop,
		"-e", "SOCIAL_PRE_ALLOCATED_VUS=" + strconv.Itoa(config.preAllocatedVUs),
		"-e", "SOCIAL_MAX_VUS=" + strconv.Itoa(config.maxVUs),
	}
	for _, envVar := range config.extraEnv {
		args = append(args, "-e", envVar)
	}
	args = append(args, config.scriptPath)

	return warmup.Run(ctx, args, config.suppressOutput)
}
