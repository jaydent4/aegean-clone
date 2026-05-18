package hotelworkflow

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
	runHotelClosedClient(c, "workflow/hotel/k6_closed_client.js")
}

func K6ClosedHotelsClientRequestLogic(c *nodes.Client) {
	runHotelClosedClient(c, "workflow/hotel/k6_closed_hotels_client.js")
}

func K6ClosedRecommendationsClientRequestLogic(c *nodes.Client) {
	runHotelClosedClient(c, "workflow/hotel/k6_closed_recommendations_client.js")
}

func K6OpenHotelsClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	userCount := common.MustInt(c.RunConfig, "hotel_user_count")
	hotelCount := common.MustInt(c.RunConfig, "hotel_hotel_count")
	k6QPS := common.MustInt(c.RunConfig, "k6_qps")
	k6PreAllocatedVUs := common.K6PreAllocatedVUs(c.RunConfig, k6QPS)
	k6MaxVUs := common.MustInt(c.RunConfig, "k6_max_vus")
	k6GracefulStop := common.K6GracefulStop(c.RunConfig)
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	baseConfig := hotelK6OpenRunConfig{
		rate:            k6QPS,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		deadline:        k6CommandDeadline,
		sender:          c.Name,
		scriptPath:      "workflow/hotel/k6_open_hotels_client.js",
		gracefulStop:    k6GracefulStop,
		extraEnv: []string{
			"HOTEL_USER_COUNT=" + strconv.Itoa(userCount),
			"HOTEL_HOTEL_COUNT=" + strconv.Itoa(hotelCount),
		},
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, duration, func(runDuration string, suppressOutput bool) error {
		config := baseConfig
		config.duration = runDuration
		config.suppressOutput = suppressOutput
		return runHotelK6Open(config)
	}, func() error {
		return c.DrainPendingRequests(k6CommandDeadline)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("hotel k6 open hotels client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("hotel k6 open hotels client failed: %v", err)
	}
}

func K6OpenRecommendationsClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	userCount := common.MustInt(c.RunConfig, "hotel_user_count")
	hotelCount := common.MustInt(c.RunConfig, "hotel_hotel_count")
	k6QPS := common.MustInt(c.RunConfig, "k6_qps")
	k6PreAllocatedVUs := common.K6PreAllocatedVUs(c.RunConfig, k6QPS)
	k6MaxVUs := common.MustInt(c.RunConfig, "k6_max_vus")
	k6GracefulStop := common.K6GracefulStop(c.RunConfig)
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	baseConfig := hotelK6OpenRunConfig{
		rate:            k6QPS,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		deadline:        k6CommandDeadline,
		sender:          c.Name,
		scriptPath:      "workflow/hotel/k6_open_recommendations_client.js",
		gracefulStop:    k6GracefulStop,
		extraEnv: []string{
			"HOTEL_USER_COUNT=" + strconv.Itoa(userCount),
			"HOTEL_HOTEL_COUNT=" + strconv.Itoa(hotelCount),
		},
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, duration, func(runDuration string, suppressOutput bool) error {
		config := baseConfig
		config.duration = runDuration
		config.suppressOutput = suppressOutput
		return runHotelK6Open(config)
	}, func() error {
		return c.DrainPendingRequests(k6CommandDeadline)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("hotel k6 open recommendations client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("hotel k6 open recommendations client failed: %v", err)
	}
}

func K6OpenReservationClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	userCount := common.MustInt(c.RunConfig, "hotel_user_count")
	hotelCount := common.MustInt(c.RunConfig, "hotel_hotel_count")
	k6QPS := common.MustInt(c.RunConfig, "k6_qps")
	k6PreAllocatedVUs := common.K6PreAllocatedVUs(c.RunConfig, k6QPS)
	k6MaxVUs := common.MustInt(c.RunConfig, "k6_max_vus")
	k6GracefulStop := common.K6GracefulStop(c.RunConfig)
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	baseConfig := hotelK6OpenRunConfig{
		rate:            k6QPS,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		deadline:        k6CommandDeadline,
		sender:          c.Name,
		scriptPath:      "workflow/hotel/k6_open_reservation_client.js",
		gracefulStop:    k6GracefulStop,
		extraEnv: []string{
			"HOTEL_USER_COUNT=" + strconv.Itoa(userCount),
			"HOTEL_HOTEL_COUNT=" + strconv.Itoa(hotelCount),
		},
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, duration, func(runDuration string, suppressOutput bool) error {
		config := baseConfig
		config.duration = runDuration
		config.suppressOutput = suppressOutput
		return runHotelK6Open(config)
	}, func() error {
		return c.DrainPendingRequests(k6CommandDeadline)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("hotel k6 open reservation client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("hotel k6 open reservation client failed: %v", err)
	}
}

func runHotelClosedClient(c *nodes.Client, scriptPath string) {
	duration := common.MustString(c.RunConfig, "duration")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	k6VUs := common.MustInt(c.RunConfig, "k6_vus")
	userCount := common.MustInt(c.RunConfig, "hotel_user_count")
	hotelCount := common.MustInt(c.RunConfig, "hotel_hotel_count")
	k6GracefulStop := common.K6GracefulStop(c.RunConfig)
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runHotelK6(hotelK6RunConfig{
		duration:     duration,
		targetURL:    k6TargetURL,
		deadline:     k6CommandDeadline,
		sender:       c.Name,
		scriptPath:   scriptPath,
		gracefulStop: k6GracefulStop,
		extraEnv: []string{
			"HOTEL_VUS=" + strconv.Itoa(k6VUs),
			"HOTEL_USER_COUNT=" + strconv.Itoa(userCount),
			"HOTEL_HOTEL_COUNT=" + strconv.Itoa(hotelCount),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("hotel k6 closed client timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("hotel k6 closed client failed: %v", err)
	}
}

type hotelK6RunConfig struct {
	duration     string
	targetURL    string
	deadline     time.Duration
	sender       string
	scriptPath   string
	gracefulStop string
	extraEnv     []string
}

type hotelK6OpenRunConfig struct {
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

func runHotelK6(config hotelK6RunConfig) error {
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

func runHotelK6Open(config hotelK6OpenRunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	args := []string{
		"run",
		"-e", "HOTEL_TARGET_URL=" + config.targetURL,
		"-e", "HOTEL_SENDER=" + config.sender,
		"-e", "HOTEL_RATE=" + strconv.Itoa(config.rate),
		"-e", "HOTEL_DURATION=" + config.duration,
		"-e", "HOTEL_GRACEFUL_STOP=" + config.gracefulStop,
		"-e", "HOTEL_PRE_ALLOCATED_VUS=" + strconv.Itoa(config.preAllocatedVUs),
		"-e", "HOTEL_MAX_VUS=" + strconv.Itoa(config.maxVUs),
	}
	for _, envVar := range config.extraEnv {
		args = append(args, "-e", envVar)
	}
	args = append(args, config.scriptPath)

	return warmup.Run(ctx, args, config.suppressOutput)
}
