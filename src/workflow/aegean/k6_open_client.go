package aegeanworkflow

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
	spinTimeSeconds := common.MustFloat64(c.RunConfig, "spin_time_seconds")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	writeKeyMod := common.MustInt(c.RunConfig, "write_key_mod")
	readKeyMod := common.MustInt(c.RunConfig, "read_key_mod")
	valueLength := common.MustInt(c.RunConfig, "value_length")
	k6QPS := common.MustInt(c.RunConfig, "k6_qps")
	duration := common.MustString(c.RunConfig, "duration")
	warmupDuration := common.StringOrDefault(c.RunConfig, "warmup_duration", "0s")
	k6PreAllocatedVUs := common.K6PreAllocatedVUs(c.RunConfig, k6QPS)
	k6MaxVUs := common.MustInt(c.RunConfig, "k6_max_vus")
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	baseConfig := k6RunConfig{
		rate:            k6QPS,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		deadline:        k6CommandDeadline,
		sender:          c.Name,
		scriptPath:      "workflow/aegean/k6_open_client.js",
		extraEnv: []string{
			"SPIN_TIME_SECONDS=" + fmt.Sprintf("%g", spinTimeSeconds),
			"WRITE_KEY_MOD=" + strconv.Itoa(writeKeyMod),
			"READ_KEY_MOD=" + strconv.Itoa(readKeyMod),
			"VALUE_LENGTH=" + strconv.Itoa(valueLength),
		},
	}

	if err := warmup.RunWarmupThenMeasured(warmupDuration, duration, func(runDuration string, suppressOutput bool) error {
		config := baseConfig
		config.duration = runDuration
		config.suppressOutput = suppressOutput
		return runK6(config)
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("k6 client request logic timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("k6 client request logic failed: %v", err)
	}
}

type k6RunConfig struct {
	rate            int
	duration        string
	preAllocatedVUs int
	maxVUs          int
	targetURL       string
	deadline        time.Duration
	sender          string
	scriptPath      string
	extraEnv        []string
	suppressOutput  bool
}

func runK6(config k6RunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	args := []string{
		"run",
		"-e", "AEGEAN_TARGET_URL=" + config.targetURL,
		"-e", "AEGEAN_SENDER=" + config.sender,
		"-e", "AEGEAN_RATE=" + strconv.Itoa(config.rate),
		"-e", "AEGEAN_DURATION=" + config.duration,
		"-e", "AEGEAN_PRE_ALLOCATED_VUS=" + strconv.Itoa(config.preAllocatedVUs),
		"-e", "AEGEAN_MAX_VUS=" + strconv.Itoa(config.maxVUs),
	}
	for _, envVar := range config.extraEnv {
		args = append(args, "-e", envVar)
	}
	args = append(args, config.scriptPath)

	return warmup.Run(ctx, args, config.suppressOutput)
}
