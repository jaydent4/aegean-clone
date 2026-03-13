package aegeanworkflow

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

func K6ClientRequestLogic(c *nodes.Client) {
	spinTimeSeconds := common.MustFloat64(c.RunConfig, "spin_time_seconds")
	k6RequestTimeout := common.MustString(c.RunConfig, "request_timeout")
	k6CommandDeadlineSeconds := common.MustInt(c.RunConfig, "command_deadline_seconds")
	writeKeyMod := common.MustInt(c.RunConfig, "write_key_mod")
	readKeyMod := common.MustInt(c.RunConfig, "read_key_mod")
	valueLength := common.MustInt(c.RunConfig, "value_length")
	k6QPS := common.MustInt(c.RunConfig, "k6_qps")
	k6Duration := common.MustString(c.RunConfig, "k6_duration")
	k6PreAllocatedVUs := common.MustInt(c.RunConfig, "k6_pre_allocated_vus")
	k6MaxVUs := common.MustInt(c.RunConfig, "k6_max_vus")
	k6CommandDeadline := time.Duration(k6CommandDeadlineSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6(k6RunConfig{
		rate:            k6QPS,
		duration:        k6Duration,
		preAllocatedVUs: k6PreAllocatedVUs,
		maxVUs:          k6MaxVUs,
		targetURL:       k6TargetURL,
		requestTimeout:  k6RequestTimeout,
		deadline:        k6CommandDeadline,
		sender:          c.Name,
		scriptPath:      "workflow/aegean/k6_client.js",
		extraEnv: []string{
			"SPIN_TIME_SECONDS=" + fmt.Sprintf("%g", spinTimeSeconds),
			"WRITE_KEY_MOD=" + strconv.Itoa(writeKeyMod),
			"READ_KEY_MOD=" + strconv.Itoa(readKeyMod),
			"VALUE_LENGTH=" + strconv.Itoa(valueLength),
		},
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
	requestTimeout  string
	deadline        time.Duration
	sender          string
	scriptPath      string
	extraEnv        []string
}

func runK6(config k6RunConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.deadline)
	defer cancel()

	args := []string{
		"run",
		"-e", "AEGEAN_TARGET_URL=" + config.targetURL,
		"-e", "AEGEAN_REQUEST_TIMEOUT=" + config.requestTimeout,
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
