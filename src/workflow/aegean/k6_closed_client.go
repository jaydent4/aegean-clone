package aegeanworkflow

import (
	"aegean/common"
	"aegean/nodes"
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

func K6ClosedClientRequestLogic(c *nodes.Client) {
	duration := common.MustString(c.RunConfig, "duration")
	spinTimeSeconds := common.MustFloat64(c.RunConfig, "spin_time_seconds")
	runTimeoutSeconds := common.MustInt(c.RunConfig, "run_timeout_seconds")
	writeKeyMod := common.MustInt(c.RunConfig, "write_key_mod")
	readKeyMod := common.MustInt(c.RunConfig, "read_key_mod")
	valueLength := common.MustInt(c.RunConfig, "value_length")
	k6VUs := common.MustInt(c.RunConfig, "k6_vus")
	k6CommandDeadline := time.Duration(runTimeoutSeconds) * time.Second

	c.WaitForNodesReady(c.ReadyNodes)
	k6TargetURL := fmt.Sprintf("http://%s:8000/", c.Name)

	if err := runK6(k6RunConfig{
		duration:   duration,
		targetURL:  k6TargetURL,
		deadline:   k6CommandDeadline,
		sender:     c.Name,
		scriptPath: "workflow/aegean/k6_closed_client.js",
		extraEnv: []string{
			"SPIN_TIME_SECONDS=" + fmt.Sprintf("%g", spinTimeSeconds),
			"WRITE_KEY_MOD=" + strconv.Itoa(writeKeyMod),
			"READ_KEY_MOD=" + strconv.Itoa(readKeyMod),
			"VALUE_LENGTH=" + strconv.Itoa(valueLength),
			"AEGEAN_VUS=" + strconv.Itoa(k6VUs),
		},
	}); err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("k6 closed client request logic timed out after %s", k6CommandDeadline)
			return
		}
		log.Printf("k6 closed client request logic failed: %v", err)
	}
}
