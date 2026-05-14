package warmup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
)

func Run(ctx context.Context, args []string, suppressOutput bool) error {
	cmd := exec.CommandContext(ctx, "k6", args...)

	var output bytes.Buffer
	if suppressOutput {
		cmd.Stdout = &output
		cmd.Stderr = &output
	} else {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Run(); err != nil {
		if suppressOutput && output.Len() > 0 {
			fmt.Fprint(os.Stderr, output.String())
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("run k6: %w", err)
	}
	return nil
}

func RunWarmupThenMeasured(warmupDuration string, measuredDuration string, run func(duration string, suppressOutput bool) error) error {
	if warmupDuration != "" && warmupDuration != "0s" {
		if err := run(warmupDuration, true); err != nil {
			return err
		}
	}
	return run(measuredDuration, false)
}
