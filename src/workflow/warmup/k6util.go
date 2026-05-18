package warmup

import (
	"aegean/common"
	"bytes"
	"context"
	"fmt"
	"log"
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

type Phase struct {
	Duration       string
	GracefulStop   string
	SuppressOutput bool
}

func RunWarmupThenMeasured(warmupDuration string, warmupGracefulStop string, measuredDuration string, run func(Phase) error, afterWarmup ...func() error) error {
	if warmupDuration != "" && warmupDuration != "0s" {
		if err := run(Phase{
			Duration:       warmupDuration,
			GracefulStop:   warmupGracefulStop,
			SuppressOutput: true,
		}); err != nil {
			return err
		}
		for _, hook := range afterWarmup {
			if hook == nil {
				continue
			}
			log.Printf("warmup complete; draining pending work before measured run")
			if err := hook(); err != nil {
				return fmt.Errorf("drain warmup work: %w", err)
			}
			log.Printf("warmup drain complete")
		}
	}
	return run(Phase{
		Duration:     measuredDuration,
		GracefulStop: common.K6MeasuredGracefulStop,
	})
}
