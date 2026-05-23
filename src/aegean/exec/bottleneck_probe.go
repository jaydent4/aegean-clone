package exec

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"aegean/common"
)

const (
	execBottleneckProbeRunConfigKey = "exec_bottleneck_probe_log"
	execBottleneckProbeWindow       = time.Second
)

type execBottleneckProbe struct {
	enabled     bool
	node        string
	windowStart time.Time
	mu          sync.Mutex
	functions   map[string]*execBottleneckFunctionStats
}

type execBottleneckFunctionStats struct {
	count int64
	total time.Duration
	max   time.Duration
}

func newExecBottleneckProbe(node string, runConfig map[string]any) *execBottleneckProbe {
	return &execBottleneckProbe{
		enabled:     common.BoolOrDefault(runConfig, execBottleneckProbeRunConfigKey, false),
		node:        node,
		windowStart: time.Now(),
		functions:   make(map[string]*execBottleneckFunctionStats),
	}
}

func (p *execBottleneckProbe) record(name string, duration time.Duration) {
	if p == nil || !p.enabled {
		return
	}
	if duration < 0 {
		duration = 0
	}
	now := time.Now()

	p.mu.Lock()
	stats := p.functions[name]
	if stats == nil {
		stats = &execBottleneckFunctionStats{}
		p.functions[name] = stats
	}
	stats.count++
	stats.total += duration
	if duration > stats.max {
		stats.max = duration
	}
	if now.Sub(p.windowStart) < execBottleneckProbeWindow {
		p.mu.Unlock()
		return
	}

	windowStart := p.windowStart
	snapshot := p.functions
	p.windowStart = now
	p.functions = make(map[string]*execBottleneckFunctionStats)
	p.mu.Unlock()

	p.logSnapshot(windowStart, now, snapshot)
}

func (p *execBottleneckProbe) logSnapshot(start, end time.Time, snapshot map[string]*execBottleneckFunctionStats) {
	if len(snapshot) == 0 {
		return
	}
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		left := snapshot[names[i]]
		right := snapshot[names[j]]
		if left.total == right.total {
			return names[i] < names[j]
		}
		return left.total > right.total
	})
	parts := make([]string, 0, len(names))
	for _, name := range names {
		stats := snapshot[name]
		avg := int64(0)
		if stats.count > 0 {
			avg = stats.total.Microseconds() / stats.count
		}
		parts = append(parts, fmt.Sprintf(
			"%s:%d:%d:%d:%d",
			name,
			stats.count,
			stats.total.Microseconds(),
			avg,
			stats.max.Microseconds(),
		))
	}
	log.Printf(
		"%s: exec_bottleneck_probe window_ms=%d functions=count:total_us:avg_us:max_us %s",
		p.node,
		end.Sub(start).Milliseconds(),
		strings.Join(parts, ","),
	)
}

func recordExecProbeDuration(probe *execBottleneckProbe, name string, start time.Time) {
	if probe == nil || !probe.enabled {
		return
	}
	probe.record(name, time.Since(start))
}
