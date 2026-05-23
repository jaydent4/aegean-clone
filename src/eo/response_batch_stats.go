package eo

import (
	"log"
	"time"
)

const responseBatchStatsWindow = time.Second

type responseBatchStats struct {
	windowStart time.Time
	batches     int
	responses   int
	maxBatch    int
}

func (e *EO) recordResponseBatch(size int) {
	if size <= 0 {
		return
	}
	now := time.Now()
	stats := &e.responseBatchStats
	if stats.windowStart.IsZero() {
		stats.windowStart = now
	}
	stats.batches++
	stats.responses += size
	if size > stats.maxBatch {
		stats.maxBatch = size
	}
	if now.Sub(stats.windowStart) < responseBatchStatsWindow {
		return
	}
	window := now.Sub(stats.windowStart)
	log.Printf(
		"%s: eo_response_batch_stats window_ms=%d batches=%d responses=%d avg_batch_size=%.2f max_batch_size=%d configured_batch_size=%d configured_timeout_us=%d",
		e.name,
		window.Milliseconds(),
		stats.batches,
		stats.responses,
		float64(stats.responses)/float64(stats.batches),
		stats.maxBatch,
		e.responseBatchSize,
		e.responseBatchTimeout.Microseconds(),
	)
	*stats = responseBatchStats{windowStart: now}
}
