package eo

import (
	"log"
	"time"
)

const raftResponseTimingWindow = time.Second

type raftResponseProposalTiming struct {
	loopStart        time.Time
	rawProposeDone   time.Time
	initialDrainDone time.Time
	groupSize        int
}

type raftResponseTimingStats struct {
	windowStart           time.Time
	responses             int
	loopToRawPropose      time.Duration
	rawProposeToDrain     time.Duration
	drainToLearn          time.Duration
	loopToLearn           time.Duration
	proposalGroupSizeSum  int
	maxProposalGroupSize  int
	maxLoopToLearn        time.Duration
	negativeDurationCount int
}

func newRaftResponseTimingStats(windowStart time.Time) raftResponseTimingStats {
	return raftResponseTimingStats{windowStart: windowStart}
}

func (s *raftResponseTimingStats) record(timing raftResponseProposalTiming, learnTime time.Time) {
	if timing.groupSize > 0 {
		s.proposalGroupSizeSum += timing.groupSize
		if timing.groupSize > s.maxProposalGroupSize {
			s.maxProposalGroupSize = timing.groupSize
		}
	}
	s.responses++
	s.addDuration(timing.rawProposeDone.Sub(timing.loopStart), &s.loopToRawPropose)
	s.addDuration(timing.initialDrainDone.Sub(timing.rawProposeDone), &s.rawProposeToDrain)
	s.addDuration(learnTime.Sub(timing.initialDrainDone), &s.drainToLearn)
	loopToLearn := learnTime.Sub(timing.loopStart)
	s.addDuration(loopToLearn, &s.loopToLearn)
	if loopToLearn > s.maxLoopToLearn {
		s.maxLoopToLearn = loopToLearn
	}
}

func (s *raftResponseTimingStats) addDuration(duration time.Duration, total *time.Duration) {
	if duration < 0 {
		s.negativeDurationCount++
		return
	}
	*total += duration
}

func (b *raftConsensusBox) maybeLogRaftResponseTiming(now time.Time, reason string) {
	if b.responseTimingStats.responses == 0 {
		if now.Sub(b.responseTimingStats.windowStart) >= raftResponseTimingWindow {
			b.responseTimingStats = newRaftResponseTimingStats(now)
		}
		return
	}
	if now.Sub(b.responseTimingStats.windowStart) < raftResponseTimingWindow {
		return
	}

	stats := b.responseTimingStats
	window := now.Sub(stats.windowStart)
	log.Printf(
		"%s: eo_raft_response_timing window_ms=%d reason=%s responses=%d avg_proposal_group_size=%.2f max_proposal_group_size=%d loop_to_raw_propose_us=%d raw_propose_to_initial_drain_us=%d initial_drain_to_learn_us=%d loop_to_learn_us=%d max_loop_to_learn_us=%d negative_durations=%d",
		b.name,
		window.Milliseconds(),
		reason,
		stats.responses,
		float64(stats.proposalGroupSizeSum)/float64(stats.responses),
		stats.maxProposalGroupSize,
		raftAvgMicros(stats.loopToRawPropose, stats.responses),
		raftAvgMicros(stats.rawProposeToDrain, stats.responses),
		raftAvgMicros(stats.drainToLearn, stats.responses),
		raftAvgMicros(stats.loopToLearn, stats.responses),
		stats.maxLoopToLearn.Microseconds(),
		stats.negativeDurationCount,
	)
	b.responseTimingStats = newRaftResponseTimingStats(now)
}

func raftAvgMicros(total time.Duration, count int) int64 {
	if count <= 0 {
		return 0
	}
	return total.Microseconds() / int64(count)
}
