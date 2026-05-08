package mixer

import (
	mixerkeys "aegean/aegean/mixer/keys"
	"aegean/common"
	"aegean/telemetry"
	"context"
)

type Mixer struct {
	Name            string
	NextCh          chan<- map[string]any
	SocialMixerMode string
}

func NewMixer(name string, nextCh chan<- map[string]any, runConfig map[string]any) *Mixer {
	if nextCh == nil {
		panic("mixer component requires non-nil nextCh")
	}
	m := &Mixer{
		Name:            name,
		NextCh:          nextCh,
		SocialMixerMode: mixerkeys.ResolveSocialMixerMode(runConfig),
	}
	return m
}

func (m *Mixer) getKeys(request map[string]any) (map[string]struct{}, map[string]struct{}) {
	payload, _ := request["op_payload"].(map[string]any)

	readKeys := make(map[string]struct{})
	writeKeys := make(map[string]struct{})

	mixerkeys.AddGenericWorkflowKeys(request, payload, readKeys, writeKeys)
	mixerkeys.AddHotelWorkflowKeys(request, payload, readKeys, writeKeys)
	mixerkeys.AddSocialWorkflowKeys(request, payload, readKeys, writeKeys, string(m.SocialMixerMode))
	mixerkeys.AddMediaWorkflowKeys(request, payload, readKeys, writeKeys)

	return readKeys, writeKeys
}

func hasIntersection(a, b map[string]struct{}) bool {
	for key := range a {
		if _, ok := b[key]; ok {
			return true
		}
	}
	return false
}

func (m *Mixer) hasConflict(reqRead, reqWrite, batchReads, batchWrites map[string]struct{}) bool {
	if hasIntersection(reqWrite, batchWrites) {
		return true
	}
	if hasIntersection(reqWrite, batchReads) {
		return true
	}
	if hasIntersection(reqRead, batchWrites) {
		return true
	}
	return false
}

type parallelBatch struct {
	requests []map[string]any
	reads    map[string]struct{}
	writes   map[string]struct{}
}

func (m *Mixer) partitionIntoParallelBatches(batch []map[string]any) [][]map[string]any {
	parallelBatches := []parallelBatch{}

	for _, request := range batch {
		reqRead, reqWrite := m.getKeys(request)
		placed := false

		for i := range parallelBatches {
			pb := &parallelBatches[i]
			if !m.hasConflict(reqRead, reqWrite, pb.reads, pb.writes) {
				pb.requests = append(pb.requests, request)
				for key := range reqRead {
					pb.reads[key] = struct{}{}
				}
				for key := range reqWrite {
					pb.writes[key] = struct{}{}
				}
				placed = true
				break
			}
		}

		if !placed {
			pb := parallelBatch{
				requests: []map[string]any{request},
				reads:    reqRead,
				writes:   reqWrite,
			}
			parallelBatches = append(parallelBatches, pb)
		}
	}

	result := make([][]map[string]any, 0, len(parallelBatches))
	for _, pb := range parallelBatches {
		result = append(result, pb.requests)
	}
	return result
}

func (m *Mixer) HandleBatchMessage(payload map[string]any) map[string]any {
	ctx := telemetry.ExtractContext(context.Background(), payload)

	seqNum := common.GetInt(payload, "seq_num")
	requests, ok := normalizeRequestSlice(payload["requests"])
	if !ok {
		return map[string]any{"status": "error", "error": "invalid requests"}
	}

	parallelBatches := m.partitionIntoParallelBatches(requests)

	message := map[string]any{
		"type":             "batch",
		"seq_num":          seqNum,
		"parallel_batches": parallelBatches,
		"nd_seed":          payload["nd_seed"],
		"nd_timestamp":     payload["nd_timestamp"],
	}
	telemetry.InjectContext(ctx, message)

	if m.NextCh != nil {
		m.NextCh <- message
	} else {
	}

	return map[string]any{"status": "mixed", "seq_num": seqNum}
}

func normalizeRequestSlice(v any) ([]map[string]any, bool) {
	switch typed := v.(type) {
	case []map[string]any:
		return typed, true
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			req, ok := item.(map[string]any)
			if !ok {
				return nil, false
			}
			out = append(out, req)
		}
		return out, true
	default:
		return nil, false
	}
}
