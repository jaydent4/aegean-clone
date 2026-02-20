package exec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

func shortHash(hash string) string {
	if len(hash) <= 6 {
		return hash
	}
	return hash[len(hash)-6:]
}

func (e *Exec) computeStateHash(stateRoot string, outputs []map[string]any, prevHash string, seqNum int) string {
	data := map[string]any{
		"seq_num":    seqNum,
		"prev_hash":  prevHash,
		"state_root": stateRoot,
		"outputs":    outputs,
	}
	encoded := marshalSorted(data)
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}

// marshalSorted produces JSON with sorted keys to match Python's json.dumps(sort_keys=True)
func marshalSorted(v any) []byte {
	var buf bytes.Buffer
	writeSorted(&buf, v)
	return buf.Bytes()
}

func writeSorted(buf *bytes.Buffer, v any) {
	switch val := v.(type) {
	case map[string]any:
		buf.WriteByte('{')
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				buf.WriteString(", ")
			}
			keyBytes, _ := json.Marshal(k)
			buf.Write(keyBytes)
			buf.WriteString(": ")
			writeSorted(buf, val[k])
		}
		buf.WriteByte('}')
	case map[string]string:
		buf.WriteByte('{')
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				buf.WriteString(", ")
			}
			keyBytes, _ := json.Marshal(k)
			buf.Write(keyBytes)
			buf.WriteString(": ")
			valBytes, _ := json.Marshal(val[k])
			buf.Write(valBytes)
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			writeSorted(buf, item)
		}
		buf.WriteByte(']')
	case []map[string]any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			writeSorted(buf, item)
		}
		buf.WriteByte(']')
	default:
		encoded, _ := json.Marshal(val)
		buf.Write(encoded)
	}
}
