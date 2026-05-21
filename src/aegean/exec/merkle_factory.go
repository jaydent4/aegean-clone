package exec

import (
	"fmt"
	"strings"

	"aegean/aegean/merkle"
	"aegean/aegean/merkle_iavl"
	"aegean/aegean/merkle_treap"
	"aegean/common"
)

const runConfigMerkle = "merkle"
const runConfigMerkleUseIAVL = "merkle_use_iavl"
const runConfigMerkleUseTreap = "merkle_use_treap"

const (
	merkleBackendCanonical = "canonical"
	merkleBackendIAVL      = "iavl"
	merkleBackendTreap     = "treap"
)

func newMerkleTreeFromMap(kv map[string]string, runConfig map[string]any) merkle.Tree {
	switch merkleBackend(runConfig) {
	case merkleBackendTreap:
		return merkle_treap.NewTreeFromMap(kv)
	case merkleBackendIAVL:
		return merkle_iavl.NewTreeFromMap(kv)
	case merkleBackendCanonical:
		return merkle.NewTreeFromMap(kv)
	default:
		panic("unreachable merkle backend")
	}
}

func merkleBackend(runConfig map[string]any) string {
	if raw, ok := runConfig[runConfigMerkle]; ok {
		value, ok := raw.(string)
		if !ok {
			panic(fmt.Sprintf("run config field %q must be a string", runConfigMerkle))
		}
		backend := strings.ToLower(strings.TrimSpace(value))
		switch backend {
		case "", merkleBackendCanonical:
			return merkleBackendCanonical
		case merkleBackendIAVL:
			return merkleBackendIAVL
		case merkleBackendTreap:
			return merkleBackendTreap
		default:
			panic(fmt.Sprintf("run config field %q has unknown backend %q", runConfigMerkle, value))
		}
	}

	// Legacy fallback for older run files.
	if common.BoolOrDefault(runConfig, runConfigMerkleUseTreap, false) {
		return merkleBackendTreap
	}
	if common.BoolOrDefault(runConfig, runConfigMerkleUseIAVL, false) {
		return merkleBackendIAVL
	}
	return merkleBackendCanonical
}

func (e *Exec) newMerkleTreeFromMap(kv map[string]string) merkle.Tree {
	return newMerkleTreeFromMap(kv, e.RunConfig)
}

func (e *Exec) ensureWorkingMerkle() {
	e.workingState.EnsureMerkle(e.newMerkleTreeFromMap)
}

func (e *Exec) ensureStableMerkle() {
	e.stableState.EnsureMerkle(e.newMerkleTreeFromMap)
}
