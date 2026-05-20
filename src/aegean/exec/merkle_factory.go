package exec

import (
	"aegean/aegean/merkle"
	"aegean/aegean/merkle_iavl"
	"aegean/common"
)

const runConfigMerkleUseIAVL = "merkle_use_iavl"

func newMerkleTreeFromMap(kv map[string]string, runConfig map[string]any) merkle.Tree {
	if common.BoolOrDefault(runConfig, runConfigMerkleUseIAVL, false) {
		return merkle_iavl.NewTreeFromMap(kv)
	}
	return merkle.NewTreeFromMap(kv)
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
