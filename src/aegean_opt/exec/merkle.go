package exec

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
)

// We would like this merkle tree implementation to be: deterministic, parallelized for performance
// and updated incrementally
type MerkleTree struct {
	kv         map[string]string
	leafHashes map[string]string
	sortedKeys []string
	keyIndex   map[string]int
	levels     [][]string
	root       string
}

const (
	merkleHashWorkers       = 4
	merkleParallelPairFloor = 8
	merkleParallelLeafFloor = 64
)

func NewMerkleTreeFromMap(kv map[string]string) *MerkleTree {
	tree := &MerkleTree{
		kv:         make(map[string]string),
		leafHashes: make(map[string]string),
		keyIndex:   make(map[string]int),
		root:       strings.Repeat("0", 64),
	}
	for key, value := range kv {
		tree.kv[key] = value
	}
	tree.rebuildFromKV()
	return tree
}

func (t *MerkleTree) Clone() *MerkleTree {
	if t == nil {
		return NewMerkleTreeFromMap(nil)
	}
	clone := &MerkleTree{
		kv:         make(map[string]string, len(t.kv)),
		leafHashes: make(map[string]string, len(t.leafHashes)),
		sortedKeys: append([]string(nil), t.sortedKeys...),
		keyIndex:   make(map[string]int, len(t.keyIndex)),
		levels:     make([][]string, len(t.levels)),
		root:       t.root,
	}
	for key, value := range t.kv {
		clone.kv[key] = value
	}
	for key, hash := range t.leafHashes {
		clone.leafHashes[key] = hash
	}
	for key, idx := range t.keyIndex {
		clone.keyIndex[key] = idx
	}
	for i := range t.levels {
		clone.levels[i] = append([]string(nil), t.levels[i]...)
	}
	return clone
}

func (t *MerkleTree) Root() string {
	if t == nil {
		return strings.Repeat("0", 64)
	}
	return t.root
}

func (t *MerkleTree) Get(key string) string {
	if t == nil {
		return ""
	}
	return t.kv[key]
}

func (t *MerkleTree) Set(key, value string) {
	if t == nil {
		return
	}
	if idx, ok := t.keyIndex[key]; ok {
		t.kv[key] = value
		leaf := hashHex("leaf|" + key + "|" + value)
		if t.leafHashes[key] == leaf {
			return
		}
		t.leafHashes[key] = leaf
		if len(t.levels) == 0 {
			t.levels = [][]string{{leaf}}
			t.root = leaf
			return
		}
		t.levels[0][idx] = leaf
		t.recomputePath(idx)
		return
	}

	t.kv[key] = value
	leaf := hashHex("leaf|" + key + "|" + value)
	t.leafHashes[key] = leaf
	insertAt := sort.SearchStrings(t.sortedKeys, key)
	t.insertLeafAt(insertAt, key, leaf)
}

func (t *MerkleTree) Delete(key string) {
	if t == nil {
		return
	}
	idx, ok := t.keyIndex[key]
	if !ok {
		return
	}
	delete(t.kv, key)
	delete(t.leafHashes, key)
	t.removeLeafAt(idx)
}

func (t *MerkleTree) SnapshotMap() map[string]string {
	out := make(map[string]string, len(t.kv))
	for key, value := range t.kv {
		out[key] = value
	}
	return out
}

func (t *MerkleTree) LeafHashes() map[string]string {
	out := make(map[string]string, len(t.leafHashes))
	for key, hash := range t.leafHashes {
		out[key] = hash
	}
	return out
}

func (t *MerkleTree) DiffFromLeafHashes(remoteLeafHashes map[string]string) (map[string]string, []string) {
	if remoteLeafHashes == nil {
		remoteLeafHashes = map[string]string{}
	}
	updates := make(map[string]string)
	deletes := make([]string, 0)
	for key, localHash := range t.leafHashes {
		remoteHash, ok := remoteLeafHashes[key]
		if !ok || remoteHash != localHash {
			updates[key] = t.kv[key]
		}
	}
	for key := range remoteLeafHashes {
		if _, ok := t.kv[key]; !ok {
			deletes = append(deletes, key)
		}
	}
	sort.Strings(deletes)
	return updates, deletes
}

func (t *MerkleTree) rebuildFromKV() {
	t.leafHashes = make(map[string]string, len(t.kv))
	t.keyIndex = make(map[string]int, len(t.kv))
	t.sortedKeys = t.sortedKeys[:0]
	t.levels = t.levels[:0]
	if len(t.kv) == 0 {
		t.root = strings.Repeat("0", 64)
		return
	}

	keys := make([]string, 0, len(t.kv))
	for key := range t.kv {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	t.sortedKeys = keys

	leafLevel := make([]string, len(keys))
	if len(keys) >= merkleParallelLeafFloor {
		t.hashLeafLevelParallel(keys, leafLevel)
	} else {
		for i, key := range keys {
			leaf := hashHex("leaf|" + key + "|" + t.kv[key])
			t.leafHashes[key] = leaf
			leafLevel[i] = leaf
		}
	}
	for i, key := range keys {
		t.keyIndex[key] = i
	}
	t.rebuildLevelsFromLeafLevel(leafLevel)
}

func (t *MerkleTree) rebuildLevelsFromLeafLevel(leafLevel []string) {
	if len(leafLevel) == 0 {
		t.levels = nil
		t.root = strings.Repeat("0", 64)
		return
	}
	t.levels = t.levels[:0]
	t.levels = append(t.levels, append([]string(nil), leafLevel...))
	level := t.levels[0]
	for len(level) > 1 {
		pairCount := (len(level) + 1) / 2
		next := make([]string, pairCount)
		if pairCount >= merkleParallelPairFloor {
			t.hashLevelParallel(level, next)
		} else {
			for i := 0; i < pairCount; i++ {
				left := level[2*i]
				right := left
				if 2*i+1 < len(level) {
					right = level[2*i+1]
				}
				next[i] = hashHex("node|" + left + "|" + right)
			}
		}
		t.levels = append(t.levels, next)
		level = next
	}
	t.root = t.levels[len(t.levels)-1][0]
}

func (t *MerkleTree) hashLevelParallel(level []string, next []string) {
	workerCount := merkleHashWorkers
	if workerCount > len(next) {
		workerCount = len(next)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	var wg sync.WaitGroup
	workCh := make(chan int, len(next))
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pairIdx := range workCh {
				left := level[2*pairIdx]
				right := left
				if 2*pairIdx+1 < len(level) {
					right = level[2*pairIdx+1]
				}
				next[pairIdx] = hashHex("node|" + left + "|" + right)
			}
		}()
	}
	for i := range next {
		workCh <- i
	}
	close(workCh)
	wg.Wait()
}

func (t *MerkleTree) hashLeafLevelParallel(keys []string, leafLevel []string) {
	workerCount := merkleHashWorkers
	if workerCount > len(keys) {
		workerCount = len(keys)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	var wg sync.WaitGroup
	workCh := make(chan int, len(keys))
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				key := keys[idx]
				leaf := hashHex("leaf|" + key + "|" + t.kv[key])
				leafLevel[idx] = leaf
			}
		}()
	}
	for i := range keys {
		workCh <- i
	}
	close(workCh)
	wg.Wait()
	for i, key := range keys {
		t.leafHashes[key] = leafLevel[i]
	}
}

func (t *MerkleTree) recomputePath(leafIdx int) {
	if len(t.levels) == 0 {
		t.root = strings.Repeat("0", 64)
		return
	}
	idx := leafIdx
	for level := 0; level < len(t.levels)-1; level++ {
		cur := t.levels[level]
		parent := t.levels[level+1]
		pairIdx := idx / 2
		left := cur[2*pairIdx]
		right := left
		if 2*pairIdx+1 < len(cur) {
			right = cur[2*pairIdx+1]
		}
		parent[pairIdx] = hashHex("node|" + left + "|" + right)
		idx = pairIdx
	}
	t.root = t.levels[len(t.levels)-1][0]
}

func (t *MerkleTree) insertLeafAt(idx int, key string, leaf string) {
	t.sortedKeys = append(t.sortedKeys, "")
	copy(t.sortedKeys[idx+1:], t.sortedKeys[idx:])
	t.sortedKeys[idx] = key

	if len(t.levels) == 0 {
		t.levels = [][]string{{leaf}}
		t.keyIndex[key] = 0
		t.root = leaf
		return
	}

	leafLevel := append(t.levels[0], "")
	copy(leafLevel[idx+1:], leafLevel[idx:])
	leafLevel[idx] = leaf
	t.reindexFrom(idx)
	t.rebuildLevelsFromLeafLevel(leafLevel)
}

func (t *MerkleTree) removeLeafAt(idx int) {
	removedKey := t.sortedKeys[idx]
	t.sortedKeys = append(t.sortedKeys[:idx], t.sortedKeys[idx+1:]...)
	delete(t.keyIndex, removedKey)
	if len(t.sortedKeys) == 0 {
		t.levels = nil
		t.root = strings.Repeat("0", 64)
		return
	}
	leafLevel := append([]string(nil), t.levels[0][:idx]...)
	leafLevel = append(leafLevel, t.levels[0][idx+1:]...)
	t.reindexFrom(idx)
	t.rebuildLevelsFromLeafLevel(leafLevel)
}

func (t *MerkleTree) reindexFrom(start int) {
	for i := start; i < len(t.sortedKeys); i++ {
		t.keyIndex[t.sortedKeys[i]] = i
	}
}

func hashHex(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])
}
