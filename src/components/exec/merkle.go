package exec

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
)

type MerkleTree struct {
	kv         map[string]string
	leafHashes map[string]string
	root       string
}

const (
	merkleHashWorkers       = 4
	merkleParallelPairFloor = 8
)

func NewMerkleTreeFromMap(kv map[string]string) *MerkleTree {
	tree := &MerkleTree{
		kv:         make(map[string]string),
		leafHashes: make(map[string]string),
		root:       strings.Repeat("0", 64),
	}
	for key, value := range kv {
		tree.kv[key] = value
	}
	tree.rebuild()
	return tree
}

func (t *MerkleTree) Clone() *MerkleTree {
	if t == nil {
		return NewMerkleTreeFromMap(nil)
	}
	return NewMerkleTreeFromMap(t.kv)
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
	t.kv[key] = value
	t.rebuild()
}

func (t *MerkleTree) Delete(key string) {
	if t == nil {
		return
	}
	delete(t.kv, key)
	t.rebuild()
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

func (t *MerkleTree) rebuild() {
	t.leafHashes = make(map[string]string, len(t.kv))
	if len(t.kv) == 0 {
		t.root = strings.Repeat("0", 64)
		return
	}

	keys := make([]string, 0, len(t.kv))
	for key := range t.kv {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	level := make([]string, 0, len(keys))
	for _, key := range keys {
		leaf := hashHex("leaf|" + key + "|" + t.kv[key])
		t.leafHashes[key] = leaf
		level = append(level, leaf)
	}

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
		level = next
	}

	t.root = level[0]
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

func hashHex(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])
}
