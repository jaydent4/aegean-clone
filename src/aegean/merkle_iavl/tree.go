package merkle_iavl

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"aegean/aegean/merkle"

	iavl "github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
)

const iavlCacheSize = 128

var _ merkle.Tree = (*Tree)(nil)

// Tree adapts cosmos/iavl to the exec Merkle tree interface.
type Tree struct {
	kv         map[string]string
	leafHashes map[string]string
	tree       *iavl.MutableTree
	root       string
}

func NewTreeFromMap(kv map[string]string) merkle.Tree {
	tree := &Tree{
		kv:         make(map[string]string, len(kv)),
		leafHashes: make(map[string]string, len(kv)),
		root:       zeroRoot(),
	}
	for key, value := range kv {
		tree.kv[key] = value
	}
	tree.rebuildFromKV()
	return tree
}

func (t *Tree) Clone() merkle.Tree {
	if t == nil {
		return NewTreeFromMap(nil)
	}
	return NewTreeFromMap(t.kv)
}

func (t *Tree) Root() string {
	if t == nil {
		return zeroRoot()
	}
	return t.root
}

func (t *Tree) Get(key string) string {
	if t == nil {
		return ""
	}
	return t.kv[key]
}

func (t *Tree) Set(key, value string) {
	if t == nil {
		return
	}
	if old, ok := t.kv[key]; ok {
		if old == value {
			return
		}
		t.kv[key] = value
		t.leafHashes[key] = merkle.LeafHash(key, value)
		t.mustSet(key, value)
		t.root = t.computeRoot()
		return
	}

	t.kv[key] = value
	t.leafHashes[key] = merkle.LeafHash(key, value)
	// IAVL roots depend on tree shape, so key-set changes rebuild from sorted
	// state to preserve the canonical root contract used by state transfer.
	t.rebuildFromKV()
}

func (t *Tree) SetNewKeysSorted(keys []string, values map[string]string) {
	if t == nil || len(keys) == 0 {
		return
	}

	hasNewKey := false
	seen := make(map[string]struct{}, len(keys))
	sortedKeys := append([]string(nil), keys...)
	sort.Strings(sortedKeys)
	uniqueKeys := make([]string, 0, len(sortedKeys))
	for _, key := range sortedKeys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		uniqueKeys = append(uniqueKeys, key)
		value := values[key]
		if _, ok := t.kv[key]; !ok {
			hasNewKey = true
		}
		t.kv[key] = value
		t.leafHashes[key] = merkle.LeafHash(key, value)
	}
	if hasNewKey {
		t.rebuildFromKV()
		return
	}
	for _, key := range uniqueKeys {
		t.mustSet(key, values[key])
	}
	t.root = t.computeRoot()
}

func (t *Tree) Delete(key string) {
	if t == nil {
		return
	}
	if _, ok := t.kv[key]; !ok {
		return
	}
	delete(t.kv, key)
	delete(t.leafHashes, key)
	t.rebuildFromKV()
}

func (t *Tree) SnapshotMap() map[string]string {
	out := make(map[string]string, len(t.kv))
	for key, value := range t.kv {
		out[key] = value
	}
	return out
}

func (t *Tree) LeafHashes() map[string]string {
	out := make(map[string]string, len(t.leafHashes))
	for key, hash := range t.leafHashes {
		out[key] = hash
	}
	return out
}

func (t *Tree) DiffFromLeafHashes(remoteLeafHashes map[string]string) (map[string]string, []string) {
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

func (t *Tree) rebuildFromKV() {
	t.tree = iavl.NewMutableTree(iavldb.NewMemDB(), iavlCacheSize, true, iavl.NewNopLogger())
	t.leafHashes = make(map[string]string, len(t.kv))
	if len(t.kv) == 0 {
		t.root = zeroRoot()
		return
	}

	keys := make([]string, 0, len(t.kv))
	for key := range t.kv {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := t.kv[key]
		t.leafHashes[key] = merkle.LeafHash(key, value)
		t.mustSet(key, value)
	}
	t.root = t.computeRoot()
}

func (t *Tree) mustSet(key, value string) {
	if _, err := t.tree.Set(iavlKey(key), []byte(value)); err != nil {
		panic(fmt.Sprintf("merkle_iavl: set key %q: %v", key, err))
	}
}

func (t *Tree) computeRoot() string {
	if t == nil || len(t.kv) == 0 {
		return zeroRoot()
	}
	root := t.tree.WorkingHash()
	if len(root) == 0 {
		return zeroRoot()
	}
	return hex.EncodeToString(root)
}

func iavlKey(key string) []byte {
	return append([]byte{0}, []byte(key)...)
}

func zeroRoot() string {
	return strings.Repeat("0", 64)
}
