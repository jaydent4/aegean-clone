package merkle_treap

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"aegean/aegean/merkle"
)

var _ merkle.Tree = (*Tree)(nil)

type node struct {
	key      string
	value    string
	leafHash string
	priority string
	hash     string
	left     *node
	right    *node
}

// Tree is a deterministic Merkle treap. Keys are ordered by string key and
// heap priority is derived from hash(key), making the shape canonical for a
// final key set while keeping inserts and updates incremental.
type Tree struct {
	kv         map[string]string
	leafHashes map[string]string
	rootNode   *node
	root       string
}

func NewTreeFromMap(kv map[string]string) merkle.Tree {
	tree := &Tree{
		kv:         make(map[string]string, len(kv)),
		leafHashes: make(map[string]string, len(kv)),
		root:       zeroRoot(),
	}
	if len(kv) == 0 {
		return tree
	}

	keys := make([]string, 0, len(kv))
	for key, value := range kv {
		tree.kv[key] = value
		tree.leafHashes[key] = merkle.LeafHash(key, value)
		keys = append(keys, key)
	}
	sort.Strings(keys)
	nodes := make([]*node, 0, len(keys))
	for _, key := range keys {
		nodes = append(nodes, newNode(key, tree.kv[key]))
	}
	tree.rootNode = buildCartesian(nodes)
	tree.root = recomputeAll(tree.rootNode)
	return tree
}

func (t *Tree) Clone() merkle.Tree {
	if t == nil {
		return NewTreeFromMap(nil)
	}
	clone := &Tree{
		kv:         make(map[string]string, len(t.kv)),
		leafHashes: make(map[string]string, len(t.leafHashes)),
		rootNode:   cloneNode(t.rootNode),
		root:       t.root,
	}
	for key, value := range t.kv {
		clone.kv[key] = value
	}
	for key, hash := range t.leafHashes {
		clone.leafHashes[key] = hash
	}
	return clone
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
	if old, ok := t.kv[key]; ok && old == value {
		return
	}
	leafHash := merkle.LeafHash(key, value)
	if _, ok := t.kv[key]; ok {
		t.kv[key] = value
		t.leafHashes[key] = leafHash
		t.rootNode = update(t.rootNode, key, value, leafHash)
		t.root = rootHash(t.rootNode)
		return
	}

	t.kv[key] = value
	t.leafHashes[key] = leafHash
	t.rootNode = insert(t.rootNode, newNodeWithLeaf(key, value, leafHash))
	t.root = rootHash(t.rootNode)
}

func (t *Tree) SetNewKeysSorted(keys []string, values map[string]string) {
	if t == nil || len(keys) == 0 {
		return
	}
	for _, key := range keys {
		t.Set(key, values[key])
	}
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
	t.rootNode = deleteNode(t.rootNode, key)
	t.root = rootHash(t.rootNode)
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

func buildCartesian(nodes []*node) *node {
	var stack []*node
	for _, cur := range nodes {
		var last *node
		for len(stack) > 0 && higherPriority(cur, stack[len(stack)-1]) {
			last = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
		}
		cur.left = last
		if len(stack) > 0 {
			stack[len(stack)-1].right = cur
		}
		stack = append(stack, cur)
	}
	if len(stack) == 0 {
		return nil
	}
	return stack[0]
}

func insert(root *node, item *node) *node {
	if root == nil {
		return item
	}
	if item.key < root.key {
		root.left = insert(root.left, item)
		if higherPriority(root.left, root) {
			return rotateRight(root)
		}
		root.recompute()
		return root
	}
	if item.key > root.key {
		root.right = insert(root.right, item)
		if higherPriority(root.right, root) {
			return rotateLeft(root)
		}
		root.recompute()
		return root
	}
	root.value = item.value
	root.leafHash = item.leafHash
	root.recompute()
	return root
}

func update(root *node, key, value, leafHash string) *node {
	if root == nil {
		return nil
	}
	if key < root.key {
		root.left = update(root.left, key, value, leafHash)
		root.recompute()
		return root
	}
	if key > root.key {
		root.right = update(root.right, key, value, leafHash)
		root.recompute()
		return root
	}
	root.value = value
	root.leafHash = leafHash
	root.recompute()
	return root
}

func deleteNode(root *node, key string) *node {
	if root == nil {
		return nil
	}
	if key < root.key {
		root.left = deleteNode(root.left, key)
		root.recompute()
		return root
	}
	if key > root.key {
		root.right = deleteNode(root.right, key)
		root.recompute()
		return root
	}
	return merge(root.left, root.right)
}

func merge(left, right *node) *node {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if higherPriority(left, right) {
		left.right = merge(left.right, right)
		left.recompute()
		return left
	}
	right.left = merge(left, right.left)
	right.recompute()
	return right
}

func rotateRight(root *node) *node {
	next := root.left
	root.left = next.right
	next.right = root
	root.recompute()
	next.recompute()
	return next
}

func rotateLeft(root *node) *node {
	next := root.right
	root.right = next.left
	next.left = root
	root.recompute()
	next.recompute()
	return next
}

func higherPriority(a, b *node) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	if a.priority != b.priority {
		return a.priority < b.priority
	}
	return a.key < b.key
}

func newNode(key, value string) *node {
	return newNodeWithLeaf(key, value, merkle.LeafHash(key, value))
}

func newNodeWithLeaf(key, value, leafHash string) *node {
	n := &node{
		key:      key,
		value:    value,
		leafHash: leafHash,
		priority: hashHex("priority|" + key),
	}
	n.recompute()
	return n
}

func cloneNode(n *node) *node {
	if n == nil {
		return nil
	}
	return &node{
		key:      n.key,
		value:    n.value,
		leafHash: n.leafHash,
		priority: n.priority,
		hash:     n.hash,
		left:     cloneNode(n.left),
		right:    cloneNode(n.right),
	}
}

func recomputeAll(n *node) string {
	if n == nil {
		return zeroRoot()
	}
	recomputeAll(n.left)
	recomputeAll(n.right)
	n.recompute()
	return n.hash
}

func (n *node) recompute() {
	if n == nil {
		return
	}
	n.hash = hashHex("treap_node|" + n.key + "|" + n.leafHash + "|" + rootHash(n.left) + "|" + rootHash(n.right))
}

func rootHash(n *node) string {
	if n == nil {
		return zeroRoot()
	}
	return n.hash
}

func hashHex(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])
}

func zeroRoot() string {
	return strings.Repeat("0", 64)
}
