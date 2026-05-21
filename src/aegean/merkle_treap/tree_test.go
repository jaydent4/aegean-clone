package merkle_treap

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestTreapSetExistingKeyMatchesRebuild(t *testing.T) {
	base := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
	}
	tree := NewTreeFromMap(base)
	tree.Set("c", "30")

	expected := NewTreeFromMap(map[string]string{
		"a": "1",
		"b": "2",
		"c": "30",
		"d": "4",
	})
	if tree.Root() != expected.Root() {
		t.Fatalf("expected update root to match rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
}

func TestTreapIncrementalInsertDeleteMatchesRebuild(t *testing.T) {
	tree := NewTreeFromMap(map[string]string{
		"b": "2",
		"d": "4",
	})
	tree.Set("a", "1")
	tree.Set("c", "3")
	tree.Delete("b")

	expected := NewTreeFromMap(map[string]string{
		"a": "1",
		"c": "3",
		"d": "4",
	})
	if tree.Root() != expected.Root() {
		t.Fatalf("expected insert/delete root to match rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
}

func TestTreapBulkInsertMatchesRebuild(t *testing.T) {
	tree := NewTreeFromMap(map[string]string{
		"b": "2",
		"d": "4",
		"f": "6",
	})
	pending := map[string]string{
		"a": "1",
		"c": "3",
		"e": "5",
	}
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	tree.SetNewKeysSorted(keys, pending)

	expected := NewTreeFromMap(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
		"e": "5",
		"f": "6",
	})
	if tree.Root() != expected.Root() {
		t.Fatalf("expected bulk insert root to match rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
	if got := tree.Get("e"); got != "5" {
		t.Fatalf("expected inserted value, got %q", got)
	}
}

func TestTreapBulkInsertWithExistingKeyMatchesRebuild(t *testing.T) {
	tree := NewTreeFromMap(map[string]string{
		"b": "2",
		"d": "4",
	})
	pending := map[string]string{
		"a": "1",
		"b": "20",
		"c": "3",
	}
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	tree.SetNewKeysSorted(keys, pending)

	expected := NewTreeFromMap(map[string]string{
		"a": "1",
		"b": "20",
		"c": "3",
		"d": "4",
	})
	if tree.Root() != expected.Root() {
		t.Fatalf("expected mixed bulk insert root to match rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
	if got := tree.Get("b"); got != "20" {
		t.Fatalf("expected updated value, got %q", got)
	}
}

func TestTreapInsertOrderDoesNotAffectRoot(t *testing.T) {
	expectedState := make(map[string]string)
	for i := 0; i < 256; i++ {
		expectedState[fmt.Sprintf("key-%03d", i)] = fmt.Sprintf("value-%03d", i)
	}
	expected := NewTreeFromMap(expectedState)

	keys := make([]string, 0, len(expectedState))
	for key := range expectedState {
		keys = append(keys, key)
	}
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	tree := NewTreeFromMap(nil)
	for _, key := range keys {
		tree.Set(key, expectedState[key])
	}
	if tree.Root() != expected.Root() {
		t.Fatalf("expected insertion-order independent root, got %s vs %s", tree.Root(), expected.Root())
	}
}

func TestTreapCloneAndDiff(t *testing.T) {
	tree := NewTreeFromMap(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	})
	clone := tree.Clone()
	clone.Set("b", "20")
	clone.Delete("c")
	clone.Set("d", "4")

	if tree.Get("b") != "2" {
		t.Fatalf("expected clone mutation not to affect original")
	}
	if tree.Root() == clone.Root() {
		t.Fatalf("expected clone root to diverge after mutations")
	}

	updates, deletes := clone.DiffFromLeafHashes(tree.LeafHashes())
	if got := updates["b"]; got != "20" {
		t.Fatalf("expected update for b, got %q", got)
	}
	if got := updates["d"]; got != "4" {
		t.Fatalf("expected update for d, got %q", got)
	}
	if len(deletes) != 1 || deletes[0] != "c" {
		t.Fatalf("expected delete for c, got %v", deletes)
	}
}
