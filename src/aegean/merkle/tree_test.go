package merkle

import (
	"sort"
	"testing"
)

func TestMerkleIncrementalSetExistingKeyMatchesCanonicalRebuild(t *testing.T) {
	base := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
	}
	tree := NewTreeFromMap(base)
	tree.Set("c", "30")

	expected := map[string]string{
		"a": "1",
		"b": "2",
		"c": "30",
		"d": "4",
	}
	canonical := NewTreeFromMap(expected)

	if tree.Root() != canonical.Root() {
		t.Fatalf("expected incremental update root to match canonical rebuild, got %s vs %s", tree.Root(), canonical.Root())
	}
}

func TestMerkleIncrementalInsertDeleteMatchesCanonicalRebuild(t *testing.T) {
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
		t.Fatalf("expected insert/delete root to match canonical rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
}

func TestMerkleBulkInsertMatchesCanonicalRebuild(t *testing.T) {
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
		t.Fatalf("expected bulk insert root to match canonical rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
	if got := tree.Get("e"); got != "5" {
		t.Fatalf("expected inserted value, got %q", got)
	}
}

func TestMerkleBulkInsertWithExistingKeyMatchesCanonicalRebuild(t *testing.T) {
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
		t.Fatalf("expected mixed bulk insert root to match canonical rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
	if got := tree.Get("b"); got != "20" {
		t.Fatalf("expected updated value, got %q", got)
	}
}

func TestMerkleBulkInsertEmptyTreeMatchesCanonicalRebuild(t *testing.T) {
	tree := NewTreeFromMap(nil)
	pending := map[string]string{
		"b": "2",
		"a": "1",
	}
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	tree.SetNewKeysSorted(keys, pending)

	expected := NewTreeFromMap(pending)
	if tree.Root() != expected.Root() {
		t.Fatalf("expected empty-tree bulk insert root to match canonical rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
}
