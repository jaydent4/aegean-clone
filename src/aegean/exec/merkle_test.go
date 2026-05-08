package exec

import "testing"

func TestMerkleIncrementalSetExistingKeyMatchesCanonicalRebuild(t *testing.T) {
	base := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
	}
	tree := NewMerkleTreeFromMap(base)
	tree.Set("c", "30")

	expected := map[string]string{
		"a": "1",
		"b": "2",
		"c": "30",
		"d": "4",
	}
	canonical := NewMerkleTreeFromMap(expected)

	if tree.Root() != canonical.Root() {
		t.Fatalf("expected incremental update root to match canonical rebuild, got %s vs %s", tree.Root(), canonical.Root())
	}
}

func TestMerkleIncrementalInsertDeleteMatchesCanonicalRebuild(t *testing.T) {
	tree := NewMerkleTreeFromMap(map[string]string{
		"b": "2",
		"d": "4",
	})
	tree.Set("a", "1")
	tree.Set("c", "3")
	tree.Delete("b")

	expected := NewMerkleTreeFromMap(map[string]string{
		"a": "1",
		"c": "3",
		"d": "4",
	})
	if tree.Root() != expected.Root() {
		t.Fatalf("expected insert/delete root to match canonical rebuild, got %s vs %s", tree.Root(), expected.Root())
	}
}
