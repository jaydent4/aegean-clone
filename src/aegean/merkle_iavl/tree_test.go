package merkle_iavl

import "testing"

func TestIAVLSetExistingKeyMatchesRebuild(t *testing.T) {
	base := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}
	tree := NewTreeFromMap(base)
	tree.Set("b", "20")

	expected := NewTreeFromMap(map[string]string{
		"a": "1",
		"b": "20",
		"c": "3",
	})
	if tree.Root() != expected.Root() {
		t.Fatalf("root mismatch after update: got %s want %s", tree.Root(), expected.Root())
	}
}

func TestIAVLSetNewKeysMatchesRebuild(t *testing.T) {
	tree := NewTreeFromMap(map[string]string{
		"b": "2",
		"d": "4",
	})
	pending := map[string]string{
		"a": "1",
		"c": "3",
		"e": "5",
	}
	tree.SetNewKeysSorted([]string{"a", "c", "e"}, pending)

	expected := NewTreeFromMap(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
		"e": "5",
	})
	if tree.Root() != expected.Root() {
		t.Fatalf("root mismatch after bulk insert: got %s want %s", tree.Root(), expected.Root())
	}
}

func TestIAVLDiffFromLeafHashes(t *testing.T) {
	tree := NewTreeFromMap(map[string]string{
		"a": "1",
		"b": "2",
	})
	remote := NewTreeFromMap(map[string]string{
		"a": "1",
		"c": "3",
	})

	updates, deletes := tree.DiffFromLeafHashes(remote.LeafHashes())
	if len(updates) != 1 || updates["b"] != "2" {
		t.Fatalf("updates = %#v, want b=2", updates)
	}
	if len(deletes) != 1 || deletes[0] != "c" {
		t.Fatalf("deletes = %#v, want [c]", deletes)
	}
}
