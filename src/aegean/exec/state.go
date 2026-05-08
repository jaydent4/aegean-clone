package exec

type State struct {
	KVStore    map[string]string
	Merkle     *MerkleTree
	MerkleRoot string
	SeqNum     int
	PrevHash   string
	Verified   bool
}

func (s *State) EnsureMerkle() {
	if s.Merkle == nil {
		if s.KVStore == nil {
			s.KVStore = make(map[string]string)
		}
		s.Merkle = NewMerkleTreeFromMap(s.KVStore)
		s.MerkleRoot = s.Merkle.Root()
		return
	}
	// Keep KVStore and Merkle initialized; hot paths rely on incremental Merkle updates.
	if s.KVStore == nil {
		s.KVStore = s.Merkle.SnapshotMap()
	}
	s.MerkleRoot = s.Merkle.Root()
}
