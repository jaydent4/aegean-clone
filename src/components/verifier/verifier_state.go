package verifier

// VerifyState tracks verifier state by sequence number
type VerifyState struct {
	// seq_num -> { token -> set(exec_ids) }
	tokens map[int]map[string]map[string]struct{}
	// seq_num -> committed token (or empty string)
	committed map[int]string
	// seq_num -> prev_hash from tokens
	prevHashes map[int]string
}

func NewVerifyState() *VerifyState {
	return &VerifyState{
		tokens:     make(map[int]map[string]map[string]struct{}),
		committed:  make(map[int]string),
		prevHashes: make(map[int]string),
	}
}

func (s *VerifyState) HasCommitted(seqNum int) bool {
	_, ok := s.committed[seqNum]
	return ok
}

func (s *VerifyState) CommittedToken(seqNum int) (string, bool) {
	value, ok := s.committed[seqNum]
	return value, ok
}

func (s *VerifyState) SetCommitted(seqNum int, token string) {
	s.committed[seqNum] = token
}

func (s *VerifyState) RecordToken(seqNum int, token string, execID string, prevHash string) {
	if _, ok := s.tokens[seqNum]; !ok {
		s.tokens[seqNum] = make(map[string]map[string]struct{})
	}
	if _, ok := s.tokens[seqNum][token]; !ok {
		s.tokens[seqNum][token] = make(map[string]struct{})
	}
	s.tokens[seqNum][token][execID] = struct{}{}
	s.prevHashes[seqNum] = prevHash
}

func (s *VerifyState) TokenCounts(seqNum int) map[string]map[string]struct{} {
	return s.tokens[seqNum]
}

func (s *VerifyState) DeleteTokens(seqNum int) {
	delete(s.tokens, seqNum)
}
