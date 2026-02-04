// Package common contains shared utilities.
// Translates: src_py/common/quorum.py
package common

import (
	"fmt"
	"log"
	"sync"
)

// QuorumHelper tracks sender sets per request and returns true when quorum is reached.
type QuorumHelper struct {
	quorumSize int
	pending    map[string]map[string]struct{}
	completed  map[string]struct{}
	mu         sync.Mutex
}

func NewQuorumHelper(quorumSize int) *QuorumHelper {
	return &QuorumHelper{
		quorumSize: quorumSize,
		pending:    make(map[string]map[string]struct{}),
		completed:  make(map[string]struct{}),
	}
}

// Add records a request sender and returns true if quorum is newly reached.
func (q *QuorumHelper) Add(requestID any, sender string) bool {
	key := fmt.Sprintf("%v", requestID)

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, done := q.completed[key]; done {
		log.Printf("Ignoring duplicate request %s from %s", key, sender)
		return false
	}

	if _, ok := q.pending[key]; !ok {
		q.pending[key] = make(map[string]struct{})
	}
	q.pending[key][sender] = struct{}{}

	if len(q.pending[key]) >= q.quorumSize {
		q.completed[key] = struct{}{}
		delete(q.pending, key)
		log.Printf("Quorum reached for request %s", key)
		return true
	}

	log.Printf("Request %s: %d/%d", key, len(q.pending[key]), q.quorumSize)
	return false
}
