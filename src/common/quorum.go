package common

import (
	"fmt"
	"log"
	"sync"
)

// QuorumHelper tracks sender sets per request and returns true when quorum is reached
type QuorumHelper struct {
	quorumSize int
	// requestID -> set of senders received so far
	received map[string]map[string]struct{}
	mu       sync.Mutex
}

func NewQuorumHelper(quorumSize int) *QuorumHelper {
	return &QuorumHelper{
		quorumSize: quorumSize,
		received:   make(map[string]map[string]struct{}),
	}
}

// Add records a request sender and returns true if quorum is newly reached
func (q *QuorumHelper) Add(requestID any, sender string) bool {
	key := fmt.Sprintf("%v", requestID)

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.received[key]; !ok {
		q.received[key] = make(map[string]struct{})
	}

	if _, exists := q.received[key][sender]; exists {
		log.Printf("Ignoring duplicate request %s from %s", key, sender)
		return false
	}
	previousCount := len(q.received[key])
	q.received[key][sender] = struct{}{}
	newCount := previousCount + 1

	if newCount == q.quorumSize {
		log.Printf("Quorum reached for request %s", key)
		return true
	}
	if newCount < q.quorumSize {
		log.Printf("Request %s: %d/%d", key, newCount, q.quorumSize)
		return false
	}

	log.Printf("Extra sender for request %s: %d/%d", key, newCount, q.quorumSize)
	return false
}

// TODO: GC, but tricky, because we still need to dedup in case already-quorumed messages arrive
// If we GC too early, the old messages may trigger another quorum
