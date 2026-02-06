package common

import "sync"

// OOOBuffer stores out-of-order messages by sequence number
type OOOBuffer[T any] struct {
	mu    sync.Mutex
	items map[int][]T
}

func NewOOOBuffer[T any]() *OOOBuffer[T] {
	return &OOOBuffer[T]{
		items: make(map[int][]T),
	}
}

// Add stores a message for the given sequence number
func (b *OOOBuffer[T]) Add(seqNum int, msg T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.items[seqNum] = append(b.items[seqNum], msg)
}

// Pop returns and removes all messages for the given sequence number
func (b *OOOBuffer[T]) Pop(seqNum int) []T {
	b.mu.Lock()
	defer b.mu.Unlock()

	msgs := b.items[seqNum]
	if len(msgs) == 0 {
		return nil
	}

	delete(b.items, seqNum)
	return msgs
}

// Drop removes all buffered messages with seq_num <= seqNum
func (b *OOOBuffer[T]) Drop(seqNum int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for key := range b.items {
		if key <= seqNum {
			delete(b.items, key)
		}
	}
}
