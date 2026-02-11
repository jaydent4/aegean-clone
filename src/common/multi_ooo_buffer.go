package common

import "sync"

type multiOOOViewState[T any] struct {
	buffer *OOOBuffer[T]
	seqs   map[int]struct{}
}

// MultiOOOBuffer stores out-of-order messages across (view, seq) and
// prioritizes retrieval by highest view, then earliest sequence.
type MultiOOOBuffer[T any] struct {
	mu     sync.Mutex
	byView map[int]*multiOOOViewState[T]
}

func NewMultiOOOBuffer[T any]() *MultiOOOBuffer[T] {
	return &MultiOOOBuffer[T]{
		byView: make(map[int]*multiOOOViewState[T]),
	}
}

func (b *MultiOOOBuffer[T]) Add(view int, seqNum int, msg T) {
	b.mu.Lock()
	state, ok := b.byView[view]
	if !ok {
		state = &multiOOOViewState[T]{
			buffer: NewOOOBuffer[T](),
			seqs:   make(map[int]struct{}),
		}
		b.byView[view] = state
	}
	state.seqs[seqNum] = struct{}{}
	b.mu.Unlock()

	state.buffer.Add(seqNum, msg)
}

// PeekNext returns the highest view and earliest sequence currently buffered.
func (b *MultiOOOBuffer[T]) PeekNext() (int, int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peekNextLocked()
}

func (b *MultiOOOBuffer[T]) Pop(view int, seqNum int) []T {
	b.mu.Lock()
	state, ok := b.byView[view]
	b.mu.Unlock()
	if !ok {
		return nil
	}

	msgs := state.buffer.Pop(seqNum)
	if len(msgs) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok = b.byView[view]
	if !ok {
		return msgs
	}
	delete(state.seqs, seqNum)
	if len(state.seqs) == 0 {
		delete(b.byView, view)
	}
	return msgs
}

func (b *MultiOOOBuffer[T]) PopNext() (int, int, []T, bool) {
	view, seqNum, ok := b.PeekNext()
	if !ok {
		return 0, 0, nil, false
	}
	msgs := b.Pop(view, seqNum)
	if len(msgs) == 0 {
		return 0, 0, nil, false
	}
	return view, seqNum, msgs, true
}

// Drop removes all buffered messages with seq_num <= seqNum across all views.
func (b *MultiOOOBuffer[T]) Drop(seqNum int) {
	b.mu.Lock()
	views := make([]int, 0, len(b.byView))
	for view := range b.byView {
		views = append(views, view)
	}
	b.mu.Unlock()

	for _, view := range views {
		b.mu.Lock()
		state, ok := b.byView[view]
		b.mu.Unlock()
		if !ok {
			continue
		}
		state.buffer.Drop(seqNum)

		b.mu.Lock()
		state, ok = b.byView[view]
		if !ok {
			b.mu.Unlock()
			continue
		}
		for s := range state.seqs {
			if s <= seqNum {
				delete(state.seqs, s)
			}
		}
		if len(state.seqs) == 0 {
			delete(b.byView, view)
		}
		b.mu.Unlock()
	}
}

func (b *MultiOOOBuffer[T]) Clear() {
	b.mu.Lock()
	b.byView = make(map[int]*multiOOOViewState[T])
	b.mu.Unlock()
}

func (b *MultiOOOBuffer[T]) peekNextLocked() (int, int, bool) {
	if len(b.byView) == 0 {
		return 0, 0, false
	}
	bestView := 0
	foundView := false
	for view := range b.byView {
		if !foundView || view > bestView {
			bestView = view
			foundView = true
		}
	}
	if !foundView {
		return 0, 0, false
	}
	state := b.byView[bestView]
	bestSeq := 0
	foundSeq := false
	for seq := range state.seqs {
		if !foundSeq || seq < bestSeq {
			bestSeq = seq
			foundSeq = true
		}
	}
	if !foundSeq {
		return 0, 0, false
	}
	return bestView, bestSeq, true
}
