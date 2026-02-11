package common

import "testing"

func TestMultiOOOBufferOrdersByViewThenSeq(t *testing.T) {
	buf := NewMultiOOOBuffer[string]()
	buf.Add(1, 2, "v1s2")
	buf.Add(2, 3, "v2s3")
	buf.Add(2, 1, "v2s1")

	view, seq, msgs, ok := buf.PopNext()
	if !ok || view != 2 || seq != 1 || len(msgs) != 1 || msgs[0] != "v2s1" {
		t.Fatalf("unexpected first pop: view=%d seq=%d msgs=%v ok=%t", view, seq, msgs, ok)
	}

	view, seq, msgs, ok = buf.PopNext()
	if !ok || view != 2 || seq != 3 || len(msgs) != 1 || msgs[0] != "v2s3" {
		t.Fatalf("unexpected second pop: view=%d seq=%d msgs=%v ok=%t", view, seq, msgs, ok)
	}

	view, seq, msgs, ok = buf.PopNext()
	if !ok || view != 1 || seq != 2 || len(msgs) != 1 || msgs[0] != "v1s2" {
		t.Fatalf("unexpected third pop: view=%d seq=%d msgs=%v ok=%t", view, seq, msgs, ok)
	}
}

func TestMultiOOOBufferDropAcrossViews(t *testing.T) {
	buf := NewMultiOOOBuffer[int]()
	buf.Add(1, 1, 11)
	buf.Add(1, 3, 13)
	buf.Add(2, 2, 22)

	buf.Drop(2)

	view, seq, msgs, ok := buf.PopNext()
	if !ok || view != 1 || seq != 3 || len(msgs) != 1 || msgs[0] != 13 {
		t.Fatalf("unexpected pop after drop: view=%d seq=%d msgs=%v ok=%t", view, seq, msgs, ok)
	}
}
