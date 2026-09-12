package monitor

import "testing"

func TestStopIsSafeAndIdempotent(t *testing.T) {
	m := New(t.TempDir())
	current, points := m.Snapshot()
	if current.At == "" || len(points) == 0 {
		t.Fatalf("initial snapshot = %#v, points=%#v", current, points)
	}
	m.Stop()
	m.Stop()
}
