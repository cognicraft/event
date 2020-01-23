package event

import "testing"

func TestChangeRecorder(t *testing.T) {
	cr := NewChangeRecorder()
	if got := len(cr.Changes()); got != 0 {
		t.Errorf("want: %d, got: %d", 0, got)
	}
	cr.Record("test")
	if got := len(cr.Changes()); got != 1 {
		t.Errorf("want: %d, got: %d", 1, got)
	}
	if got := cr.Changes()[0]; got != "test" {
		t.Errorf("want: %v, got: %v", "test", got)
	}
	cr.Record("next")
	if got := len(cr.Changes()); got != 2 {
		t.Errorf("want: %d, got: %d", 2, got)
	}
	if got := cr.Changes()[1]; got != "next" {
		t.Errorf("want: %v, got: %v", "next", got)
	}
	cr.ClearChanges()
	if got := len(cr.Changes()); got != 0 {
		t.Errorf("want: %d, got: %d", 0, got)
	}
}
