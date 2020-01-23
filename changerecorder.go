package event

import "sync"

func NewChangeRecorder() *ChangeRecorder {
	return &ChangeRecorder{}
}

// ChangeRecorder can be used to represent a unit of work within an event sourced system.
type ChangeRecorder struct {
	mu      sync.RWMutex
	changes Events
}

// Record will add an event to the list of pending changes.
func (cr *ChangeRecorder) Record(e Event) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.changes = append(cr.changes, e)
}

// Changes retrieves all currently pending changes.
func (cr *ChangeRecorder) Changes() Events {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.changes
}

// ClearChanges will remove all pending chnages from the underlying data structure.
func (cr *ChangeRecorder) ClearChanges() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.changes = nil
}
