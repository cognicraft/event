package event

func NewChangeRecorder() *ChangeRecorder {
	return &ChangeRecorder{
		changes: Events{},
	}
}

type ChangeRecorder struct {
	changes Events
}

func (cr *ChangeRecorder) Record(e Event) {
	cr.changes = append(cr.changes, e)
}

func (cr *ChangeRecorder) Changes() Events {
	return cr.changes
}

func (cr *ChangeRecorder) ClearChanges() {
	cr.changes = Events{}
}
