package event

type Event interface{}

type Events []Event

func (es Events) Stream() EventStream {
	out := make(chan Event, len(es))
	go func() {
		defer close(out)
		for _, e := range es {
			out <- e
		}
	}()
	return out
}

type EventStream <-chan Event
