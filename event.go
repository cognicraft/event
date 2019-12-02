package event

type Event interface{}

type Events []Event

type EventStream <-chan Event
