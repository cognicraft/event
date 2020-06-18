package event

import (
	"fmt"
	"io"
)

type Store interface {
	io.Closer
	Version(streamID string) uint64
	Load(streamID string) RecordStream
	LoadFrom(streamID string, skip uint64) RecordStream
	LoadSlice(streamID string, skip uint64, limit uint64) (*Slice, error)
	Append(streamID string, expectedVersion uint64, records Records) error
	SubscribeToStream(streamID string) Subscription
	SubscribeToStreamFrom(streamID string, version uint64) Subscription
	SubscribeToStreamFromCurrent(streamID string) Subscription
}

type Slice struct {
	StreamID      string  `json:"stream-id"`
	From          uint64  `json:"from"`
	Next          uint64  `json:"next"`
	Records       Records `json:"records"`
	IsEndOfStream bool    `json:"is-end-of-stream"`
}

type OptimisticConcurrencyError struct {
	Stream   string
	Expected uint64
	Actual   uint64
}

func (e OptimisticConcurrencyError) Error() string {
	return fmt.Sprintf("optimistic-concurrency-error on stream %s expected version %d but actually got %d", e.Stream, e.Expected, e.Actual)
}

type Subscription interface {
	Records() RecordStream
	On(callback func(r Record))
	Cancel() error
}
