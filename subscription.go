package event

import (
	"github.com/cognicraft/pubsub"
)

type loadSliceFunc func(streamID string, skip uint64, limit uint64) (*Slice, error)

type subscribeFunc func(topic pubsub.Topic, callback func(topic pubsub.Topic, data interface{})) pubsub.Subscription

type subscription struct {
	batchSize uint64
	subscribe subscribeFunc
	loadSlice loadSliceFunc
	streamID  string
	from      uint64
	done      chan struct{}
	update    chan string
}

func (s *subscription) Records() RecordStream {
	s.done = make(chan struct{})
	s.update = make(chan string)
	out := make(chan Record)
	go func() {
		defer close(out)
		enqeue := func(streamID string, skip uint64, limit uint64) uint64 {
			next := skip
			for {
				select {
				case <-s.done:
					return next
				default:
					slice, err := s.loadSlice(streamID, next, limit)
					if err != nil {
						return next
					}
					for _, e := range slice.Records {
						select {
						case out <- e:
						case <-s.done:
							if e.StreamIndex+1 < slice.Next {
								return e.StreamIndex + 1
							}
							return slice.Next
						}
					}
					next = slice.Next
					if slice.IsEndOfStream {
						return next
					}
				}
			}
		}
		// catch up
		next := enqeue(s.streamID, s.from, s.batchSize)
		// follow
		changes := s.subscribe(topicAppend, s.onAppend)
		defer changes.Cancel()
		for {
			select {
			case <-s.done:
				close(s.update)
				return
			case <-s.update:
				next = enqeue(s.streamID, next, s.batchSize)
			}
		}
	}()
	return out
}

func (s *subscription) On(callback func(r Record)) {
	records := s.Records()
	go func() {
		for e := range records {
			callback(e)
		}
	}()
}

func (s *subscription) Cancel() error {
	if s.done != nil {
		close(s.done)
	}
	return nil
}

func (s *subscription) onAppend(t pubsub.Topic, data interface{}) {
	streamID, _ := data.(string)
	if s.streamID == All || s.streamID == streamID {
		s.update <- streamID
	}
}
