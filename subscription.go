package event

import "github.com/cognicraft/pubsub"

func (s *Store) SubscribeToStream(streamID string) Subscription {
	return &subscription{
		store:    s,
		streamID: streamID,
		from:     0,
	}
}

func (s *Store) SubscribeToStreamFrom(streamID string, version uint64) Subscription {
	return &subscription{
		store:    s,
		streamID: streamID,
		from:     version,
	}
}

func (s *Store) SubscribeToStreamFromCurrent(streamID string) Subscription {
	return &subscription{
		store:    s,
		streamID: streamID,
		from:     s.Version(streamID),
	}
}

type Subscription interface {
	Records() RecordStream
	On(callback func(r Record))
	Cancel() error
}

type subscription struct {
	store    *Store
	streamID string
	from     uint64
	done     chan struct{}
	update   chan string
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
					slice, err := s.store.LoadSlice(streamID, next, limit)
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
		next := enqeue(s.streamID, s.from, s.store.batchSize)
		// follow
		changes := s.store.publisher.Subscribe(topicAppend, s.onAppend)
		defer changes.Cancel()
		for {
			select {
			case <-s.done:
				close(s.update)
				return
			case <-s.update:
				next = enqeue(s.streamID, next, s.store.batchSize)
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

func (s *subscription) onAppend(t pubsub.Topic, args ...interface{}) {
	if len(args) < 0 {
		return
	}
	streamID, _ := args[0].(string)
	if s.streamID == All || s.streamID == streamID {
		s.update <- streamID
	}
}
