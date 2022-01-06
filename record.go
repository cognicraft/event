package event

import (
	"encoding/json"
	"time"
)

type Record struct {
	ID                string          `json:"id,omitempty"`        // the unique id of the event
	StreamID          string          `json:"stream-id"`           // the id of the current stream
	StreamIndex       uint64          `json:"stream-index"`        // the index of the event within the current stream
	OriginStreamID    string          `json:"origin-stream-id"`    // the id of the origin stream
	OriginStreamIndex uint64          `json:"origin-stream-index"` // the index of the event within the origin stream
	RecordedOn        time.Time       `json:"recorded-on"`         // the time the event was first recorded
	Type              string          `json:"type"`                // the type of the event
	Data              json.RawMessage `json:"data,omitempty"`      // the data of the event
	Metadata          json.RawMessage `json:"metadata,omitempty"`  // metadata to the event
}

func Encode(v interface{}) (json.RawMessage, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func Decode(data json.RawMessage, v interface{}) error {
	return json.Unmarshal(data, v)
}

type Records []Record

func (rs Records) Stream() RecordStream {
	out := make(chan Record, len(rs))
	go func() {
		defer close(out)
		for _, r := range rs {
			out <- r
		}
	}()
	return out
}

type RecordStream <-chan Record

func (rs RecordStream) Records() Records {
	var out Records
	for r := range rs {
		out = append(out, r)
	}
	return out
}

func Chunked(in RecordStream, batchSize int, timeout time.Duration) <-chan Records {
	out := make(chan Records)

	go func() {
		defer close(out)

		var current Records
		dispatch := func() {
			if len(current) > 0 {
				out <- current
				current = nil
			}
		}

		for {
			select {
			case <-time.After(timeout):
				dispatch()
			case r, ok := <-in:
				if !ok {
					dispatch()
					return
				}
				current = append(current, r)
				if len(current) >= batchSize {
					dispatch()
				}
			}
		}
	}()

	return out
}
