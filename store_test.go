package event

import (
	"encoding/json"
	"testing"
)

func TestStore(t *testing.T) {
	s, err := NewStore(":memory:")
	// s, err := NewStore("test.db")
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}
	defer s.Close()
	v := s.Version("foo")
	if v != 0 {
		t.Errorf("want: %d, got: %d", 0, v)
	}
	err = s.Append("foo", 0, Records{
		{ID: "1", Type: "test", Data: json.RawMessage(`{}`)},
	})
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}

	err = s.Append("foo", 0, Records{
		{ID: "2", Type: "test", Data: json.RawMessage(`{}`)},
	})
	if err == nil {
		t.Errorf("expected optimistic concurrency error")
	}
	t.Logf("%v", err)

	err = s.Append("foo", 1, Records{
		{ID: "2", Type: "test", Data: json.RawMessage(`{}`)},
		{ID: "3", Type: "test", Data: json.RawMessage(`{}`)},
		{ID: "4", Type: "test", Data: json.RawMessage(`{}`)},
	})
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}

	err = s.Append("bar", 0, Records{
		{ID: "1", Type: "test", Data: json.RawMessage(`{}`)},
		{ID: "2", Type: "test", Data: json.RawMessage(`{}`)},
	})
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}

	slice, err := s.LoadSlice("foo", 0, 1)
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}
	if slice.StreamID != "foo" {
		t.Errorf("want: %s, got: %s", "foo", slice.StreamID)
	}
	if slice.From != 0 {
		t.Errorf("want: %d, got: %d", 0, slice.From)
	}
	if slice.Next != 1 {
		t.Errorf("want: %d, got: %d", 1, slice.Next)
	}
	if len(slice.Records) != 1 {
		t.Errorf("want: %d, got: %d", 1, len(slice.Records))
	}
	if slice.IsEndOfStream {
		t.Errorf("expected to have more events")
	}

	var count int
	for r := range s.Load("foo") {
		t.Logf("%v", r)
		count++
	}
	if count != 4 {
		t.Errorf("expected count to be %d, bug got: %d", 4, count)
	}
	// t.Fail()
}
