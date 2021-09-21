package event

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
)

func TestBasicStore(t *testing.T) {
	s, err := NewBasicStore(":memory:")
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}
	defer s.Close()
	exersizeStore(t, s)
}

func TestChunkedStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Fatalf("could not create directory: %v", err)
	}
	defer os.RemoveAll(dir)

	s, err := NewChunkedStore(dir + "?chunk-size=2")
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}
	defer s.Close()
	exersizeStore(t, s)
}

func exersizeStore(t *testing.T, s Store) {
	v := s.Version("foo")
	if v != 0 {
		t.Errorf("want: %d, got: %d", 0, v)
	}
	err := s.Append("foo", 0, Records{
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
		t.Fatalf("expected no error, but got: %v", err)
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

	{
		recs := s.Load("foo").Records()
		if count := len(recs); count != 4 {
			t.Errorf("expected count to be %d, but got: %d", 4, count)
		}
		exp := Records{
			{StreamID: "foo", StreamIndex: 0, OriginStreamID: "foo", OriginStreamIndex: 0, ID: "1", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: "foo", StreamIndex: 1, OriginStreamID: "foo", OriginStreamIndex: 1, ID: "2", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: "foo", StreamIndex: 2, OriginStreamID: "foo", OriginStreamIndex: 2, ID: "3", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: "foo", StreamIndex: 3, OriginStreamID: "foo", OriginStreamIndex: 3, ID: "4", Type: "test", Data: json.RawMessage(`{}`)},
		}
		if !similar(exp, recs) {
			t.Errorf("expected other events")
		}
	}

	if err := s.Append(All, 0, Records{
		{ID: "1", Type: "test", Data: json.RawMessage(`{}`)},
	}); err == nil {
		t.Errorf("expected an error")
	}

	{
		recs := s.Load(All).Records()
		if count := len(recs); count != 6 {
			t.Errorf("expected count to be %d, but got: %d", 6, count)
		}
		exp := Records{
			{StreamID: All, StreamIndex: 0, OriginStreamID: "foo", OriginStreamIndex: 0, ID: "1", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: All, StreamIndex: 1, OriginStreamID: "foo", OriginStreamIndex: 1, ID: "2", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: All, StreamIndex: 2, OriginStreamID: "foo", OriginStreamIndex: 2, ID: "3", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: All, StreamIndex: 3, OriginStreamID: "foo", OriginStreamIndex: 3, ID: "4", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: All, StreamIndex: 4, OriginStreamID: "bar", OriginStreamIndex: 0, ID: "1", Type: "test", Data: json.RawMessage(`{}`)},
			{StreamID: All, StreamIndex: 5, OriginStreamID: "bar", OriginStreamIndex: 1, ID: "2", Type: "test", Data: json.RawMessage(`{}`)},
		}
		if !similar(exp, recs) {
			t.Errorf("want:\n%#v\ngot:\n%#v\n", exp, recs)
		}
	}

}
