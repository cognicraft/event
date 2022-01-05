package event

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/cognicraft/io"
	"github.com/cognicraft/uuid"
)

type RecordMutation func(r *Record)

func WithMetadata(v interface{}) RecordMutation {
	return func(r *Record) {
		if v == nil {
			return
		}
		if md, err := Encode(v); err == nil {
			r.Metadata = md
		}
	}
}

type CodecOption func(*Codec)

// ExtractID sets an ExtractIDFunc for the codec
func ExtractID(fn ExtractIDFunc) CodecOption {
	return func(c *Codec) {
		c.extractID = fn
	}
}

type ExtractIDFunc func(e Event) string

// IDByField creates an ExtractIDFunc that uses reflection to extract a string
// from a field with the fieldName. If that field is not present or the
// extracted string is empty a uuid v4 will be generated as a result.
func IDByField(fieldName string) ExtractIDFunc {
	return func(e Event) string {
		value := reflect.ValueOf(e)
		fieldID := value.FieldByName(fieldName)
		if !fieldID.IsValid() {
			return uuid.MakeV4()
		}
		eID, _ := fieldID.Interface().(string)
		if eID == "" {
			return uuid.MakeV4()
		}
		return eID
	}
}

var (
	DefaultExtractID = IDByField("ID")
)

// NewCodec creates a new Codec to encode Events into Records and decode
// Records into Events.
func NewCodec(opts ...CodecOption) *Codec {
	c := &Codec{
		TypeRegistry: io.NewTypeRegistry(),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.extractID == nil {
		c.extractID = DefaultExtractID
	}
	return c
}

type Codec struct {
	*io.TypeRegistry
	extractID ExtractIDFunc
}

func (c *Codec) EncodeAll(events Events, muts ...RecordMutation) (Records, error) {
	var recs Records
	for _, evt := range events {
		rec, err := c.Encode(evt, muts...)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

func (c *Codec) Encode(event Event, muts ...RecordMutation) (Record, error) {
	name, data, err := c.Marshal(json.Marshal, event)
	if err != nil {
		return Record{}, err
	}
	r := Record{
		ID:         c.extractID(event),
		RecordedOn: time.Now().UTC(),
		Type:       name,
		Data:       json.RawMessage(data),
	}
	// apply all additional mutations
	for _, mut := range muts {
		mut(&r)
	}
	return r, nil
}

func (c *Codec) DecodeAll(records Records) (Events, error) {
	var evts Events
	for _, rec := range records {
		evt, err := c.Decode(rec)
		if err != nil {
			return nil, err
		}
		evts = append(evts, evt)
	}
	return evts, nil
}

func (c *Codec) Decode(record Record) (Event, error) {
	return c.Unmarshal(json.Unmarshal, record.Type, record.Data)
}
