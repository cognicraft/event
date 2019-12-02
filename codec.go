package event

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/cognicraft/io"
	"github.com/cognicraft/uuid"
)

func NewCodec() *Codec {
	return &Codec{
		TypeRegistry: io.NewTypeRegistry(),
	}
}

type Codec struct {
	*io.TypeRegistry
}

func (c *Codec) Encode(event Event) (Record, error) {
	name, data, err := c.Marshal(json.Marshal, event)
	if err != nil {
		return Record{}, err
	}
	r := Record{
		ID:         id(event),
		RecordedOn: time.Now().UTC(),
		Type:       name,
		Data:       json.RawMessage(data),
	}
	return r, nil
}

func (c *Codec) Decode(record Record) (Event, error) {
	return c.Unmarshal(json.Unmarshal, record.Type, record.Data)
}

func id(e Event) string {
	value := reflect.ValueOf(e)
	fieldID := value.FieldByName("ID")
	if !fieldID.IsValid() {
		return uuid.MakeV4()
	}
	eID, _ := fieldID.Interface().(string)
	if eID == "" {
		return uuid.MakeV4()
	}
	return eID
}
