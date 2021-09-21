package event

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"time"
)

const (
	rfc3339NanoSortable = "2006-01-02T15:04:05.000000000Z07:00"
)

func formatTime(t time.Time) string {
	return t.UTC().Format(rfc3339NanoSortable)
}

func parseTime(raw string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, raw)
	return t
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

/*
streamID, streamIndex, originStreamID, originStreamIndex, recordedOn, id, typ, data, metadata
*/
func records(rows *sql.Rows) (Records, error) {
	var res Records
	for rows.Next() {
		var streamID string
		var streamIndex uint64
		var originStreamID string
		var originStreamIndex uint64
		var id string
		var typ string
		var recordedOn string
		var data []byte
		var metadata []byte
		err := rows.Scan(&streamID, &streamIndex, &originStreamID, &originStreamIndex, &recordedOn, &id, &typ, &data, &metadata)
		if err != nil {
			return nil, err
		}
		r := Record{
			StreamID:          streamID,
			StreamIndex:       streamIndex,
			OriginStreamID:    originStreamID,
			OriginStreamIndex: originStreamIndex,
			RecordedOn:        parseTime(recordedOn),
			ID:                id,
			Type:              typ,
			Data:              json.RawMessage(data),
			Metadata:          json.RawMessage(metadata),
		}
		res = append(res, r)
	}
	return res, nil
}

func similar(a Records, b Records) bool {
	if len(a) != len(b) {
		return false
	}
	for i, ar := range a {
		br := b[i]
		ar.RecordedOn = time.Time{}
		br.RecordedOn = time.Time{}
		if !reflect.DeepEqual(ar, br) {
			return false
		}
	}
	return true
}
