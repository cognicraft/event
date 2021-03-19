package event

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/cognicraft/pubsub"
	"github.com/cognicraft/sqlutil"
)

const (
	All = "$all"
)

const (
	topicAppend pubsub.Topic = "append"
)

func NewBasicStore(dataSourceName string) (*BasicStore, error) {
	s := &BasicStore{
		dataSourceName: setOptions(dataSourceName),
		batchSize:      50,
		publisher:      pubsub.NewPublisher(),
	}
	return s, s.init()
}

type BasicStore struct {
	dataSourceName string
	batchSize      uint64
	mu             sync.Mutex
	db             *sql.DB
	publisher      pubsub.Publisher
}

func (s *BasicStore) Version(streamID string) uint64 {
	var row *sql.Row
	if All == streamID {
		row = s.db.QueryRow(`SELECT (storeIndex+1) as version FROM events ORDER BY storeIndex DESC LIMIT 1;`)
	} else {
		row = s.db.QueryRow(`SELECT (streamIndex+1) as version FROM events WHERE streamID = ? ORDER BY storeIndex DESC LIMIT 1;`, streamID, 0)
	}
	var version uint64
	err := row.Scan(&version)
	if err != nil {
		// stream does not exist
		return 0
	}
	return version
}

func (s *BasicStore) Load(streamID string) RecordStream {
	return s.LoadFrom(streamID, 0)
}

func (s *BasicStore) LoadFrom(streamID string, skip uint64) RecordStream {
	out := make(chan Record)
	go func() {
		defer close(out)
		next := skip
		for {
			slice, err := s.LoadSlice(streamID, next, s.batchSize)
			if err != nil {
				return
			}
			for _, e := range slice.Records {
				out <- e
			}
			if slice.IsEndOfStream {
				return
			}
			next = slice.Next
		}
	}()
	return out
}

func (s *BasicStore) LoadSlice(streamID string, skip uint64, limit uint64) (*Slice, error) {
	if All == streamID {
		return s.loadAllSlice(skip, limit)
	}
	query := `
	SELECT streamID, streamIndex, recordedOn, id, type, data, metadata
	FROM   events
	WHERE  streamID = ?
	       AND streamIndex >= ?
	ORDER  BY streamIndex
	LIMIT  ?;`
	rows, err := s.db.Query(query, streamID, int64(skip), int64(limit)+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	slice := Slice{
		StreamID: streamID,
		From:     skip,
	}
	for rows.Next() {
		var streamID string
		var streamIndex uint64
		var id string
		var typ string
		var recordedOn string
		var data []byte
		var metadata []byte
		err := rows.Scan(&streamID, &streamIndex, &recordedOn, &id, &typ, &data, &metadata)
		if err != nil {
			return nil, err
		}
		r := Record{
			ID:                id,
			StreamID:          streamID,
			StreamIndex:       streamIndex,
			OriginStreamID:    streamID,
			OriginStreamIndex: streamIndex,
			RecordedOn:        parseTime(recordedOn),
			Type:              typ,
			Data:              json.RawMessage(data),
			Metadata:          json.RawMessage(metadata),
		}
		slice.Records = append(slice.Records, r)
	}

	nEvents := uint64(len(slice.Records))
	slice.IsEndOfStream = (nEvents <= limit)
	if slice.IsEndOfStream {
		slice.Next = skip + nEvents
	} else {
		slice.Records = slice.Records[:limit]
		slice.Next = skip + nEvents - 1
	}
	return &slice, nil
}

func (s *BasicStore) loadAllSlice(skip uint64, limit uint64) (*Slice, error) {
	query := `
	SELECT storeIndex, streamID, streamIndex, recordedOn, id, type, data, metadata
	FROM   events
	WHERE  storeIndex >= ?
	ORDER  BY storeIndex
	LIMIT  ?;`
	rows, err := s.db.Query(query, int64(skip), int64(limit)+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	slice := Slice{
		StreamID: All,
		From:     skip,
	}
	for rows.Next() {
		var storeIndex uint64
		var streamID string
		var streamIndex uint64
		var id string
		var typ string
		var recordedOn string
		var data []byte
		var metadata []byte
		err := rows.Scan(&storeIndex, &streamID, &streamIndex, &recordedOn, &id, &typ, &data, &metadata)
		if err != nil {
			return nil, err
		}
		r := Record{
			ID:                id,
			StreamID:          All,
			StreamIndex:       storeIndex,
			OriginStreamID:    streamID,
			OriginStreamIndex: streamIndex,
			RecordedOn:        parseTime(recordedOn),
			Type:              typ,
			Data:              json.RawMessage(data),
			Metadata:          json.RawMessage(metadata),
		}
		slice.Records = append(slice.Records, r)
	}

	nEvents := uint64(len(slice.Records))
	slice.IsEndOfStream = (nEvents <= limit)
	if slice.IsEndOfStream {
		slice.Next = skip + nEvents
	} else {
		slice.Records = slice.Records[:limit]
		slice.Next = skip + nEvents - 1
	}
	return &slice, nil
}

func (s *BasicStore) Append(streamID string, expectedVersion uint64, records Records) error {
	if All == streamID {
		return fmt.Errorf("cannot append to all stream")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := sqlutil.Transact(s.db, func(tx *sql.Tx) error {
		streamVersion := uint64(0)
		row := tx.QueryRow(`SELECT (streamIndex+1) as version FROM events WHERE streamID = ? ORDER BY streamIndex DESC LIMIT 1;`, streamID)
		row.Scan(&streamVersion)
		if streamVersion != expectedVersion {
			return OptimisticConcurrencyError{Stream: streamID, Expected: expectedVersion, Actual: streamVersion}
		}

		storeVersion := uint64(0)
		row = tx.QueryRow(`SELECT (storeIndex+1) as version FROM events ORDER BY storeIndex DESC LIMIT 1;`)
		row.Scan(&storeVersion)

		for _, e := range records {
			storeIndex := uint64(storeVersion)
			storeVersion++
			streamIndex := uint64(streamVersion)
			streamVersion++
			if e.RecordedOn.IsZero() {
				e.RecordedOn = time.Now().UTC()
			}
			e.StreamID = streamID
			e.StreamIndex = streamIndex
			e.OriginStreamID = streamID
			e.OriginStreamIndex = streamIndex
			if _, err := tx.Exec(`INSERT INTO events (storeIndex, streamID, streamIndex, recordedOn, id, type, data, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
				storeIndex, streamID, streamIndex, formatTime(e.RecordedOn), e.ID, e.Type, []byte(e.Data), []byte(e.Metadata)); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		s.publisher.Publish(topicAppend, streamID)
	}
	return err
}

func (s *BasicStore) Close() error {
	return s.db.Close()
}

func (s *BasicStore) init() error {
	dir := path.Dir(s.dataSourceName)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}
	s.db, err = sql.Open("sqlite3", s.dataSourceName)
	if err != nil {
		return err
	}
	s.db.SetMaxOpenConns(1)
	_, err = s.db.Exec(initialize)
	if err != nil {
		return err
	}
	return nil

}

func setOptions(dsn string) string {
	return fmt.Sprintf("%s?_foreign_keys=on&_journal_mode=WAL&_locking_mode=NORMAL&_synchronous=OFF", dsn)
}

const initialize = `
CREATE TABLE IF NOT EXISTS events (
  storeIndex INTEGER NOT NULL,
  streamID TEXT NOT NULL,
  streamIndex INTEGER NOT NULL,
  recordedOn TEXT NOT NULL,
  id TEXT NOT NULL,
  type TEXT NOT NULL,
  data BLOB,
  metadata BLOB,
  PRIMARY KEY (storeIndex)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_streamID_streamIndex
ON events (streamID, streamIndex);
`
