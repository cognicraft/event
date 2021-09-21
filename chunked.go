package event

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/cognicraft/pubsub"
	"github.com/cognicraft/sqlutil"
)

var (
	_ Store = (*ChunkedStore)(nil)
)

const (
	defaultCSBatchSize = uint64(50)
	defaultCSChunkSize = uint64(1000000)
)

func parseChunkedStoreDSN(dsn string) (string, uint64, uint64) {
	dir := ""
	batchSize := defaultCSBatchSize
	chunkSize := defaultCSChunkSize

	opts := urn(dsn)
	dir = opts.Path()
	vals := opts.Query()
	if opt := vals.Get("batch-size"); opt != "" {
		if val, err := strconv.ParseUint(opt, 10, 64); err == nil {
			batchSize = val
		}
	}
	if opt := vals.Get("chunk-size"); opt != "" {
		if val, err := strconv.ParseUint(opt, 10, 64); err == nil {
			chunkSize = val
		}
	}
	return dir, batchSize, chunkSize
}

func NewChunkedStore(dataSourceName string) (*ChunkedStore, error) {
	s := &ChunkedStore{
		publisher: pubsub.NewPublisher(),
	}
	s.dir, s.batchSize, s.chunkSize = parseChunkedStoreDSN(dataSourceName)
	return s, s.init()
}

type ChunkedStore struct {
	dir       string
	batchSize uint64
	chunkSize uint64
	mu        sync.Mutex
	index     *sql.DB
	publisher pubsub.Publisher
}

func (s *ChunkedStore) Version(streamID string) uint64 {
	qVersion := s.index.QueryRow(`SELECT version FROM streams WHERE id = ? LIMIT 1;`, streamID)
	var version uint64
	qVersion.Scan(&version)
	return version
}

func (s *ChunkedStore) Load(streamID string) RecordStream {
	return s.LoadFrom(streamID, 0)
}

func (s *ChunkedStore) LoadFrom(streamID string, skip uint64) RecordStream {
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

func (s *ChunkedStore) LoadSlice(streamID string, skip uint64, limit uint64) (*Slice, error) {
	var c *readChunk
	var err error
	res := &Slice{
		StreamID: streamID,
		From:     skip,
	}
	nSkip := skip
	nLimit := limit
	for {
		var chunkID int
		if All == streamID {
			chunkID = int(nSkip) / int(s.chunkSize)
		} else {
			qChunkID := s.index.QueryRow(`SELECT chunkID FROM chunk_streams WHERE streamID = ? AND ? BETWEEN minIndex AND maxIndex LIMIT 1;`, streamID, nSkip)
			qChunkID.Scan(&chunkID)
		}
		c, err = s.readChunk(chunkID)
		if err != nil {
			// no chunk exists
			break
		}
		records, err := c.loadRecords(streamID, nSkip, nLimit+1)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			// no new records could be found
			break
		}
		res.Records = append(res.Records, records...)
		if len(res.Records) >= int(limit) {
			// enough records have been found
			break
		}
		nEvents := uint64(len(records))
		nSkip += nEvents
		nLimit -= nEvents
	}
	res.IsEndOfStream = (len(res.Records) <= int(limit))
	if !res.IsEndOfStream {
		res.Records = res.Records[:limit]
	}
	if n := len(res.Records); n > 0 {
		res.Next = res.Records[n-1].StreamIndex + 1
	}
	return res, nil
}

func (s *ChunkedStore) Append(streamID string, expectedVersion uint64, records Records) error {
	if All == streamID {
		return fmt.Errorf("cannot append to all stream")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	c, err := s.lastChunk()
	if err != nil {
		return err
	}

	streamVersion := s.Version(streamID)
	if streamVersion != expectedVersion {
		return OptimisticConcurrencyError{Stream: streamID, Expected: expectedVersion, Actual: streamVersion}
	}

	storeVersion := s.Version(All)
	for i, r := range records {
		storeVersion++
		r.StreamID = streamID
		r.StreamIndex = streamVersion
		streamVersion++
		if r.RecordedOn.IsZero() {
			r.RecordedOn = time.Now().UTC()
		}
		records[i] = r
	}

	toAppend := records
	for len(toAppend) > 0 {
		rem := int(c.remaining())
		if rem == 0 {
			if c, err = s.nextChunk(); err != nil {
				return err
			}
			continue
		}
		var next Records
		if len(toAppend) <= rem {
			next = toAppend
			toAppend = nil
		} else {
			next = toAppend[:rem]
			toAppend = toAppend[rem:]
		}
		if err := c.append(next); err != nil {
			return err
		}
		if err := s.updateIndex(storeVersion, c.id, streamID, next[0].StreamIndex, next[len(next)-1].StreamIndex); err != nil {
			return err
		}
	}
	if err == nil {
		s.publisher.Publish(topicAppend, streamID)
	}
	return nil
}

func (s *ChunkedStore) SubscribeToStream(streamID string) Subscription {
	return &subscription{
		loadSlice: s.LoadSlice,
		batchSize: s.batchSize,
		subscribe: s.publisher.Subscribe,
		streamID:  streamID,
		from:      0,
	}
}

func (s *ChunkedStore) SubscribeToStreamFrom(streamID string, version uint64) Subscription {
	return &subscription{
		loadSlice: s.LoadSlice,
		batchSize: s.batchSize,
		subscribe: s.publisher.Subscribe,
		streamID:  streamID,
		from:      version,
	}
}

func (s *ChunkedStore) SubscribeToStreamFromCurrent(streamID string) Subscription {
	return &subscription{
		loadSlice: s.LoadSlice,
		batchSize: s.batchSize,
		subscribe: s.publisher.Subscribe,
		streamID:  streamID,
		from:      s.Version(streamID),
	}
}

func (s *ChunkedStore) Close() error {
	return nil
}

func (s *ChunkedStore) init() error {
	err := os.MkdirAll(s.directory(), os.ModePerm)
	if err != nil {
		return err
	}
	s.index, err = sql.Open("sqlite3", filepath.Join(s.directory(), "index.db"))
	if err != nil {
		return err
	}
	s.index.SetMaxOpenConns(1)
	_, err = s.index.Exec(initialize_index)
	if err != nil {
		return err
	}
	return nil
}

func (s *ChunkedStore) directory() string {
	return s.dir
}

func (s *ChunkedStore) lastChunk() (*writeChunk, error) {
	cIdxQ := s.index.QueryRow(`SELECT id FROM chunks WHERE status = 'active' ORDER BY id DESC LIMIT 1;`)
	var cIdx int
	if err := cIdxQ.Scan(&cIdx); err != nil {
		//log.Printf("ERROR: %v", err)
		if _, err := s.index.Exec(`INSERT INTO chunks (id, status) VALUES (?, ?);`, 0, "active"); err != nil {
			//log.Printf("ERROR: %v", err)
		}
	}
	return s.writechunk(cIdx)
}

func (s *ChunkedStore) nextChunk() (*writeChunk, error) {
	cIdxQ := s.index.QueryRow(`SELECT id FROM chunks WHERE status = 'active' ORDER BY id DESC LIMIT 1;`)
	var cIdx int
	if err := cIdxQ.Scan(&cIdx); err != nil {
		return nil, err
	}
	if _, err := s.index.Exec(`UPDATE chunks SET status = ? WHERE id = ?;`, "complete", cIdx); err != nil {
		log.Printf("ERROR: %v", err)
	}
	nIdx := cIdx + 1
	if _, err := s.index.Exec(`INSERT INTO chunks (id, status) VALUES (?, ?);`, nIdx, "active"); err != nil {
		log.Printf("ERROR: %v", err)
	}
	return s.writechunk(nIdx)
}

func (s *ChunkedStore) updateIndex(storeVersion uint64, chunkID int, streamID string, minIndex uint64, maxIndex uint64) error {
	err := sqlutil.Transact(s.index, func(tx *sql.Tx) error {
		qMinIndex := tx.QueryRow("SELECT minIndex FROM chunk_streams WHERE chunkID = ? AND streamID = ? LIMIT 1;", chunkID, streamID)
		var sMinIndex uint64
		if err := qMinIndex.Scan(&sMinIndex); err == nil {
			minIndex = min(minIndex, sMinIndex)
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO chunk_streams (chunkID, streamID, minIndex, maxIndex) VALUES (?, ?, ?, ?);`,
			chunkID, streamID, minIndex, maxIndex); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO streams (id, version) VALUES (?, ?);`, streamID, maxIndex+1); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO streams (id, version) VALUES (?, ?);`, All, storeVersion); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *ChunkedStore) writechunk(id int) (*writeChunk, error) {
	var exists bool
	q := s.index.QueryRow(`SELECT 1 FROM chunks WHERE id = ? AND status = 'active';`, id)
	q.Scan(&exists)
	if !exists {
		return nil, fmt.Errorf("chunk does not exist")
	}
	c := &writeChunk{
		store: s,
		id:    id,
	}
	return c, c.init()
}

func (s *ChunkedStore) readChunk(id int) (*readChunk, error) {
	var exists bool
	q := s.index.QueryRow(`SELECT 1 FROM chunks WHERE id = ?;`, id)
	q.Scan(&exists)
	if !exists {
		return nil, fmt.Errorf("chunk does not exist")
	}
	c := &readChunk{
		store: s,
		id:    id,
	}
	return c, c.init()
}

type writeChunk struct {
	store *ChunkedStore
	id    int
	mu    sync.Mutex
	db    *sql.DB
}

func (c *writeChunk) init() error {
	var err error
	c.db, err = sql.Open("sqlite3", filepath.Join(c.store.directory(), fmt.Sprintf("%010d.db", c.id)))
	if err != nil {
		return err
	}
	c.db.SetMaxOpenConns(1)
	_, err = c.db.Exec(initialize_chunk)
	if err != nil {
		return err
	}
	return nil
}

func (c *writeChunk) version() uint64 {
	vQ := c.db.QueryRow(`SELECT (storeIndex+1) as version FROM events ORDER BY storeIndex DESC LIMIT 1;`)
	var version uint64
	err := vQ.Scan(&version)
	if err != nil {
		// stream does not exist
		return uint64(c.id) * c.store.chunkSize
	}
	return version
}

func (c *writeChunk) remaining() uint64 {
	sv := c.version()
	return uint64(c.id+1)*c.store.chunkSize - sv
}

func (c *writeChunk) append(records Records) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	storeVersion := c.version()
	err := sqlutil.Transact(c.db, func(tx *sql.Tx) error {
		for _, r := range records {
			storeIndex := storeVersion
			storeVersion++
			if _, err := tx.Exec(`INSERT INTO events (storeIndex, streamID, streamIndex, recordedOn, id, type, data, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
				storeIndex, r.StreamID, r.StreamIndex, formatTime(r.RecordedOn), r.ID, r.Type, []byte(r.Data), []byte(r.Metadata)); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

type readChunk struct {
	store *ChunkedStore
	id    int
	mu    sync.Mutex
	db    *sql.DB
}

func (c *readChunk) init() error {
	var err error
	c.db, err = sql.Open("sqlite3", filepath.Join(c.store.directory(), fmt.Sprintf("%010d.db", c.id)))
	if err != nil {
		return err
	}
	return nil
}

func (c *readChunk) loadRecords(streamID string, skip uint64, limit uint64) (Records, error) {
	var rows *sql.Rows
	var err error
	if All == streamID {
		query := `
		SELECT '$all', storeIndex, streamID, streamIndex, recordedOn, id, type, data, metadata
		FROM   events
		WHERE  storeIndex >= ?
		ORDER  BY storeIndex
		LIMIT  ?;`
		rows, err = c.db.Query(query, int64(skip), int64(limit)+1)
	} else {
		query := `
		SELECT streamID, streamIndex, streamID, streamIndex, recordedOn, id, type, data, metadata
		FROM   events
		WHERE  streamID = ?
		       AND streamIndex >= ?
		ORDER  BY streamIndex
		LIMIT  ?;`
		rows, err = c.db.Query(query, streamID, int64(skip), int64(limit)+1)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return records(rows)
}

const initialize_index = `
CREATE TABLE IF NOT EXISTS chunks (
  id INTEGER NOT NULL,
  status TEXT NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS streams (
  id TEXT,
  version INTEGER,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS chunk_streams (
  chunkID INTEGER,
  streamID TEXT,
  minIndex INTEGER,
  maxIndex INTEGER,
  PRIMARY KEY (chunkID, streamID)
);
`

const initialize_chunk = `
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
