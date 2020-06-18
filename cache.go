package event

import (
	"encoding/json"
	"sync"
)

type Constructor func() Mutator

func NewCache(
	store Store,
	codec *Codec,
	new Constructor,
	snapshotThreshold int,
) *Cache {
	c := &Cache{
		Store:             store,
		codec:             codec,
		new:               new,
		snapshotThreshold: snapshotThreshold,
		storage:           map[string]Snapshot{},
	}
	return c
}

type Cache struct {
	Store
	codec             *Codec
	new               Constructor
	snapshotThreshold int
	mu                sync.RWMutex
	storage           map[string]Snapshot
}

func (c *Cache) Append(streamID string, expectedVersion uint64, records Records) error {
	err := c.Store.Append(streamID, expectedVersion, records)
	if err != nil {
		return err
	}
	if len(records) >= c.snapshotThreshold {
		m := c.new()
		c.LoadState(streamID, m)
	}
	return nil
}

func (c *Cache) LoadState(streamID string, m Mutator) error {
	// TODO: check if mutator is same as typ
	snap, _ := c.loadSnapshot(streamID)
	if snap.Version > 0 {
		// we have a snapshot
		if err := json.Unmarshal(snap.Data, m); err != nil {
			return err
		}
	}
	mutations := 0
	for r := range c.LoadFrom(streamID, snap.Version) {
		e, err := c.codec.Decode(r)
		if err != nil {
			return err
		}
		m.Mutate(e)
		mutations++
	}
	// log.Printf("mutations: %d\n", mutations)
	if mutations >= c.snapshotThreshold {
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		err = c.storeSnapshot(Snapshot{StreamID: streamID, Version: snap.Version + uint64(mutations), Data: data})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) loadSnapshot(streamID string) (Snapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap, ok := c.storage[streamID]
	if !ok {
		// TODO: try level2 storage
		return Snapshot{StreamID: streamID, Version: 0, Data: nil}, nil
	}
	//log.Printf("loadSnapshot: %s:@%d\n", snap.StreamID, snap.Version)
	return snap, nil
}

func (c *Cache) storeSnapshot(snap Snapshot) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storage[snap.StreamID] = snap
	// TODO: add to level2
	//log.Printf("storeSnapshot: %s@%d\n", snap.StreamID, snap.Version)
	return nil
}

type Snapshot struct {
	StreamID string
	Version  uint64
	Data     []byte
}
