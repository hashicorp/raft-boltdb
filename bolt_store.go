package raftboltdb

import (
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0600

	// Bucket names we perform transactions in
	dbLogs = "logs"
	dbConf = "conf"
)

type BoltStore struct {
	// conn is the underlying handle to the db.
	conn *bolt.DB

	// The path to the Bolt database file
	path string
}

// NewBoltStore takes a file path and returns a connected Raft backend.
func NewBoltStore(path string) (*BoltStore, error) {
	// Try to connect
	handle, err := bolt.Open(path, dbFileMode, nil)
	if err != nil {
		return nil, err
	}

	// Create and return the new db store
	store := &BoltStore{
		conn: handle,
		path: path,
	}

	// Set up our buckets
	if err := store.initialize(); err != nil {
		store.Close()
		return nil, err
	}

	return store, nil
}

// initialize is used to set up all of the buckets.
func (b *BoltStore) initialize() error {
	// Create all the buckets
	err := b.conn.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(dbLogs)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(dbConf)); err != nil {
			return err
		}
		return nil
	})
	return err
}

// Close is used to gracefully close the DB connection.
func (b *BoltStore) Close() error {
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BoltStore) FirstIndex() (uint64, error) {
	var idx uint64
	err := b.conn.View(func(tx *bolt.Tx) error {
		curs := tx.Bucket([]byte(dbLogs)).Cursor()
		if first, _ := curs.First(); first == nil {
			idx = 0
		} else {
			idx = bytesToUint64(first)
		}
		return nil
	})
	return idx, err
}

// LastIndex returns the last known index from the Raft log.
func (b *BoltStore) LastIndex() (uint64, error) {
	var idx uint64
	err := b.conn.View(func(tx *bolt.Tx) error {
		curs := tx.Bucket([]byte(dbLogs)).Cursor()
		if last, _ := curs.Last(); last == nil {
			idx = 0
		} else {
			idx = bytesToUint64(last)
		}
		return nil
	})
	return idx, err
}

// GetLog is used to retrieve a log from BoltDB at a given index.
func (b *BoltStore) GetLog(idx uint64, log *raft.Log) error {
	var k, v []byte
	err := b.conn.View(func(tx *bolt.Tx) error {
		curs := tx.Bucket([]byte(dbLogs)).Cursor()
		k, v = curs.Seek(uint64ToBytes(idx))
		return nil
	})
	if err != nil {
		return err
	}
	if k == nil {
		return raft.ErrLogNotFound
	}
	decodeMsgPack(v, log)
	return nil
}
