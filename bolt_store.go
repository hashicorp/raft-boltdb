package raftboltdb

import (
	"github.com/boltdb/bolt"
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
