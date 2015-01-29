package raftboltdb

import (
	"github.com/boltdb/bolt"
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
	handle, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Create and return the new db store
	store := &BoltStore{
		conn: handle,
		path: path,
	}
	return store, nil
}
