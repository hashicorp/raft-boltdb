package raftboltdb

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

func testBoltStore(t *testing.T) *BoltStore {
	fh, err := ioutil.TempFile("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store
}

func TestNewBoltStore(t *testing.T) {
	fh, err := ioutil.TempFile("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the file was created
	if store.path != fh.Name() {
		t.Fatalf("unexpected file path %q", store.path)
	}
	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure our tables were created
	db, err := bolt.Open(fh.Name(), dbFileMode, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if _, err := tx.CreateBucket([]byte(dbLogs)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
	if _, err := tx.CreateBucket([]byte(dbConf)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
}

func TestBoltStore_FirstIndex(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	err = store.conn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(dbLogs))
		bucket.Put(uint64ToBytes(1), []byte("log1"))
		bucket.Put(uint64ToBytes(2), []byte("log2"))
		return nil
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBoltStore_LastIndex(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	err = store.conn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(dbLogs))
		bucket.Put(uint64ToBytes(1), []byte("log1"))
		bucket.Put(uint64ToBytes(2), []byte("log2"))
		return nil
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 2 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBoltStore_GetLog(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Create a fake raft log
	existing := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}
	logBuf, err := encodeMsgPack(existing)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Set a mock raft log
	err = store.conn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(dbLogs))
		bucket.Put(uint64ToBytes(1), logBuf.Bytes())
		return nil
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(1, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, existing) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestBoltStore_SetLog(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestBoltStore_SetLogs(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	log1 := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}
	log2 := &raft.Log{
		Data:  []byte("log2"),
		Index: 2,
	}
	logs := []*raft.Log{log1, log2}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log1, result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log2, result2) {
		t.Fatalf("bad: %#v", result2)
	}
}
