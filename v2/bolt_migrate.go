//go:build (386 || amd64 || arm || arm64 || ppc || ppc64 || ppc64le || s390x) && !nobolt
// +build 386 amd64 arm arm64 ppc ppc64 ppc64le s390x
// +build !nobolt

package raftboltdb

import (
	"fmt"
	"os"
	"time"

	v1 "github.com/boltdb/bolt"
)

// MigrateToV2 reads in the source file path of a BoltDB file
// and outputs all the data migrated to a Bbolt destination file
func MigrateToV2(source, destination string) (*BoltStore, error) {
	_, err := os.Stat(destination)
	if err == nil {
		return nil, fmt.Errorf("file exists in destination %v", destination)
	}

	srcDb, err := v1.Open(source, dbFileMode, &v1.Options{
		ReadOnly: true,
		Timeout:  1 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("failed opening source database: %v", err)
	}

	//Start a connection to the source
	srctx, err := srcDb.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed connecting to source database: %v", err)
	}
	defer srctx.Rollback()

	//Create the destination
	destDb, err := New(Options{Path: destination})
	if err != nil {
		return nil, fmt.Errorf("failed creating destination database: %v", err)
	}
	//Start a connection to the new
	desttx, err := destDb.conn.Begin(true)
	if err != nil {
		destDb.Close()
		os.Remove(destination)
		return nil, fmt.Errorf("failed connecting to destination database: %v", err)
	}

	defer desttx.Rollback()

	//Loop over both old buckets and set them in the new
	buckets := [][]byte{dbConf, dbLogs}
	for _, b := range buckets {
		srcB := srctx.Bucket(b)
		destB := desttx.Bucket(b)
		err = srcB.ForEach(func(k, v []byte) error {
			return destB.Put(k, v)
		})
		if err != nil {
			destDb.Close()
			os.Remove(destination)
			return nil, fmt.Errorf("failed to copy %v bucket: %v", string(b), err)
		}
	}

	//If the commit fails, clean up
	if err := desttx.Commit(); err != nil {
		destDb.Close()
		os.Remove(destination)
		return nil, fmt.Errorf("failed commiting data to destination: %v", err)
	}

	return destDb, nil

}
