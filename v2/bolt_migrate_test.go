//go:build (386 || amd64 || arm || arm64 || ppc || ppc64 || ppc64le || s390x) && !nobolt
// +build 386 amd64 arm arm64 ppc ppc64 ppc64le s390x
// +build !nobolt

package raftboltdb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
	v1 "github.com/hashicorp/raft-boltdb"
)

func TestBoltStore_MigrateToV2(t *testing.T) {

	dir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(dir)

	srcFile := filepath.Join(dir, "/sourcepath")
	destFile := filepath.Join(dir, "/destpath")

	// Successfully creates and returns a store
	srcDb, err := v1.NewBoltStore(srcFile)
	if err != nil {
		t.Fatalf("failed creating source database: %s", err)
	}
	defer srcDb.Close()

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}

	//Store logs source
	if err := srcDb.StoreLogs(logs); err != nil {
		t.Fatalf("failed storing logs in source database: %s", err)
	}
	srcResult := new(raft.Log)
	if err := srcDb.GetLog(2, srcResult); err != nil {
		t.Fatalf("failed getting log from source database: %s", err)
	}

	if err := srcDb.Close(); err != nil {
		t.Fatalf("failed closing source database: %s", err)
	}

	destDb, err := MigrateToV2(srcFile, destFile)
	if err != nil {
		t.Fatalf("did not migrate successfully, err %v", err)
	}
	defer destDb.Close()

	destResult := new(raft.Log)
	if err := destDb.GetLog(2, destResult); err != nil {
		t.Fatalf("failed getting log from destination database: %s", err)
	}

	if !reflect.DeepEqual(srcResult, destResult) {
		t.Errorf("BoltDB log did not equal Bbolt log, Boltdb %v, Bbolt: %v", srcResult, destResult)
	}

}
