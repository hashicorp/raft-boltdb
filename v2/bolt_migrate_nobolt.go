//go:build (!386 && !amd64 && !arm && !arm64 && !ppc && !ppc64 && !ppc64le && !s390x) || nobolt
// +build !386,!amd64,!arm,!arm64,!ppc,!ppc64,!ppc64le,!s390x nobolt

package raftboltdb

// MigrateToV2 reads in the source file path of a BoltDB file
// and outputs all the data migrated to a Bbolt destination file
func MigrateToV2(source, destination string) (*BoltStore, error) {
	return nil, ErrNotImplemented
}
