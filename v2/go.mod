module github.com/hashicorp/raft-boltdb/v2

go 1.20

require (
	github.com/armon/go-metrics v0.4.1
	github.com/boltdb/bolt v1.3.1
	github.com/hashicorp/go-msgpack/v2 v2.1.1
	github.com/hashicorp/raft v1.6.0
	github.com/hashicorp/raft-boltdb v0.0.0-20230125174641-2a8082862702
	go.etcd.io/bbolt v1.3.5
)

require (
	github.com/fatih/color v1.13.0 // indirect
	github.com/hashicorp/go-hclog v1.5.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	golang.org/x/sys v0.13.0 // indirect
)

replace github.com/hashicorp/raft => /Users/swenson/projects/raft
