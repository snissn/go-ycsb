# AGENTS

## TODO
- [x] Add TreeDB backend adapter and register it
- [x] Document TreeDB properties and add it to supported database list
- [x] Run TreeDB benchmark smoke (load + run)
- [ ] Run go test ./... (optional)

## Test Gates
- go test ./...
- go run ./cmd/go-ycsb load treedb -P workloads/workloada -p recordcount=1000 -p operationcount=1000 -p dropdata=true -p treedb.dir=/tmp/treedb
- go run ./cmd/go-ycsb run treedb -P workloads/workloada -p recordcount=1000 -p operationcount=1000 -p treedb.dir=/tmp/treedb
