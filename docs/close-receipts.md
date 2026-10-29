# Checked client cleanup receipts

The original `c195a9bc777b6aeb1e2f5550db96d872338f2f40` command discards
DB/workload Close and command errors. A successful TOTAL line therefore does
not establish successful cleanup. The command now runs both original cleanup
calls, retains each return, writes an optional receipt with checked I/O, then
exits nonzero on command, DB Close, workload Close, or receipt failure.

Set these environment variables separately for each client phase:

```
YCSB_CLOSE_RECEIPT_PATH=/absolute/new-exclusive-phase.json
YCSB_PHASE_ID=unique-load-or-run-id
YCSB_CLIENT_BUILD_ID=manifest-bound-client-build-id
YCSB_PROFILE=launcher-declared-server-profile
```

Receipt output is opt-in. Phase and client build IDs are required when a path
is selected. The file is created exclusively, mode 0600, after both cleanup
calls. Existing paths (including symlinks) are refused. Write and file-close
errors fail the process; a failed write may leave an incomplete file. Consumers
must reject missing/truncated/mismatched receipts and require the observed
child exit to succeed. The profile is a launcher declaration, not a server
witness. The launcher must bind IDs to actual PID/start, source/patch, toolchain
and client/server ELF hashes and independently validate server profile/owner.

Schema v1 has `schema_version`, `phase_id`, `client_build_id`, `db`, `phase`,
`declared_profile`, `total_excludes_close`, `command`, `db_close`,
`workload_close` and `native_flush`. Each result has `attempted`, `success` and
`error`. DB/workload Close and native dial/flush/connection-close results also
record `start_utc`, `end_utc` and monotonic `elapsed_ns`. Command timing fields
are zero: only its actual return is observed. A skipped call has attempted and
success false, zero timestamps/duration and an empty error.

`native_flush` contains `applicable`, `flush_enabled`, actual numeric `ack`
(visible=1, flushed=2, synced=3), `dial`, `flush`, and aggregate
`connection_close`. Native dial includes HELLO. Dial failure leaves flush
unattempted and preserves the actual dial error. Disabled flush is skipped,
not a successful required flush. Connection cleanup retains its actual errors.
The optional typed `ycsb.CloseReporter` returns the snapshot from the original
Close; `DbWrapper` forwards it with no additional Close/FlushAll. Mongo and
other backends without this capability report native flush not applicable,
while the command still observes their aggregate DB Close return.

For a native phase requiring flush, require successful command, attempted
successful DB/workload Close, applicable/enabled native flush, successful dial,
and attempted successful native flush. Native wire payload, ACK settings,
operation timeout and number of original flush calls are unchanged. No shadow
flush is performed. `measurement.Output()`, TOTAL's timing, random workload
and data-integrity defaults are unchanged. Close time is separate from TOTAL.
A flush success does not certify server checkpoint/reopen or crash durability.
Default random workloads do not retain every expected random write; counters
alone do not prove exact-value integrity. Initialization fatal exits, crashes
or forced termination can prevent receipts; those attempts remain invalid.

## Reproducing the original defect

`close-receipt-evidence/original-red.log` is a real subprocess RED from the
pinned client with only the test fixture added. The peer accepts HELLO,
collection metadata, catalog reads and two inserts. INSERT and TOTAL both have
Count 2 with no operation errors. The peer observes exactly one original
command 34/FlushAll with ACK visible, then disconnects. The original process
exits successfully; the regression fails specifically on that exit. The saved
test fixture and SHA-256 bind the original test executable and test source.
The test executable remains in the task evidence directory, not Git.

To reproduce independently, check out the pinned base in an isolated worktree,
copy `close-receipt-evidence/original-close-regression_test.go.txt` to
`cmd/go-ycsb/close_receipt_test.go`, then run:

```
GOWORK=off go test ./cmd/go-ycsb -run '^TestCloseFailureAfterTotalsIsFatal$' -count=1 -v
```

The test must fail with `client exited successfully after original
Close/FlushAll failed`. Setup failures do not establish this RED. Initial
fixture-metadata and inherited GOROOT/compiler failures were retained separately
in the task evidence; they are not the accepted reproduction.

The repaired `TestCloseFailureAfterTotalsIsFatal` runs the same real main path,
requires the failed actual flush/DB Close receipt, successful workload cleanup,
identity and call timing, and nonzero subprocess exit after valid totals.
Other controls cover successful flush, failed dial/HELLO, disabled flush,
connection-close failure, wrapper forwarding, all-error retention, receipt
identity, exclusive paths, write failure and file-close failure.

## Repository gates

```
GOWORK=off go test ./cmd/go-ycsb ./db/treedbnative ./pkg/client ./pkg/ycsb -count=1
GOWORK=off go test -race ./cmd/go-ycsb ./db/treedbnative ./pkg/client -count=1
GOWORK=off go test ./...
```

Run the repository AGENTS.md 1,000-record `load treedb` and `run treedb`
smokes in the same fresh task-owned directory. They are in-process client
package controls, not public COW or product performance qualification.

Pinned-base gates had seven Elasticsearch `fmt.Println` format-string vet
failures and a workload test loading `workloads/workloadc` relative to the
package directory. The minimal gate fixes render the intended formatted error
logs with newlines/string keys and use the correct test-only fixture path.
Their original failures are retained alongside the RED evidence.

No protocol compatibility overlay is included here. The pinned source still
selects native wire minor 0. A separate previously retained host185 overlay
selected minor 1 (nativewire.go SHA-256
`b09334ad0f53ea49ca553953b123a2617625c04a766dbed624e4058c2399f787`).
Any use of that compatibility adjustment needs its own disclosed patch/build/
ELF binding and actual server compatibility check; do not mix builds silently.

Observed local validation used Go 1.26.10 linux/amd64, CGO enabled, the existing
Go module/build cache, and `GOMAXPROCS=2` for package/test/smoke processes only.
Focused tests, full `go test ./...` (vet enabled), race tests, `make`, and both
required 1,000-record smokes passed. Exact argv/environment/exit records and
raw stdout, including a real failed original-close receipt, are retained in
`close-receipt-evidence`. The final focused/full/race runs use no build tags;
`make` and smoke builds selected system SQLite (`libsqlite3`). CI's configured
Go version is 1.25.5; remote CI is a separate current-head gate.

The first race compilation was stopped by its owner before fast4tb filled;
its SIGINT exit was an engineering interruption. Subsequent compilation used
`/home/mikers/ycsb-close-5335-20261029-tmp` because fast4tb had insufficient
headroom, and passed. Source, evidence and the task-owned smoke database stayed
on fast4tb. These controls establish producer/package behavior, not product
throughput, public COW activation, or server lifecycle acceptance.
