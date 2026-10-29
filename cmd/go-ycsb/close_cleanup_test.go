package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type closeOnlyDB struct {
	ycsb.DB
	err   error
	calls int
}

func (d *closeOnlyDB) Close() error { d.calls++; return d.err }

type closeOnlyWorkload struct {
	err   error
	calls int
}

func (w *closeOnlyWorkload) Close() error { w.calls++; return w.err }
func TestCleanupPreservesAllErrorsAndContinues(t *testing.T) {
	commandErr, dbErr, workloadErr := errors.New("command"), errors.New("db"), errors.New("workload")
	d, w := &closeOnlyDB{err: dbErr}, &closeOnlyWorkload{err: workloadErr}
	err := finishCommand(commandErr, d, w, receiptOptions{})
	for _, want := range []error{commandErr, dbErr, workloadErr} {
		if !errors.Is(err, want) {
			t.Fatalf("lost %v: %v", want, err)
		}
	}
	if d.calls != 1 || w.calls != 1 {
		t.Fatalf("cleanup calls: db=%d workload=%d", d.calls, w.calls)
	}
}
func TestCloseReceiptExclusiveAndWriteFailureStillCleansUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	options := receiptOptions{Path: path, PhaseID: "phase", ClientBuildID: "build"}
	d, w := &closeOnlyDB{}, &closeOnlyWorkload{}
	if err := finishCommand(nil, d, w, options); err != nil {
		t.Fatal(err)
	}
	original, _ := os.ReadFile(path)
	if err := finishCommand(nil, d, w, options); err == nil {
		t.Fatal("reused receipt accepted")
	}
	after, _ := os.ReadFile(path)
	if string(original) != string(after) {
		t.Fatal("existing receipt changed")
	}
	options.Path = filepath.Join(t.TempDir(), "absent", "receipt.json")
	if err := finishCommand(nil, d, w, options); err == nil {
		t.Fatal("write failure accepted")
	}
	if d.calls != 3 || w.calls != 3 {
		t.Fatal("receipt failure skipped cleanup")
	}
}

type failingReceiptWriter struct {
	writeErr, closeErr error
	short              bool
	closed             bool
}

func (w *failingReceiptWriter) Write(b []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.short {
		return len(b) - 1, nil
	}
	return len(b), nil
}
func (w *failingReceiptWriter) Close() error { w.closed = true; return w.closeErr }
func TestCloseReceiptChecksWriteAndClose(t *testing.T) {
	for _, which := range []string{"write", "close", "short"} {
		t.Run(which, func(t *testing.T) {
			want := errors.New(which)
			w := &failingReceiptWriter{}
			if which == "write" {
				w.writeErr = want
			} else if which == "close" {
				w.closeErr = want
			} else {
				want = io.ErrShortWrite
				w.short = true
			}
			err := writeCloseReceipt("unused", closeReceipt{}, func(string) (io.WriteCloser, error) { return w, nil })
			if !errors.Is(err, want) || !w.closed {
				t.Fatalf("unchecked %s: %v closed=%v", which, err, w.closed)
			}
		})
	}
}

func TestMissingReceiptIdentityStillCleansUp(t *testing.T) {
	d, w := &closeOnlyDB{}, &closeOnlyWorkload{}
	if err := finishCommand(nil, d, w, receiptOptions{Path: filepath.Join(t.TempDir(), "receipt.json")}); err == nil {
		t.Fatal("missing identity accepted")
	}
	if d.calls != 1 || w.calls != 1 {
		t.Fatal("identity failure skipped cleanup")
	}
}

func TestCommandAndReceiptWriteFailuresAreFatal(t *testing.T) {
	if mode := os.Getenv("YCSB_ERROR_TEST_CHILD"); mode != "" {
		if mode == "command" {
			os.Args = []string{"go-ycsb", "unknown-command"}
		} else {
			os.Args = []string{"go-ycsb", "load", "basic", "-p", "recordcount=1", "-p", "operationcount=1", "-p", "threadcount=1"}
		}
		main()
		os.Exit(0)
	}
	for _, mode := range []string{"command", "receipt-write"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "receipt.json")
			if mode == "receipt-write" {
				path = filepath.Join(t.TempDir(), "absent", "receipt.json")
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCommandAndReceiptWriteFailuresAreFatal$")
			cmd.Env = append(os.Environ(), "YCSB_ERROR_TEST_CHILD="+mode, "YCSB_CLOSE_RECEIPT_PATH="+path, "YCSB_PHASE_ID=error-control", "YCSB_CLIENT_BUILD_ID=test")
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("%s exited successfully: %s", mode, out)
			}
			if mode == "command" {
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var receipt closeReceipt
				if err := json.Unmarshal(raw, &receipt); err != nil {
					t.Fatal(err)
				}
				if receipt.Command.Success || receipt.Command.Error == "" {
					t.Fatalf("lost command failure: %s", raw)
				}
			} else if !strings.Contains(string(out), "TOTAL") || !strings.Contains(string(out), "create close receipt") {
				t.Fatalf("write failure not after TOTAL: %s", out)
			}
		})
	}
}
