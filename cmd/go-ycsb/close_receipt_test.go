package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloseFailureAfterTotalsIsFatal(t *testing.T) {
	if os.Getenv("YCSB_CLOSE_TEST_CHILD") == "1" {
		ycsb.RegisterWorkloadCreator("close-test", closeTestWorkloadCreator{})
		os.Args = []string{"go-ycsb", "load", "treedb-native", "-p", "treedb.address=" + os.Getenv("YCSB_CLOSE_TEST_ADDR"), "-p", "recordcount=2", "-p", "operationcount=2", "-p", "threadcount=1", "-p", "fieldcount=1", "-p", "fieldlength=4", "-p", "treedb.autocreate=false", "-p", "treedb.create_key_index=false", "-p", "treedb.operation_timeout=2s", "-p", "workload=close-test"}
		main()
		os.Exit(0)
	}
	addr, counts, stop := startCloseTestServer(t)
	defer stop()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCloseFailureAfterTotalsIsFatal$")
	receiptPath := filepath.Join(t.TempDir(), "close.json")
	marker := filepath.Join(t.TempDir(), "workload-closed")
	cmd.Env = append(os.Environ(), "YCSB_CLOSE_RECEIPT_PATH="+receiptPath, "YCSB_PHASE_ID=test-load", "YCSB_CLIENT_BUILD_ID=test-build", "YCSB_PROFILE=test-profile", "YCSB_CLOSE_TEST_MARKER="+marker, "YCSB_CLOSE_TEST_CHILD=1", "YCSB_CLOSE_TEST_ADDR="+addr)
	out, err := cmd.CombinedOutput()
	t.Logf("original client output:\n%s", out)
	if !regexp.MustCompile(`(?m)^TOTAL  - .*, Count: 2,`).Match(out) || !regexp.MustCompile(`(?m)^INSERT - .*, Count: 2,`).Match(out) || strings.Contains(string(out), "INSERT_ERROR") {
		t.Fatalf("ordinary operations did not succeed: %s", out)
	}
	if counts.Load() != 1 {
		t.Fatalf("original FlushAll calls=%d want 1", counts.Load())
	}
	if err == nil {
		t.Fatal("client exited successfully after original Close/FlushAll failed")
	}
	raw, readErr := os.ReadFile(receiptPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	t.Logf("original close receipt:\n%s", raw)
	var receipt closeReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.SchemaVersion != 1 || receipt.PhaseID != "test-load" || receipt.ClientBuildID != "test-build" || receipt.DeclaredProfile != "test-profile" || receipt.DB != "treedb-native" || receipt.Phase != "load" || !receipt.TotalExcludesClose {
		t.Fatalf("receipt identity: %+v", receipt)
	}
	if !receipt.Command.Success || !receipt.DBClose.Attempted || receipt.DBClose.Success || receipt.DBClose.Error == "" || !receipt.WorkloadClose.Success || !receipt.NativeFlush.Flush.Attempted || receipt.NativeFlush.Flush.Success || receipt.NativeFlush.Flush.Error == "" || !receipt.NativeFlush.Dial.Success || receipt.NativeFlush.ACK != 1 {
		t.Fatalf("receipt actual failure: %+v", receipt)
	}
	if receipt.NativeFlush.Flush.StartUTC.Before(receipt.DBClose.StartUTC) || receipt.NativeFlush.Flush.EndUTC.After(receipt.DBClose.EndUTC) || receipt.NativeFlush.Flush.ElapsedNS <= 0 || receipt.DBClose.ElapsedNS < receipt.NativeFlush.Flush.ElapsedNS {
		t.Fatalf("receipt call ordering: %+v", receipt)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("workload cleanup did not run: %v", err)
	}

}

// This public protocol peer accepts HELLO and ordinary inserts; only the actual
// final FlushAll is disconnected. It never injects an operation error.
func startCloseTestServer(t *testing.T) (string, *atomic.Int64, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var count atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				c.SetDeadline(time.Now().Add(10 * time.Second))
				for {
					h := make([]byte, 40)
					if _, err := io.ReadFull(c, h); err != nil {
						return
					}
					n := binary.LittleEndian.Uint64(h[32:])
					if n > 1<<20 {
						return
					}
					b := make([]byte, n)
					if _, err := io.ReadFull(c, b); err != nil {
						return
					}
					typ := binary.LittleEndian.Uint16(h[10:])
					var response []byte
					if typ == 1 {
						binary.LittleEndian.PutUint16(h[10:], 2)
					} else {
						sec := testSections(b)
						command, _ := binary.Uvarint(sec[1])
						switch command {
						case 34:
							ack, _ := binary.Uvarint(sec[6])
							if ack != 1 {
								t.Errorf("actual FlushAll ACK=%d want visible=1", ack)
							}
							count.Add(1)
							return
						case 15:
							response = testSection(110, []byte{1})
						case 57:
							raw := binary.AppendUvarint(nil, 1)
							key := "treedb.native_wire.catalog.version"
							raw = binary.AppendUvarint(raw, uint64(len(key)))
							raw = append(raw, key...)
							raw = append(raw, 1, '1')
							response = testSection(11, raw)
						case 11:
							meta := []byte{2, 9}
							meta = append(meta, "usertable"...)
							meta = append(meta, 2)
							meta = append(meta, make([]byte, 13)...)
							vec := []byte{1}
							vec = binary.AppendUvarint(vec, uint64(len(meta)))
							vec = append(vec, meta...)
							response = testSection(107, vec)
						case 30: // accepted insert
						default:
							t.Errorf("unexpected command %d", command)
							return
						}
						binary.LittleEndian.PutUint16(h[10:], 4)
					}
					binary.LittleEndian.PutUint64(h[32:], uint64(len(response)))
					if _, err := c.Write(append(h, response...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String(), &count, func() { ln.Close(); wg.Wait() }
}
func testSection(id uint64, b []byte) []byte {
	out := binary.AppendUvarint(nil, id)
	out = append(out, 0)
	out = binary.AppendUvarint(out, uint64(len(b)))
	return append(out, b...)
}
func testSections(b []byte) map[uint64][]byte {
	out := map[uint64][]byte{}
	for len(b) > 0 {
		id, n := binary.Uvarint(b)
		if n <= 0 {
			return out
		}
		b = b[n:]
		_, n = binary.Uvarint(b)
		if n <= 0 {
			return out
		}
		b = b[n:]
		l, n := binary.Uvarint(b)
		if n <= 0 || l > uint64(len(b)-n) {
			return out
		}
		b = b[n:]
		out[id] = b[:l]
		b = b[l:]
	}
	return out
}

// Embed the real core workload so ordinary generation and measurement remain intact.
type closeTestWorkloadCreator struct{}

func (closeTestWorkloadCreator) Create(p *properties.Properties) (ycsb.Workload, error) {
	w, err := ycsb.GetWorkloadCreator("core").Create(p)
	return &closeTestWorkload{Workload: w}, err
}

type closeTestWorkload struct{ ycsb.Workload }

func (w *closeTestWorkload) Close() error {
	return errors.Join(w.Workload.Close(), os.WriteFile(os.Getenv("YCSB_CLOSE_TEST_MARKER"), []byte("closed"), 0600))
}
