package client

import (
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"testing"
)

type reportDB struct {
	ycsb.DB
	calls int
}

func (db *reportDB) Close() error { db.calls++; return nil }
func (db *reportDB) CloseReport() ycsb.DBCloseReport {
	return ycsb.DBCloseReport{NativeFlush: ycsb.NativeFlushReport{Applicable: true, FlushEnabled: true, ACK: 3}}
}

type noReportDB struct{ ycsb.DB }

func TestDbWrapperForwardsCloseReportWithoutClosing(t *testing.T) {
	db := &reportDB{}
	wrapper := DbWrapper{DB: db}
	if wrapper.CloseReport() != db.CloseReport() || db.calls != 0 {
		t.Fatal("report not forwarded or Close called")
	}
	if err := wrapper.Close(); err != nil {
		t.Fatal(err)
	}
	_ = wrapper.CloseReport()
	if db.calls != 1 {
		t.Fatal("report repeated Close")
	}
	if (DbWrapper{DB: &noReportDB{}}).CloseReport().NativeFlush.Applicable {
		t.Fatal("unsupported report must be not applicable")
	}
}
