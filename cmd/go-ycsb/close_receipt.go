package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type closeReceipt struct {
	SchemaVersion      int                    `json:"schema_version"`
	PhaseID            string                 `json:"phase_id"`
	ClientBuildID      string                 `json:"client_build_id"`
	DB                 string                 `json:"db"`
	Phase              string                 `json:"phase"`
	DeclaredProfile    string                 `json:"declared_profile"`
	TotalExcludesClose bool                   `json:"total_excludes_close"`
	Command            ycsb.CloseResult       `json:"command"`
	DBClose            ycsb.CloseResult       `json:"db_close"`
	WorkloadClose      ycsb.CloseResult       `json:"workload_close"`
	NativeFlush        ycsb.NativeFlushReport `json:"native_flush"`
}

type receiptOptions struct{ Path, PhaseID, ClientBuildID, DB, Phase, Profile string }

func receiptEnvironment() receiptOptions {
	options := receiptOptions{Path: os.Getenv("YCSB_CLOSE_RECEIPT_PATH"), PhaseID: os.Getenv("YCSB_PHASE_ID"), ClientBuildID: os.Getenv("YCSB_CLIENT_BUILD_ID"), Profile: os.Getenv("YCSB_PROFILE"), DB: dbName}
	if globalProps != nil {
		options.Phase = globalProps.GetString("command", "")
	}
	return options
}

// finishCommand is shared by production and tests. It observes the original
// calls, always finishes cleanup, then writes the optional receipt. TOTAL is
// already emitted by runClientCommandFunc; no measurement boundary moves.
func finishCommand(commandErr error, db ycsb.DB, workload interface{ Close() error }, options receiptOptions) error {
	r := closeReceipt{SchemaVersion: 1, PhaseID: options.PhaseID, ClientBuildID: options.ClientBuildID, DB: options.DB, Phase: options.Phase, DeclaredProfile: options.Profile, TotalExcludesClose: true}
	r.Command.Attempted, r.Command.Success = true, commandErr == nil
	if commandErr != nil {
		r.Command.Error = commandErr.Error()
	}
	var dbErr, workloadErr error
	if db != nil {
		result, start := ycsb.StartCloseResult()
		dbErr = db.Close()
		result.Finish(start, dbErr)
		r.DBClose = result
		if reporter, ok := db.(ycsb.CloseReporter); ok {
			r.NativeFlush = reporter.CloseReport().NativeFlush
		}
	}
	if workload != nil {
		result, start := ycsb.StartCloseResult()
		workloadErr = workload.Close()
		result.Finish(start, workloadErr)
		r.WorkloadClose = result
	}
	var receiptErr error
	if options.Path != "" {
		if options.PhaseID == "" || options.ClientBuildID == "" {
			receiptErr = errors.New("close receipt requires YCSB_PHASE_ID and YCSB_CLIENT_BUILD_ID")
		} else {
			receiptErr = writeCloseReceipt(options.Path, r, func(path string) (io.WriteCloser, error) {
				return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			})
		}
	}
	return errors.Join(commandErr, dbErr, workloadErr, receiptErr)
}

func writeCloseReceipt(path string, receipt closeReceipt, open func(string) (io.WriteCloser, error)) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("encode close receipt: %w", err)
	}
	data = append(data, '\n')
	file, err := open(path)
	if err != nil {
		return fmt.Errorf("create close receipt: %w", err)
	}
	written, writeErr := file.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return fmt.Errorf("write/close receipt: %w", err)
	}
	return nil
}
