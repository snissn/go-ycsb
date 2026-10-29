// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ycsb

import (
	"context"
	"fmt"
	"time"

	"github.com/magiconair/properties"
)

// DBCreator creates a database layer.
type DBCreator interface {
	Create(p *properties.Properties) (DB, error)
}

// DB is the layer to access the database to be benchmarked.
type DB interface {
	// Close closes the database layer.
	Close() error

	// InitThread initializes the state associated to the goroutine worker.
	// The Returned context will be passed to the following usage.
	InitThread(ctx context.Context, threadID int, threadCount int) context.Context

	// CleanupThread cleans up the state when the worker finished.
	CleanupThread(ctx context.Context)

	// Read reads a record from the database and returns a map of each field/value pair.
	// table: The name of the table.
	// key: The record key of the record to read.
	// fields: The list of fields to read, nil|empty for reading all.
	Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error)

	// Scan scans records from the database.
	// table: The name of the table.
	// startKey: The first record key to read.
	// count: The number of records to read.
	// fields: The list of fields to read, nil|empty for reading all.
	Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error)

	// Update updates a record in the database. Any field/value pairs will be written into the
	// database or overwritten the existing values with the same field name.
	// table: The name of the table.
	// key: The record key of the record to update.
	// values: A map of field/value pairs to update in the record.
	Update(ctx context.Context, table string, key string, values map[string][]byte) error

	// Insert inserts a record in the database. Any field/value pairs will be written into the
	// database.
	// table: The name of the table.
	// key: The record key of the record to insert.
	// values: A map of field/value pairs to insert in the record.
	Insert(ctx context.Context, table string, key string, values map[string][]byte) error

	// Delete deletes a record from the database.
	// table: The name of the table.
	// key: The record key of the record to delete.
	Delete(ctx context.Context, table string, key string) error
}

type BatchDB interface {
	// BatchInsert inserts batch records in the database.
	// table: The name of the table.
	// keys: The keys of batch records.
	// values: The values of batch records.
	BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error

	// BatchRead reads records from the database.
	// table: The name of the table.
	// keys: The keys of records to read.
	// fields: The list of fields to read, nil|empty for reading all.
	BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error)

	// BatchUpdate updates records in the database.
	// table: The name of table.
	// keys: The keys of records to update.
	// values: The values of records to update.
	BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error

	// BatchDelete deletes records from the database.
	// table: The name of the table.
	// keys: The keys of the records to delete.
	BatchDelete(ctx context.Context, table string, keys []string) error
}

// AnalyzeDB is the interface for the DB that can perform an analysis on given table.
type AnalyzeDB interface {
	// Analyze performs a key distribution analysis for the table.
	// table: The name of the table.
	Analyze(ctx context.Context, table string) error
}

var dbCreators = map[string]DBCreator{}

// RegisterDBCreator registers a creator for the database
func RegisterDBCreator(name string, creator DBCreator) {
	_, ok := dbCreators[name]
	if ok {
		panic(fmt.Sprintf("duplicate register database %s", name))
	}

	dbCreators[name] = creator
}

// GetDBCreator gets the DBCreator for the database
func GetDBCreator(name string) DBCreator {
	return dbCreators[name]
}

// CloseReporter optionally exposes a snapshot of the original Close call.
// Reading it must never initiate another Close or flush.
type CloseReporter interface {
	CloseReport() DBCloseReport
}

// CloseResult describes one actual call. A skipped call has Attempted=false;
// Success is true only when that call returned nil.
type CloseResult struct {
	Attempted bool      `json:"attempted"`
	Success   bool      `json:"success"`
	Error     string    `json:"error"`
	StartUTC  time.Time `json:"start_utc"`
	EndUTC    time.Time `json:"end_utc"`
	ElapsedNS int64     `json:"elapsed_ns"`
}

// StartCloseResult starts an observation without affecting the measured call.
func StartCloseResult() (CloseResult, time.Time) {
	start := time.Now()
	return CloseResult{Attempted: true, StartUTC: start.UTC()}, start
}

// Finish records the actual return and monotonic elapsed duration.
func (r *CloseResult) Finish(start time.Time, err error) {
	end := time.Now()
	r.EndUTC, r.ElapsedNS, r.Success = end.UTC(), end.Sub(start).Nanoseconds(), err == nil
	if err != nil {
		r.Error = err.Error()
	}
}

// NativeFlushReport is not applicable for backends without a native flush.
// Disabled flush is explicitly skipped, never a successful required flush.
type NativeFlushReport struct {
	Applicable      bool        `json:"applicable"`
	FlushEnabled    bool        `json:"flush_enabled"`
	ACK             uint64      `json:"ack"`
	Dial            CloseResult `json:"dial"`
	Flush           CloseResult `json:"flush"`
	ConnectionClose CloseResult `json:"connection_close"`
}

// DBCloseReport contains backend-specific evidence from the original Close.
type DBCloseReport struct {
	NativeFlush NativeFlushReport `json:"native_flush"`
}
