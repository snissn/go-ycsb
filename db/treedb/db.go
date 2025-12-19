// Copyright 2024 PingCAP, Inc.
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

package treedb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/magiconair/properties"
	treedb "github.com/snissn/gomap/TreeDB"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	treeDBDir                    = "treedb.dir"
	treeDBMode                   = "treedb.mode"
	treeDBChunkSize              = "treedb.chunk_size"
	treeDBKeepRecent             = "treedb.keep_recent"
	treeDBFlushThreshold         = "treedb.flush_threshold"
	treeDBPreferAppendAlloc      = "treedb.prefer_append_alloc"
	treeDBLeafFillPPM            = "treedb.leaf_fill_ppm"
	treeDBInternalFillPPM        = "treedb.internal_fill_ppm"
	treeDBMaxQueuedMemtables     = "treedb.max_queued_memtables"
	treeDBSlowdownBacklogSec     = "treedb.slowdown_backlog_seconds"
	treeDBStopBacklogSec         = "treedb.stop_backlog_seconds"
	treeDBMaxBacklogBytes        = "treedb.max_backlog_bytes"
	treeDBWriterFlushMaxMems     = "treedb.writer_flush_max_memtables"
	treeDBWriterFlushMaxMs       = "treedb.writer_flush_max_ms"
	treeDBFlushBuildConcurrency  = "treedb.flush_build_concurrency"
	treeDBDisableBackgroundPrune = "treedb.disable_background_prune"
)

type treeDBCreator struct{}

type treeDB struct {
	p *properties.Properties

	db *treedb.DB

	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c treeDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(treeDBDir, "/tmp/treedb")
	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		_ = os.RemoveAll(dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	opts := treedb.Options{
		Dir:                     dir,
		ChunkSize:               p.GetInt64(treeDBChunkSize, 0),
		KeepRecent:              p.GetUint64(treeDBKeepRecent, 0),
		FlushThreshold:          p.GetInt64(treeDBFlushThreshold, 0),
		PreferAppendAlloc:       p.GetBool(treeDBPreferAppendAlloc, false),
		MaxQueuedMemtables:      p.GetInt(treeDBMaxQueuedMemtables, 0),
		SlowdownBacklogSeconds:  p.GetFloat64(treeDBSlowdownBacklogSec, 0),
		StopBacklogSeconds:      p.GetFloat64(treeDBStopBacklogSec, 0),
		MaxBacklogBytes:         p.GetInt64(treeDBMaxBacklogBytes, 0),
		WriterFlushMaxMemtables: p.GetInt(treeDBWriterFlushMaxMems, 0),
		FlushBuildConcurrency:   p.GetInt(treeDBFlushBuildConcurrency, 0),
		DisableBackgroundPrune:  p.GetBool(treeDBDisableBackgroundPrune, false),
	}

	if v := p.GetInt(treeDBLeafFillPPM, 0); v > 0 {
		opts.LeafFillTargetPPM = uint32(v)
	}
	if v := p.GetInt(treeDBInternalFillPPM, 0); v > 0 {
		opts.InternalFillTargetPPM = uint32(v)
	}
	if ms := p.GetInt(treeDBWriterFlushMaxMs, 0); ms > 0 {
		opts.WriterFlushMaxDuration = time.Duration(ms) * time.Millisecond
	}

	mode := strings.ToLower(p.GetString(treeDBMode, "cached"))
	var db *treedb.DB
	var err error
	switch mode {
	case "", "cached":
		db, err = treedb.Open(opts)
	case "backend":
		db, err = treedb.OpenBackend(opts)
	default:
		return nil, fmt.Errorf("unknown treedb.mode %q", mode)
	}
	if err != nil {
		return nil, err
	}

	return &treeDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func (db *treeDB) Close() error {
	return db.db.Close()
}

func (db *treeDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *treeDB) CleanupThread(_ context.Context) {
}

func (db *treeDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *treeDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	rowKey := db.getRowKey(table, key)
	value, err := db.db.Get(rowKey)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return map[string][]byte{}, nil
	}
	row := append([]byte(nil), value...)
	return db.r.Decode(row, fields)
}

func (db *treeDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, len(keys))
	for i, key := range keys {
		values, err := db.Read(ctx, table, key, fields)
		if err != nil {
			return nil, err
		}
		res[i] = values
	}
	return res, nil
}

func (db *treeDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	rowStartKey := db.getRowKey(table, startKey)
	it, err := db.db.Iterator(rowStartKey, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	i := 0
	for it.Valid() && i < count {
		if tomb, ok := it.(interface{ IsDeleted() bool }); ok && tomb.IsDeleted() {
			it.Next()
			continue
		}
		row := it.ValueCopy(nil)
		m, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
		i++
		it.Next()
	}

	if err := it.Error(); err != nil {
		return nil, err
	}
	return res, nil
}

func (db *treeDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}
	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()
	buf, err = db.r.Encode(buf, m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)
	return db.db.Set(rowKey, buf)
}

func (db *treeDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	batch := db.db.NewBatch()
	defer batch.Close()

	for i, key := range keys {
		m, err := db.Read(ctx, table, key, nil)
		if err != nil {
			return err
		}
		for field, value := range values[i] {
			m[field] = value
		}

		buf := db.bufPool.Get()
		buf, err = db.r.Encode(buf, m)
		if err != nil {
			db.bufPool.Put(buf)
			return err
		}
		row := append([]byte(nil), buf...)
		db.bufPool.Put(buf)

		rowKey := db.getRowKey(table, key)
		if err := batch.Set(rowKey, row); err != nil {
			return err
		}
	}

	return batch.Write()
}

func (db *treeDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()
	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)
	return db.db.Set(rowKey, buf)
}

func (db *treeDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	batch := db.db.NewBatch()
	defer batch.Close()

	for i, key := range keys {
		buf := db.bufPool.Get()
		buf, err := db.r.Encode(buf, values[i])
		if err != nil {
			db.bufPool.Put(buf)
			return err
		}
		row := append([]byte(nil), buf...)
		db.bufPool.Put(buf)

		rowKey := db.getRowKey(table, key)
		if err := batch.Set(rowKey, row); err != nil {
			return err
		}
	}

	return batch.Write()
}

func (db *treeDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)
	return db.db.Delete(rowKey)
}

func (db *treeDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	batch := db.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		rowKey := db.getRowKey(table, key)
		if err := batch.Delete(rowKey); err != nil {
			return err
		}
	}

	return batch.Write()
}

func init() {
	ycsb.RegisterDBCreator("treedb", treeDBCreator{})
}
