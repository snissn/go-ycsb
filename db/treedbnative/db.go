package treedbnative

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	treedbNetworkProp          = "treedb.network"
	treedbAddressProp          = "treedb.address"
	treedbAddrProp             = "treedb.addr"
	treedbDialTimeoutProp      = "treedb.dial_timeout"
	treedbOpTimeoutProp        = "treedb.operation_timeout"
	treedbAutoCreateProp       = "treedb.autocreate"
	treedbCreateKeyIndexProp   = "treedb.create_key_index"
	treedbUseScanIndexProp     = "treedb.use_scan_index"
	treedbKeyFieldProp         = "treedb.key_field"
	treedbScanBatchSizeProp    = "treedb.scan_batch_size"
	treedbAckProp              = "treedb.ack"
	treedbFlushOnCloseProp     = "treedb.flush_on_close"
	treedbDefaultNetwork       = "tcp"
	treedbDefaultAddress       = "127.0.0.1:7100"
	treedbDefaultDialTimeout   = "2s"
	treedbDefaultOpTimeout     = "0s"
	treedbDefaultKeyField      = "_ycsb_key"
	treedbDefaultScanBatchSize = 1024
	treedbDefaultFlushOnClose  = true

	keyIndexName = "_ycsb_key_1"
)

type treedbContextKey string

const stateContextKey = treedbContextKey("treedb-state")

type treedbCreator struct{}

type treedbDB struct {
	network        string
	address        string
	dialTimeout    time.Duration
	operationLimit time.Duration
	autoCreate     bool
	createKeyIndex bool
	useScanIndex   bool
	keyField       string
	scanBatchSize  int
	ack            uint64
	flushOnClose   bool

	prepareMu sync.Mutex
	prepared  map[string]struct{}

	clientsMu sync.Mutex
	clients   map[*nativeWireClient]struct{}

	fallbackMu sync.Mutex
	fallback   *treedbState

	loggedErrors atomic.Int64
}

type treedbState struct {
	client          *nativeWireClient
	err             error
	handles         map[string]uint64
	scanIndexBroken map[string]bool
}

func init() {
	ycsb.RegisterDBCreator("treedb-native", treedbCreator{})
}

func (treedbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dialTimeout, err := parseDurationProperty(p, treedbDialTimeoutProp, treedbDefaultDialTimeout)
	if err != nil {
		return nil, err
	}
	operationLimit, err := parseDurationProperty(p, treedbOpTimeoutProp, treedbDefaultOpTimeout)
	if err != nil {
		return nil, err
	}
	ack, err := parseAckPolicy(p.GetString(treedbAckProp, "visible"))
	if err != nil {
		return nil, err
	}
	address := p.GetString(treedbAddressProp, "")
	if address == "" {
		address = p.GetString(treedbAddrProp, treedbDefaultAddress)
	}
	db := &treedbDB{
		network:        p.GetString(treedbNetworkProp, treedbDefaultNetwork),
		address:        address,
		dialTimeout:    dialTimeout,
		operationLimit: operationLimit,
		autoCreate:     p.GetBool(treedbAutoCreateProp, true),
		createKeyIndex: p.GetBool(treedbCreateKeyIndexProp, true),
		useScanIndex:   p.GetBool(treedbUseScanIndexProp, true),
		keyField:       p.GetString(treedbKeyFieldProp, treedbDefaultKeyField),
		scanBatchSize:  p.GetInt(treedbScanBatchSizeProp, treedbDefaultScanBatchSize),
		ack:            ack,
		flushOnClose:   p.GetBool(treedbFlushOnCloseProp, treedbDefaultFlushOnClose),
		prepared:       make(map[string]struct{}),
		clients:        make(map[*nativeWireClient]struct{}),
	}
	if db.scanBatchSize <= 0 {
		db.scanBatchSize = treedbDefaultScanBatchSize
	}
	if db.keyField == "" {
		db.keyField = treedbDefaultKeyField
	}
	ctx := context.Background()
	table := p.GetString(prop.TableName, prop.TableNameDefault)
	if db.autoCreate || p.GetBool(prop.DropData, prop.DropDataDefault) {
		if err := db.prepareCollection(ctx, table); err != nil {
			return nil, err
		}
	}
	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		if err := db.clearCollection(ctx, table); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func (db *treedbDB) Close() error {
	var out error
	if db.flushOnClose {
		ctx, cancel := db.operationContext(context.Background())
		client, err := db.dial(ctx)
		if err == nil {
			err = client.FlushAll(ctx, db.ack)
		}
		if client != nil {
			err = appendDBError(err, db.closeClient(client))
		}
		cancel()
		out = appendDBError(out, err)
	}

	db.clientsMu.Lock()
	clients := make([]*nativeWireClient, 0, len(db.clients))
	for client := range db.clients {
		clients = append(clients, client)
	}
	db.clients = make(map[*nativeWireClient]struct{})
	db.clientsMu.Unlock()

	for _, client := range clients {
		out = appendDBError(out, client.Close())
	}
	return out
}

func (db *treedbDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	opCtx, cancel := db.operationContext(ctx)
	defer cancel()
	client, err := db.dial(opCtx)
	state := &treedbState{
		client:          client,
		err:             err,
		handles:         make(map[string]uint64),
		scanIndexBroken: make(map[string]bool),
	}
	return context.WithValue(ctx, stateContextKey, state)
}

func (db *treedbDB) CleanupThread(ctx context.Context) {
	state, _ := ctx.Value(stateContextKey).(*treedbState)
	if state == nil || state.client == nil {
		return
	}
	_ = db.closeClient(state.client)
	state.client = nil
}

func (db *treedbDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	state, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		return nil, err
	}
	_ = state
	docs, present, err := client.GetMany(ctx, handle, keysToIDs([]string{key}))
	if err != nil {
		return nil, err
	}
	if len(present) == 0 || !present[0] {
		return nil, fmt.Errorf("treedb: key %q not found in table %q", key, table)
	}
	return db.decodeRow(docs[0], fields)
}

func (db *treedbDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	_, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		return nil, err
	}
	ids := keysToIDs(keys)
	docs, present, err := client.GetMany(ctx, handle, ids)
	if err != nil {
		return nil, err
	}
	out := make([]map[string][]byte, len(keys))
	for i, ok := range present {
		if !ok {
			return nil, fmt.Errorf("treedb: key %q not found in table %q", keys[i], table)
		}
		row, err := db.decodeRow(docs[i], fields)
		if err != nil {
			return nil, err
		}
		out[i] = row
	}
	return out, nil
}

func (db *treedbDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	state, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		return nil, err
	}
	if db.useScanIndex && db.createKeyIndex && !state.scanIndexBroken[table] {
		rows, err := db.scanByKeyIndex(ctx, client, handle, table, startKey, count, fields)
		if err == nil {
			return rows, nil
		}
		if !isWireRemoteError(err, wireErrIndexNotFound) && !isWireRemoteError(err, wireErrInvalidCommand) {
			return nil, err
		}
		state.scanIndexBroken[table] = true
	}
	return db.scanByCursor(ctx, client, handle, startKey, count, fields)
}

func (db *treedbDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	_, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		return err
	}
	docs, present, err := client.GetMany(ctx, handle, keysToIDs([]string{key}))
	if err != nil {
		return err
	}
	if len(present) == 0 || !present[0] {
		return fmt.Errorf("treedb: key %q not found in table %q", key, table)
	}
	row, err := db.decodeRow(docs[0], nil)
	if err != nil {
		return err
	}
	for field, value := range values {
		if field == db.keyField {
			continue
		}
		row[field] = value
	}
	doc, err := db.encodeRow(key, row)
	if err != nil {
		return err
	}
	return client.ReplaceBatch(ctx, handle, keysToIDs([]string{key}), [][]byte{doc}, db.ack)
}

func (db *treedbDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("treedb: keys length %d does not match values length %d", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	_, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		return err
	}
	ids := keysToIDs(keys)
	docs, present, err := client.GetMany(ctx, handle, ids)
	if err != nil {
		return err
	}
	replacements := make([][]byte, len(keys))
	for i, ok := range present {
		if !ok {
			return fmt.Errorf("treedb: key %q not found in table %q", keys[i], table)
		}
		row, err := db.decodeRow(docs[i], nil)
		if err != nil {
			return err
		}
		for field, value := range values[i] {
			if field == db.keyField {
				continue
			}
			row[field] = value
		}
		replacements[i], err = db.encodeRow(keys[i], row)
		if err != nil {
			return err
		}
	}
	return client.ReplaceBatch(ctx, handle, ids, replacements, db.ack)
}

func (db *treedbDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	_, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		db.logOperationError("insert_open", table, key, err)
		return err
	}
	doc, err := db.encodeRow(key, values)
	if err != nil {
		return err
	}
	err = client.InsertBatch(ctx, handle, keysToIDs([]string{key}), [][]byte{doc}, db.ack)
	if err != nil {
		db.logOperationError("insert", table, key, err)
	}
	return err
}

func (db *treedbDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("treedb: keys length %d does not match values length %d", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	_, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		key := ""
		if len(keys) > 0 {
			key = keys[0]
		}
		db.logOperationError("batch_insert_open", table, key, err)
		return err
	}
	ids := keysToIDs(keys)
	docs := make([][]byte, len(keys))
	for i := range keys {
		docs[i], err = db.encodeRow(keys[i], values[i])
		if err != nil {
			return err
		}
	}
	err = client.InsertBatch(ctx, handle, ids, docs, db.ack)
	if err != nil {
		key := ""
		if len(keys) > 0 {
			key = keys[0]
		}
		db.logOperationError("batch_insert", table, key, err)
	}
	return err
}

func (db *treedbDB) Delete(ctx context.Context, table string, key string) error {
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	_, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		return err
	}
	return client.DeleteBatch(ctx, handle, keysToIDs([]string{key}), db.ack)
}

func (db *treedbDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := db.operationContext(ctx)
	defer cancel()
	_, client, handle, err := db.clientAndHandle(ctx, table)
	if err != nil {
		return err
	}
	return client.DeleteBatch(ctx, handle, keysToIDs(keys), db.ack)
}

func (db *treedbDB) Analyze(context.Context, string) error {
	return nil
}

func (db *treedbDB) clientAndHandle(ctx context.Context, table string) (*treedbState, *nativeWireClient, uint64, error) {
	state, err := db.state(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	handle, ok := state.handles[table]
	if ok {
		return state, state.client, handle, nil
	}
	handle, err = state.client.OpenCollection(ctx, table)
	if err != nil && isWireRemoteError(err, wireErrCollectionNotFound) && db.autoCreate {
		if prepareErr := db.prepareCollection(ctx, table); prepareErr != nil {
			return nil, nil, 0, prepareErr
		}
		handle, err = state.client.OpenCollection(ctx, table)
	}
	if err != nil {
		return nil, nil, 0, err
	}
	state.handles[table] = handle
	return state, state.client, handle, nil
}

func (db *treedbDB) state(ctx context.Context) (*treedbState, error) {
	if state, ok := ctx.Value(stateContextKey).(*treedbState); ok && state != nil {
		if state.err != nil {
			return nil, state.err
		}
		if state.client == nil {
			return nil, fmt.Errorf("treedb: thread client is closed")
		}
		return state, nil
	}
	db.fallbackMu.Lock()
	defer db.fallbackMu.Unlock()
	if db.fallback != nil {
		if db.fallback.err != nil {
			return nil, db.fallback.err
		}
		return db.fallback, nil
	}
	client, err := db.dial(ctx)
	db.fallback = &treedbState{
		client:          client,
		err:             err,
		handles:         make(map[string]uint64),
		scanIndexBroken: make(map[string]bool),
	}
	if err != nil {
		return nil, err
	}
	return db.fallback, nil
}

func (db *treedbDB) dial(ctx context.Context) (*nativeWireClient, error) {
	client, err := dialNativeWire(ctx, db.network, db.address, db.dialTimeout)
	if err != nil {
		return nil, err
	}
	db.clientsMu.Lock()
	db.clients[client] = struct{}{}
	db.clientsMu.Unlock()
	return client, nil
}

func (db *treedbDB) closeClient(client *nativeWireClient) error {
	if client == nil {
		return nil
	}
	db.clientsMu.Lock()
	delete(db.clients, client)
	db.clientsMu.Unlock()
	return client.Close()
}

func (db *treedbDB) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db.operationLimit <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, db.operationLimit)
}

func (db *treedbDB) prepareCollection(ctx context.Context, table string) error {
	db.prepareMu.Lock()
	defer db.prepareMu.Unlock()
	if _, ok := db.prepared[table]; ok {
		return nil
	}
	client, err := db.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = db.closeClient(client) }()

	_, err = client.OpenCollection(ctx, table)
	if err != nil {
		if !isWireRemoteError(err, wireErrCollectionNotFound) {
			return err
		}
		if !db.autoCreate {
			return err
		}
		if createErr := client.CreateCollection(ctx, table, db.createKeyIndex, db.keyField); createErr != nil {
			if _, retryErr := client.OpenCollection(ctx, table); retryErr != nil {
				return createErr
			}
		}
	}
	if db.createKeyIndex {
		if err := db.ensureKeyIndex(ctx, client, table); err != nil {
			return err
		}
	}
	db.prepared[table] = struct{}{}
	return nil
}

func (db *treedbDB) ensureKeyIndex(ctx context.Context, client *nativeWireClient, table string) error {
	if ok, err := db.hasKeyIndex(ctx, client, table); err != nil {
		return err
	} else if ok {
		return nil
	}
	if err := client.CreateIndex(ctx, table, keyIndexName, db.keyField, true); err != nil {
		ok, checkErr := db.hasKeyIndex(ctx, client, table)
		if checkErr == nil && ok {
			return nil
		}
		return err
	}
	return nil
}

func (db *treedbDB) hasKeyIndex(ctx context.Context, client *nativeWireClient, table string) (bool, error) {
	indexes, err := client.ListIndexes(ctx, table)
	if err != nil {
		return false, err
	}
	for _, index := range indexes {
		if index.Name == keyIndexName && index.Field == db.keyField {
			return true, nil
		}
	}
	return false, nil
}

func (db *treedbDB) clearCollection(ctx context.Context, table string) error {
	client, err := db.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = db.closeClient(client) }()
	handle, err := client.OpenCollection(ctx, table)
	if err != nil {
		if isWireRemoteError(err, wireErrCollectionNotFound) {
			return nil
		}
		return err
	}
	for {
		res, err := client.OpenScan(ctx, handle, db.scanBatchSize)
		if err != nil {
			return err
		}
		cursorID := res.Cursor.CursorID
		deleted := 0
		for {
			if len(res.IDs) > 0 {
				if err := client.DeleteBatch(ctx, handle, res.IDs, db.ack); err != nil {
					_ = client.CursorClose(ctx, cursorID)
					return err
				}
				deleted += len(res.IDs)
			}
			if !res.Cursor.HasMore {
				break
			}
			res, err = client.CursorNext(ctx, cursorID, db.scanBatchSize)
			if err != nil {
				_ = client.CursorClose(ctx, cursorID)
				return err
			}
		}
		if cursorID != 0 {
			_ = client.CursorClose(ctx, cursorID)
		}
		if deleted == 0 {
			return nil
		}
	}
}

func (db *treedbDB) scanByKeyIndex(ctx context.Context, client *nativeWireClient, handle uint64, table, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	ids, _, err := client.IndexRange(ctx, handle, keyIndexName, startKey, count)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	docs, present, err := client.GetMany(ctx, handle, ids)
	if err != nil {
		return nil, err
	}
	out := make([]map[string][]byte, 0, len(ids))
	for i, ok := range present {
		if !ok {
			return nil, fmt.Errorf("treedb: indexed key %q not found in table %q", string(ids[i]), table)
		}
		row, err := db.decodeRow(docs[i], fields)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (db *treedbDB) scanByCursor(ctx context.Context, client *nativeWireClient, handle uint64, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	out := make([]map[string][]byte, 0, count)
	res, err := client.OpenScan(ctx, handle, db.scanBatchSize)
	if err != nil {
		return nil, err
	}
	cursorID := res.Cursor.CursorID
	defer func() { _ = client.CursorClose(context.Background(), cursorID) }()
	for {
		for i, id := range res.IDs {
			if len(out) >= count {
				return out, nil
			}
			if string(id) < startKey {
				continue
			}
			if i >= len(res.Docs) {
				return nil, fmt.Errorf("treedb: scan returned %d ids but only %d docs", len(res.IDs), len(res.Docs))
			}
			row, err := db.decodeRow(res.Docs[i], fields)
			if err != nil {
				return nil, err
			}
			out = append(out, row)
		}
		if len(out) >= count || !res.Cursor.HasMore {
			return out, nil
		}
		res, err = client.CursorNext(ctx, cursorID, db.scanBatchSize)
		if err != nil {
			return nil, err
		}
	}
}

func (db *treedbDB) encodeRow(key string, values map[string][]byte) ([]byte, error) {
	row := make(map[string]interface{}, len(values)+1)
	for field, value := range values {
		if field == db.keyField {
			continue
		}
		row[field] = value
	}
	row[db.keyField] = key
	return json.Marshal(row)
}

func (db *treedbDB) decodeRow(doc []byte, fields []string) (map[string][]byte, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(doc, &raw); err != nil {
		return nil, err
	}
	wanted := fieldSet(fields)
	out := make(map[string][]byte, len(raw))
	for field, encoded := range raw {
		if field == db.keyField {
			continue
		}
		if wanted != nil {
			if _, ok := wanted[field]; !ok {
				continue
			}
		}
		var value []byte
		if err := json.Unmarshal(encoded, &value); err == nil {
			out[field] = value
			continue
		}
		var text string
		if err := json.Unmarshal(encoded, &text); err == nil {
			out[field] = []byte(text)
			continue
		}
		out[field] = append([]byte(nil), encoded...)
	}
	return out, nil
}

func keysToIDs(keys []string) [][]byte {
	ids := make([][]byte, len(keys))
	for i, key := range keys {
		ids[i] = []byte(key)
	}
	return ids
}

func fieldSet(fields []string) map[string]struct{} {
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		out[field] = struct{}{}
	}
	return out
}

func parseDurationProperty(p *properties.Properties, key, fallback string) (time.Duration, error) {
	raw := p.GetString(key, fallback)
	if raw == "" || raw == "0" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("treedb: invalid %s=%q: %w", key, raw, err)
	}
	return d, nil
}

func parseAckPolicy(raw string) (uint64, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "none", "0":
		return 0, nil
	case "visible", "ack_visible", "1":
		return wireAckVisible, nil
	case "flushed", "flush", "ack_flushed", "2":
		return wireAckFlushed, nil
	case "synced", "sync", "ack_synced", "3":
		return wireAckSynced, nil
	default:
		return 0, fmt.Errorf("treedb: unsupported %s=%q", treedbAckProp, raw)
	}
}

func (db *treedbDB) logOperationError(op, table, key string, err error) {
	if err == nil || os.Getenv("TREEDB_YCSB_LOG_ERRORS") == "" {
		return
	}
	if n := db.loggedErrors.Add(1); n <= 100 {
		fmt.Fprintf(os.Stderr, "treedb %s error table=%s key=%s err=%v\n", op, table, key, err)
	}
}

func appendDBError(base, next error) error {
	if base == nil {
		return next
	}
	if next == nil {
		return base
	}
	return fmt.Errorf("%v; %w", base, next)
}
