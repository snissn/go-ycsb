package treedbnative

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	wireMagic            = "TDB1"
	wireProtocolMajorV1  = uint16(1)
	wireProtocolMinorV0  = uint16(0)
	wireFrameHeaderLenV1 = uint16(40)
	wireMaxFrameSize     = uint64(64 << 20)
	wireMaxHeaderLen     = wireFrameHeaderLenV1
	wireMaxSections      = 1024
	wireMaxSectionLen    = uint64(16 << 20)
	wireMaxVectorItems   = 1 << 20
	wireMaxVectorBytes   = uint64(64 << 20)
	wireMaxStringMap     = 4096
	wireMaxBufferedBody  = 32 << 10
)

const maxInt = int(^uint(0) >> 1)

type wireFrameType uint16

const (
	wireFrameHello wireFrameType = iota + 1
	wireFrameHelloOK
	wireFrameRequest
	wireFrameResponse
	wireFrameData
	wireFrameError
	wireFrameCancel
	wireFramePing
	wireFramePong
	wireFrameGoaway
)

type wireSectionID uint64

const (
	wireSectionCommandHeader    wireSectionID = 1
	wireSectionError            wireSectionID = 2
	wireSectionAckPolicy        wireSectionID = 6
	wireSectionIdempotencyKey   wireSectionID = 8
	wireSectionResponseMeta     wireSectionID = 11
	wireSectionCursorMeta       wireSectionID = 12
	wireSectionCollectionRef    wireSectionID = 100
	wireSectionDocumentFormat   wireSectionID = 101
	wireSectionDocumentIDs      wireSectionID = 102
	wireSectionDocuments        wireSectionID = 103
	wireSectionExpectedCatalog  wireSectionID = 105
	wireSectionReplacementMode  wireSectionID = 106
	wireSectionCollectionMeta   wireSectionID = 107
	wireSectionIndexDefinition  wireSectionID = 108
	wireSectionIndexName        wireSectionID = 109
	wireSectionCollectionHandle wireSectionID = 110
	wireSectionIndexLowerBound  wireSectionID = 112
	wireSectionIndexUpperBound  wireSectionID = 113
	wireSectionCursorRef        wireSectionID = 114
	wireSectionCursorLimits     wireSectionID = 115
	wireSectionPresenceBitmap   wireSectionID = 116
	wireSectionTruncated        wireSectionID = 117
)

type wireCommandID uint64

const (
	wireCommandCreateCollection wireCommandID = 10
	wireCommandListIndexes      wireCommandID = 13
	wireCommandCreateIndex      wireCommandID = 12
	wireCommandOpenCollection   wireCommandID = 15
	wireCommandCloseCollection  wireCommandID = 16
	wireCommandInsertBatch      wireCommandID = 30
	wireCommandReplaceBatch     wireCommandID = 31
	wireCommandDeleteBatch      wireCommandID = 32
	wireCommandFlushAll         wireCommandID = 34
	wireCommandGetMany          wireCommandID = 50
	wireCommandIndexRange       wireCommandID = 52
	wireCommandOpenScan         wireCommandID = 53
	wireCommandCursorNext       wireCommandID = 54
	wireCommandCursorClose      wireCommandID = 55
	wireCommandStats            wireCommandID = 57
)

const (
	wireCommandFlagOmitResultIDs uint64 = 1 << iota
	wireCommandFlagOmitResponseMeta
)

const (
	wireDocumentFormatDefault uint64 = 0
	wireDocumentFormatJSON    uint64 = 1
	wireDocumentFormatBSON    uint64 = 2
)

const (
	wireAckVisible uint64 = 1
	wireAckFlushed uint64 = 2
	wireAckSynced  uint64 = 3
)

type wireErrorCode uint64

const (
	wireErrMalformedFrame wireErrorCode = iota + 1
	wireErrUnsupportedVersion
	wireErrUnsupportedFeature
	wireErrAuthRequired
	wireErrPermissionDenied
	wireErrInvalidCommand
	wireErrCollectionNotFound
	wireErrIndexNotFound
	wireErrDuplicateDocumentID
	wireErrDocumentExists
	wireErrUniqueIndexConflict
	wireErrCatalogVersionMismatch
	wireErrReadOnly
	wireErrTimeout
	wireErrCanceled
	wireErrResourceExhausted
	wireErrInternal
	wireErrDurabilityUnavailable
	wireErrConsistencyUnavailable
	wireErrCursorNotFound
	wireErrCatalogChanged
	wireErrIdempotencyConflict
	wireErrCommitAmbiguous
)

type wireProtocolError struct {
	Code   wireErrorCode
	Reason string
}

func (e *wireProtocolError) Error() string {
	if e == nil {
		return "nativewire: <nil>"
	}
	if e.Reason == "" {
		return fmt.Sprintf("nativewire: error code %d", e.Code)
	}
	return fmt.Sprintf("nativewire: error code %d: %s", e.Code, e.Reason)
}

type wireRemoteError struct {
	Code      wireErrorCode
	Retryable bool
	Message   string
}

func (e *wireRemoteError) Error() string {
	if e == nil {
		return "nativewire: <nil>"
	}
	if e.Message == "" {
		return fmt.Sprintf("nativewire: remote error code %d", e.Code)
	}
	return fmt.Sprintf("nativewire: remote error code %d: %s", e.Code, e.Message)
}

func wireError(code wireErrorCode, format string, args ...interface{}) error {
	return &wireProtocolError{Code: code, Reason: fmt.Sprintf(format, args...)}
}

func isWireRemoteError(err error, code wireErrorCode) bool {
	var remoteErr *wireRemoteError
	return errors.As(err, &remoteErr) && remoteErr.Code == code
}

type wireHeader struct {
	Type      wireFrameType
	Flags     uint32
	StreamID  uint64
	RequestID uint64
	BodyLen   uint64
	Major     uint16
	Minor     uint16
}

type wireSection struct {
	ID    wireSectionID
	Flags uint64
	Bytes []byte
}

type nativeWireClient struct {
	conn                  net.Conn
	nextReq               uint64
	nextKey               uint64
	catalogVersionPlusOne uint64
	mu                    sync.Mutex
	requestMu             sync.Mutex
	requestBody           []byte
	writeBody             []byte
	readBody              []byte
}

func dialNativeWire(ctx context.Context, network, address string, timeout time.Duration) (*nativeWireClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	client := &nativeWireClient{conn: conn}
	if err := client.Hello(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func (c *nativeWireClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *nativeWireClient) Hello(ctx context.Context) error {
	_, _, err := c.roundTrip(ctx, 0, wireFrameHello, nil, wireFrameHelloOK)
	return err
}

func (c *nativeWireClient) Stats(ctx context.Context) (map[string]string, error) {
	sections, err := c.command(ctx, 0, wireCommandStats, 0, nil)
	if err != nil {
		return nil, err
	}
	payload, ok, err := singletonWireSection(sections, wireSectionResponseMeta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, wireError(wireErrMalformedFrame, "stats response missing response_meta")
	}
	return decodeWireStringMap(payload)
}

func (c *nativeWireClient) FlushAll(ctx context.Context, ack uint64) error {
	var sections []wireSection
	if ack != 0 {
		sections = append(sections, wireSection{ID: wireSectionAckPolicy, Bytes: binary.AppendUvarint(nil, ack)})
	}
	_, err := c.command(ctx, 0, wireCommandFlushAll, 0, sections)
	return err
}

func (c *nativeWireClient) CurrentCatalogVersion(ctx context.Context) (uint64, error) {
	stats, err := c.Stats(ctx)
	if err != nil {
		return 0, err
	}
	raw, ok := stats["treedb.native_wire.catalog.version"]
	if !ok {
		return 0, wireError(wireErrInvalidCommand, "catalog version is unavailable")
	}
	version, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, wireError(wireErrMalformedFrame, "invalid catalog version %q", raw)
	}
	atomic.StoreUint64(&c.catalogVersionPlusOne, version+1)
	return version, nil
}

func (c *nativeWireClient) CreateCollection(ctx context.Context, name string, withKeyIndex bool, keyField string, documentFormat uint64) error {
	guard, err := c.replicatedMetadataGuard(ctx, "create_collection")
	if err != nil {
		return err
	}
	sections := append(guard, wireSection{ID: wireSectionCollectionMeta, Bytes: encodeCollectionMeta(name, withKeyIndex, keyField, documentFormat)})
	response, err := c.command(ctx, 0, wireCommandCreateCollection, 0, sections)
	if err != nil {
		c.clearCatalogVersionOnMismatch(err)
		return err
	}
	c.updateCatalogVersionFromResponse(response)
	return nil
}

func (c *nativeWireClient) CreateIndex(ctx context.Context, collection, indexName, fieldName string, unique bool) error {
	guard, err := c.replicatedMetadataGuard(ctx, "create_index")
	if err != nil {
		return err
	}
	sections := append(guard,
		collectionNameRef(collection),
		wireSection{ID: wireSectionIndexDefinition, Bytes: encodeIndexDefinition(indexName, fieldName, unique)},
	)
	response, err := c.command(ctx, 0, wireCommandCreateIndex, 0, sections)
	if err != nil {
		c.clearCatalogVersionOnMismatch(err)
		return err
	}
	c.updateCatalogVersionFromResponse(response)
	return nil
}

func (c *nativeWireClient) ListIndexes(ctx context.Context, collection string) ([]wireIndexDefinition, error) {
	sections, err := c.command(ctx, 0, wireCommandListIndexes, 0, []wireSection{collectionNameRef(collection)})
	if err != nil {
		return nil, err
	}
	raw, ok, err := singletonWireSection(sections, wireSectionIndexDefinition)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, wireError(wireErrMalformedFrame, "list_indexes missing index_definition")
	}
	items, err := decodeWireByteVector(raw)
	if err != nil {
		return nil, err
	}
	indexes := make([]wireIndexDefinition, 0, len(items))
	for _, item := range items {
		def, err := decodeIndexDefinition(item)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, def)
	}
	return indexes, nil
}

func (c *nativeWireClient) OpenCollection(ctx context.Context, name string) (uint64, error) {
	sections, err := c.command(ctx, 0, wireCommandOpenCollection, 0, []wireSection{collectionNameRef(name)})
	if err != nil {
		return 0, err
	}
	raw, ok, err := singletonWireSection(sections, wireSectionCollectionHandle)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, wireError(wireErrMalformedFrame, "open_collection missing collection_handle")
	}
	handle, n, err := readWireUvarint(raw)
	if err != nil {
		return 0, err
	}
	if n != len(raw) {
		return 0, wireError(wireErrMalformedFrame, "collection_handle has trailing bytes")
	}
	if handle == 0 {
		return 0, wireError(wireErrMalformedFrame, "collection_handle cannot be zero")
	}
	return handle, nil
}

func (c *nativeWireClient) CloseCollection(ctx context.Context, handle uint64) error {
	_, err := c.command(ctx, 0, wireCommandCloseCollection, 0, []wireSection{collectionHandleRef(handle)})
	return err
}

func (c *nativeWireClient) GetMany(ctx context.Context, handle uint64, ids [][]byte) ([][]byte, []bool, error) {
	c.requestMu.Lock()
	body, err := appendGetManyBody(c.requestBody[:0], handle, ids)
	if err != nil {
		c.requestMu.Unlock()
		return nil, nil, err
	}
	sections, err := c.roundTripCommandBody(ctx, 0, body)
	c.requestBody = body[:0]
	c.requestMu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	rawDocs, ok, err := singletonWireSection(sections, wireSectionDocuments)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, wireError(wireErrMalformedFrame, "get_many missing documents")
	}
	docs, err := decodeWireByteVector(rawDocs)
	if err != nil {
		return nil, nil, err
	}
	if len(docs) != len(ids) {
		return nil, nil, wireError(wireErrMalformedFrame, "get_many documents length %d does not match ids length %d", len(docs), len(ids))
	}
	rawPresence, ok, err := singletonWireSection(sections, wireSectionPresenceBitmap)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, wireError(wireErrMalformedFrame, "get_many missing presence bitmap")
	}
	present, err := decodePresenceBitmap(rawPresence, len(ids))
	if err != nil {
		return nil, nil, err
	}
	return docs, present, nil
}

func (c *nativeWireClient) InsertBatch(ctx context.Context, handle uint64, ids, docs [][]byte, documentFormat uint64, ack uint64) error {
	return c.mutationWithRetry(ctx, "insert_batch", func(guard []wireSection) ([]wireSection, error) {
		c.requestMu.Lock()
		body, err := appendInsertBatchBody(c.requestBody[:0], handle, ids, docs, documentFormat, ack, wireCommandFlagOmitResultIDs, guard)
		if err != nil {
			c.requestMu.Unlock()
			return nil, err
		}
		sections, err := c.roundTripCommandBody(ctx, 0, body)
		c.requestBody = body[:0]
		c.requestMu.Unlock()
		return sections, err
	})
}

func (c *nativeWireClient) ReplaceBatch(ctx context.Context, handle uint64, ids, docs [][]byte, documentFormat uint64, ack uint64) error {
	return c.mutationWithRetry(ctx, "replace_batch", func(guard []wireSection) ([]wireSection, error) {
		sections := append(guard,
			collectionHandleRef(handle),
			wireSection{ID: wireSectionDocumentFormat, Bytes: binary.AppendUvarint(nil, documentFormat)},
			wireSection{ID: wireSectionDocumentIDs, Bytes: appendWireByteVector(nil, ids...)},
			wireSection{ID: wireSectionDocuments, Bytes: appendWireByteVector(nil, docs...)},
			wireSection{ID: wireSectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)},
		)
		if ack != 0 {
			sections = append(sections, wireSection{ID: wireSectionAckPolicy, Bytes: binary.AppendUvarint(nil, ack)})
		}
		return c.command(ctx, 0, wireCommandReplaceBatch, 0, sections)
	})
}

func (c *nativeWireClient) DeleteBatch(ctx context.Context, handle uint64, ids [][]byte, ack uint64) error {
	return c.mutationWithRetry(ctx, "delete_batch", func(guard []wireSection) ([]wireSection, error) {
		sections := append(guard,
			collectionHandleRef(handle),
			wireSection{ID: wireSectionDocumentIDs, Bytes: appendWireByteVector(nil, ids...)},
		)
		if ack != 0 {
			sections = append(sections, wireSection{ID: wireSectionAckPolicy, Bytes: binary.AppendUvarint(nil, ack)})
		}
		return c.command(ctx, 0, wireCommandDeleteBatch, 0, sections)
	})
}

func (c *nativeWireClient) IndexRange(ctx context.Context, handle uint64, indexName, lower string, limit int) ([][]byte, bool, error) {
	sections := []wireSection{
		collectionHandleRef(handle),
		{ID: wireSectionIndexName, Bytes: encodeIndexName(indexName)},
		{ID: wireSectionIndexLowerBound, Bytes: encodeIndexBoundString(lower, true, false)},
		{ID: wireSectionCursorLimits, Bytes: encodeCursorLimits(limit, 0)},
	}
	response, err := c.command(ctx, 0, wireCommandIndexRange, 0, sections)
	if err != nil {
		return nil, false, err
	}
	return decodeIDsAndTruncated(response)
}

func (c *nativeWireClient) OpenScan(ctx context.Context, handle uint64, limit int) (wireDocumentsResult, error) {
	sections := []wireSection{collectionHandleRef(handle)}
	if limit > 0 {
		sections = append(sections, wireSection{ID: wireSectionCursorLimits, Bytes: encodeCursorLimits(limit, 0)})
	}
	response, err := c.command(ctx, 0, wireCommandOpenScan, 0, sections)
	if err != nil {
		return wireDocumentsResult{}, err
	}
	return decodeDocumentsResult(response)
}

func (c *nativeWireClient) CursorNext(ctx context.Context, cursorID uint64, limit int) (wireDocumentsResult, error) {
	sections := []wireSection{
		{ID: wireSectionCursorRef, Bytes: binary.AppendUvarint(nil, cursorID)},
		{ID: wireSectionCursorLimits, Bytes: encodeCursorLimits(limit, 0)},
	}
	response, err := c.command(ctx, cursorID, wireCommandCursorNext, 0, sections)
	if err != nil {
		return wireDocumentsResult{}, err
	}
	return decodeDocumentsResult(response)
}

func (c *nativeWireClient) CursorClose(ctx context.Context, cursorID uint64) error {
	if cursorID == 0 {
		return nil
	}
	_, err := c.command(ctx, cursorID, wireCommandCursorClose, 0, []wireSection{{ID: wireSectionCursorRef, Bytes: binary.AppendUvarint(nil, cursorID)}})
	return err
}

func (c *nativeWireClient) mutationWithRetry(ctx context.Context, commandName string, fn func(guard []wireSection) ([]wireSection, error)) error {
	for attempt := 0; ; attempt++ {
		guard, err := c.replicatedMutationGuard(ctx, commandName)
		if err != nil {
			return err
		}
		sections, err := fn(guard)
		if err == nil {
			c.updateCatalogVersionFromResponse(sections)
			return nil
		}
		if !isWireRemoteError(err, wireErrCatalogVersionMismatch) || attempt >= 128 {
			c.clearCatalogVersionOnMismatch(err)
			return err
		}
		c.clearCatalogVersionOnMismatch(err)
		if delay := time.Duration(attempt+1) * time.Millisecond; delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (c *nativeWireClient) replicatedMetadataGuard(ctx context.Context, command string) ([]wireSection, error) {
	version, err := c.CurrentCatalogVersion(ctx)
	if err != nil {
		return nil, err
	}
	return c.replicatedGuardForVersion(command, version)
}

func (c *nativeWireClient) replicatedMutationGuard(ctx context.Context, command string) ([]wireSection, error) {
	versionPlusOne := atomic.LoadUint64(&c.catalogVersionPlusOne)
	var version uint64
	if versionPlusOne != 0 {
		version = versionPlusOne - 1
	} else {
		var err error
		version, err = c.CurrentCatalogVersion(ctx)
		if err != nil {
			return nil, err
		}
	}
	return c.replicatedGuardForVersion(command, version)
}

func (c *nativeWireClient) replicatedGuardForVersion(command string, version uint64) ([]wireSection, error) {
	key := []byte("go-ycsb/" + command + "/" + strconv.FormatUint(atomic.AddUint64(&c.nextKey, 1), 10))
	return []wireSection{
		{ID: wireSectionIdempotencyKey, Bytes: key},
		{ID: wireSectionExpectedCatalog, Bytes: binary.AppendUvarint(nil, version)},
	}, nil
}

func (c *nativeWireClient) clearCatalogVersionOnMismatch(err error) {
	if isWireRemoteError(err, wireErrCatalogVersionMismatch) {
		atomic.StoreUint64(&c.catalogVersionPlusOne, 0)
	}
}

func (c *nativeWireClient) updateCatalogVersionFromResponse(sections []wireSection) {
	version, ok, err := catalogVersionFromResponseMeta(sections)
	if err != nil || !ok || version == ^uint64(0) {
		atomic.StoreUint64(&c.catalogVersionPlusOne, 0)
		return
	}
	atomic.StoreUint64(&c.catalogVersionPlusOne, version+1)
}

func catalogVersionFromResponseMeta(sections []wireSection) (uint64, bool, error) {
	raw, ok, err := singletonWireSection(sections, wireSectionResponseMeta)
	if err != nil || !ok {
		return 0, ok, err
	}
	values, err := decodeWireStringMap(raw)
	if err != nil {
		return 0, false, err
	}
	rawVersion, ok := values["catalog_version"]
	if !ok {
		return 0, false, nil
	}
	version, err := strconv.ParseUint(rawVersion, 10, 64)
	if err != nil {
		return 0, true, wireError(wireErrMalformedFrame, "invalid catalog_version %q", rawVersion)
	}
	return version, true, nil
}

func (c *nativeWireClient) command(ctx context.Context, streamID uint64, commandID wireCommandID, commandFlags uint64, sections []wireSection) ([]wireSection, error) {
	c.requestMu.Lock()
	body, err := appendCommandRequestBody(c.requestBody[:0], commandID, commandFlags, sections)
	if err != nil {
		c.requestMu.Unlock()
		return nil, err
	}
	response, err := c.roundTripCommandBody(ctx, streamID, body)
	c.requestBody = body[:0]
	c.requestMu.Unlock()
	return response, err
}

func (c *nativeWireClient) roundTripCommandBody(ctx context.Context, streamID uint64, body []byte) ([]wireSection, error) {
	_, response, err := c.roundTrip(ctx, streamID, wireFrameRequest, body, wireFrameResponse)
	if err != nil {
		return nil, err
	}
	return decodeWireSections(response)
}

func (c *nativeWireClient) roundTrip(ctx context.Context, streamID uint64, typ wireFrameType, body []byte, want wireFrameType) (wireHeader, []byte, error) {
	if c == nil || c.conn == nil {
		return wireHeader{}, nil, io.ErrClosedPipe
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return wireHeader{}, nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.roundTripLocked(ctx, streamID, typ, body, want)
}

func (c *nativeWireClient) roundTripLocked(ctx context.Context, streamID uint64, typ wireFrameType, body []byte, want wireFrameType) (wireHeader, []byte, error) {
	requestID := atomic.AddUint64(&c.nextReq, 1)
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
	}
	stopCancel := c.interruptDeadlineOnContextCancel(ctx)
	defer func() {
		stopCancel()
		_ = c.conn.SetDeadline(time.Time{})
	}()

	var err error
	c.writeBody, err = writeWireFrameBuffered(c.conn, wireHeader{Type: typ, StreamID: streamID, RequestID: requestID}, body, c.writeBody)
	if err != nil {
		return wireHeader{}, nil, c.errorOrCanceled(ctx, err)
	}
	header, response, err := readWireFrameInto(c.conn, c.readBody)
	if err != nil {
		return wireHeader{}, nil, c.closeOnProtocolError(c.errorOrCanceled(ctx, err))
	}
	c.readBody = response[:0]
	if err := validateWireHeaderVersion(header); err != nil {
		return header, response, c.closeOnProtocolError(err)
	}
	if header.RequestID != requestID {
		return header, response, c.closeOnProtocolError(wireError(wireErrMalformedFrame, "response request_id %d want %d", header.RequestID, requestID))
	}
	if header.Type == wireFrameError {
		return header, response, c.closeOnProtocolError(decodeWireError(response))
	}
	if header.Type != want {
		return header, response, c.closeOnProtocolError(wireError(wireErrMalformedFrame, "response frame type %d want %d", header.Type, want))
	}
	return header, response, nil
}

func (c *nativeWireClient) interruptDeadlineOnContextCancel(ctx context.Context) func() {
	if c == nil || c.conn == nil || ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			_ = c.conn.SetDeadline(time.Now())
		case <-done:
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
			<-stopped
		})
	}
}

func (c *nativeWireClient) errorOrCanceled(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		_ = c.conn.Close()
		return ctx.Err()
	}
	return err
}

func (c *nativeWireClient) closeOnProtocolError(err error) error {
	if err == nil {
		return nil
	}
	var remoteErr *wireRemoteError
	if errors.As(err, &remoteErr) {
		return err
	}
	if c != nil && c.conn != nil {
		_ = c.conn.Close()
	}
	return err
}

func appendCommandRequestBody(dst []byte, commandID wireCommandID, commandFlags uint64, sections []wireSection) ([]byte, error) {
	commandPayload := appendWireCommandHeader(nil, commandID, 1, commandFlags)
	all := make([]wireSection, 0, len(sections)+1)
	all = append(all, wireSection{ID: wireSectionCommandHeader, Bytes: commandPayload})
	all = append(all, sections...)
	total := 0
	for _, section := range all {
		sectionLen := wireSectionEncodedLen(section)
		if sectionLen < 0 || total > maxInt-sectionLen {
			return nil, wireError(wireErrResourceExhausted, "request body length exceeds int capacity")
		}
		total += sectionLen
	}
	if cap(dst) < total {
		dst = make([]byte, 0, total)
	} else {
		dst = dst[:0]
	}
	for _, section := range all {
		var err error
		dst, err = appendWireSection(dst, section)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func appendGetManyBody(dst []byte, handle uint64, ids [][]byte) ([]byte, error) {
	sections := []wireSection{
		{ID: wireSectionCommandHeader, Bytes: appendWireCommandHeader(nil, wireCommandGetMany, 1, 0)},
		collectionHandleRef(handle),
		{ID: wireSectionDocumentIDs, Bytes: appendWireByteVector(nil, ids...)},
	}
	return appendWireSections(dst, sections)
}

func appendInsertBatchBody(dst []byte, handle uint64, ids, docs [][]byte, documentFormat uint64, ack uint64, commandFlags uint64, guard []wireSection) ([]byte, error) {
	sections := make([]wireSection, 0, len(guard)+6)
	sections = append(sections, wireSection{ID: wireSectionCommandHeader, Bytes: appendWireCommandHeader(nil, wireCommandInsertBatch, 1, commandFlags)})
	sections = append(sections, guard...)
	sections = append(sections,
		collectionHandleRef(handle),
		wireSection{ID: wireSectionDocumentFormat, Bytes: binary.AppendUvarint(nil, documentFormat)},
		wireSection{ID: wireSectionDocumentIDs, Bytes: appendWireByteVector(nil, ids...)},
		wireSection{ID: wireSectionDocuments, Bytes: appendWireByteVector(nil, docs...)},
	)
	if ack != 0 {
		sections = append(sections, wireSection{ID: wireSectionAckPolicy, Bytes: binary.AppendUvarint(nil, ack)})
	}
	return appendWireSections(dst, sections)
}

func appendWireSections(dst []byte, sections []wireSection) ([]byte, error) {
	total := 0
	for _, section := range sections {
		sectionLen := wireSectionEncodedLen(section)
		if sectionLen < 0 || total > maxInt-sectionLen {
			return nil, wireError(wireErrResourceExhausted, "request body length exceeds int capacity")
		}
		total += sectionLen
	}
	if cap(dst) < total {
		dst = make([]byte, 0, total)
	} else {
		dst = dst[:0]
	}
	for _, section := range sections {
		var err error
		dst, err = appendWireSection(dst, section)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func appendWireCommandHeader(dst []byte, id wireCommandID, version, flags uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(id))
	dst = binary.AppendUvarint(dst, version)
	return binary.AppendUvarint(dst, flags)
}

func collectionNameRef(name string) wireSection {
	payload := make([]byte, 0, len(name)+1)
	payload = append(payload, 1)
	payload = append(payload, name...)
	return wireSection{ID: wireSectionCollectionRef, Bytes: payload}
}

func collectionHandleRef(handle uint64) wireSection {
	payload := []byte{2}
	payload = binary.AppendUvarint(payload, handle)
	return wireSection{ID: wireSectionCollectionRef, Bytes: payload}
}

func encodeCollectionMeta(name string, withKeyIndex bool, keyField string, documentFormat uint64) []byte {
	dst := binary.AppendUvarint(nil, 2)             // collection_meta version
	dst = appendWireString(dst, name)               // name
	dst = binary.AppendUvarint(dst, documentFormat) // document_format
	dst = binary.AppendUvarint(dst, 0)              // default data root storage
	dst = binary.AppendUvarint(dst, 0)              // default index root storage
	dst = appendWireBool(dst, false)                // allow array values in index
	dst = appendWireBool(dst, false)                // disable indexed write memtables
	dst = appendWireBool(dst, false)                // buffered indexed writes
	dst = binary.AppendVarint(dst, 0)               // buffered indexed max docs
	dst = binary.AppendVarint(dst, 0)               // buffered indexed max bytes
	dst = binary.AppendVarint(dst, 0)               // buffered indexed max root runs
	dst = appendWireBool(dst, false)                // async flush
	dst = appendWireBool(dst, false)                // overlay roots
	dst = binary.AppendVarint(dst, 0)               // async flush max queued units
	if withKeyIndex && keyField != "" {
		dst = binary.AppendUvarint(dst, 1) // index count
		dst = appendIndexDefinitionNoVersion(dst, keyIndexName, keyField, true)
	} else {
		dst = binary.AppendUvarint(dst, 0) // index count
	}
	dst = binary.AppendUvarint(dst, 0) // vector index count
	return dst
}

func encodeIndexDefinition(name, field string, unique bool) []byte {
	dst := binary.AppendUvarint(nil, 1)
	return appendIndexDefinitionNoVersion(dst, name, field, unique)
}

func appendIndexDefinitionNoVersion(dst []byte, name, field string, unique bool) []byte {
	dst = appendWireString(dst, name)
	dst = appendWireString(dst, field)
	dst = binary.AppendUvarint(dst, 1) // string index value type
	dst = appendWireBool(dst, unique)
	dst = appendWireBool(dst, false)   // multikey
	dst = binary.AppendUvarint(dst, 0) // default root storage
	return dst
}

func encodeIndexName(name string) []byte {
	return appendWireString(nil, name)
}

func encodeScalarString(value string) []byte {
	dst := binary.AppendUvarint(nil, 1)
	return appendWireString(dst, value)
}

func encodeIndexBoundString(value string, inclusive, unbounded bool) []byte {
	dst := appendWireBool(nil, unbounded)
	dst = appendWireBool(dst, inclusive)
	if unbounded {
		return dst
	}
	return append(dst, encodeScalarString(value)...)
}

func encodeCursorLimits(maxItems, maxBytes int) []byte {
	if maxItems < 0 {
		maxItems = 0
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	dst := binary.AppendUvarint(nil, uint64(maxItems))
	return binary.AppendUvarint(dst, uint64(maxBytes))
}

func writeWireFrameBuffered(w io.Writer, header wireHeader, body []byte, dst []byte) ([]byte, error) {
	if len(body) > wireMaxBufferedBody {
		return dst[:0], writeWireFrame(w, header, body)
	}
	frameHeader, err := appendWireHeader(nil, header, uint64(len(body)))
	if err != nil {
		return dst[:0], err
	}
	if len(body) > maxInt-len(frameHeader) {
		return dst[:0], wireError(wireErrResourceExhausted, "frame length exceeds int capacity")
	}
	total := len(frameHeader) + len(body)
	if cap(dst) < total {
		dst = make([]byte, 0, total)
	} else {
		dst = dst[:0]
	}
	dst = append(dst, frameHeader...)
	dst = append(dst, body...)
	return dst[:0], writeAll(w, dst)
}

func writeWireFrame(w io.Writer, header wireHeader, body []byte) error {
	frameHeader, err := appendWireHeader(nil, header, uint64(len(body)))
	if err != nil {
		return err
	}
	if err := writeAll(w, frameHeader); err != nil {
		return err
	}
	return writeAll(w, body)
}

func appendWireHeader(dst []byte, header wireHeader, bodyLen uint64) ([]byte, error) {
	if !validWireFrameType(header.Type) {
		return dst, wireError(wireErrInvalidCommand, "unknown frame type %d", header.Type)
	}
	if header.Flags&0x0000ffff != 0 {
		return dst, wireError(wireErrUnsupportedFeature, "unknown required frame flags 0x%08x", header.Flags&0x0000ffff)
	}
	var buf [wireFrameHeaderLenV1]byte
	copy(buf[0:4], wireMagic)
	binary.LittleEndian.PutUint16(buf[4:6], wireFrameHeaderLenV1)
	binary.LittleEndian.PutUint16(buf[6:8], wireProtocolMajorV1)
	binary.LittleEndian.PutUint16(buf[8:10], wireProtocolMinorV0)
	binary.LittleEndian.PutUint16(buf[10:12], uint16(header.Type))
	binary.LittleEndian.PutUint32(buf[12:16], header.Flags)
	binary.LittleEndian.PutUint64(buf[16:24], header.StreamID)
	binary.LittleEndian.PutUint64(buf[24:32], header.RequestID)
	binary.LittleEndian.PutUint64(buf[32:40], bodyLen)
	return append(dst, buf[:]...), nil
}

func readWireFrameInto(r io.Reader, dst []byte) (wireHeader, []byte, error) {
	var headerBuf [wireFrameHeaderLenV1]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return wireHeader{}, nil, err
	}
	header, err := decodeWireHeader(headerBuf[:])
	if err != nil {
		return wireHeader{}, nil, err
	}
	if header.BodyLen == 0 {
		return header, nil, nil
	}
	if header.BodyLen > uint64(maxInt) {
		return wireHeader{}, nil, wireError(wireErrResourceExhausted, "frame body exceeds int capacity")
	}
	bodyLen := int(header.BodyLen)
	var body []byte
	if bodyLen <= cap(dst) {
		body = dst[:bodyLen]
	} else {
		body = make([]byte, bodyLen)
	}
	if _, err := io.ReadFull(r, body); err != nil {
		return wireHeader{}, nil, err
	}
	return header, body, nil
}

func decodeWireHeader(src []byte) (wireHeader, error) {
	if len(src) < int(wireFrameHeaderLenV1) {
		return wireHeader{}, wireError(wireErrMalformedFrame, "short frame header: %d", len(src))
	}
	if string(src[0:4]) != wireMagic {
		return wireHeader{}, wireError(wireErrMalformedFrame, "bad frame magic")
	}
	headerLen := binary.LittleEndian.Uint16(src[4:6])
	if headerLen < wireFrameHeaderLenV1 {
		return wireHeader{}, wireError(wireErrMalformedFrame, "invalid header length %d", headerLen)
	}
	if headerLen > wireMaxHeaderLen {
		return wireHeader{}, wireError(wireErrResourceExhausted, "header length %d exceeds limit %d", headerLen, wireMaxHeaderLen)
	}
	if headerLen > wireFrameHeaderLenV1 {
		return wireHeader{}, wireError(wireErrUnsupportedFeature, "unnegotiated fixed-header extension")
	}
	header := wireHeader{
		Major:     binary.LittleEndian.Uint16(src[6:8]),
		Minor:     binary.LittleEndian.Uint16(src[8:10]),
		Type:      wireFrameType(binary.LittleEndian.Uint16(src[10:12])),
		Flags:     binary.LittleEndian.Uint32(src[12:16]),
		StreamID:  binary.LittleEndian.Uint64(src[16:24]),
		RequestID: binary.LittleEndian.Uint64(src[24:32]),
		BodyLen:   binary.LittleEndian.Uint64(src[32:40]),
	}
	if !validWireFrameType(header.Type) {
		return wireHeader{}, wireError(wireErrInvalidCommand, "unknown frame type %d", header.Type)
	}
	if header.Flags&0x0000ffff != 0 {
		return wireHeader{}, wireError(wireErrUnsupportedFeature, "unknown required frame flags 0x%08x", header.Flags&0x0000ffff)
	}
	frameLen := uint64(headerLen) + header.BodyLen
	if frameLen < header.BodyLen {
		return wireHeader{}, wireError(wireErrMalformedFrame, "frame length overflow")
	}
	if frameLen > wireMaxFrameSize {
		return wireHeader{}, wireError(wireErrResourceExhausted, "frame length %d exceeds limit %d", frameLen, wireMaxFrameSize)
	}
	return header, nil
}

func validateWireHeaderVersion(header wireHeader) error {
	if header.Major != wireProtocolMajorV1 {
		return wireError(wireErrUnsupportedVersion, "major version %d is not selected major %d", header.Major, wireProtocolMajorV1)
	}
	if header.Minor != wireProtocolMinorV0 {
		return wireError(wireErrUnsupportedVersion, "minor version %d is not selected minor %d", header.Minor, wireProtocolMinorV0)
	}
	return nil
}

func validWireFrameType(typ wireFrameType) bool {
	return typ >= wireFrameHello && typ <= wireFrameGoaway
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func appendWireSection(dst []byte, section wireSection) ([]byte, error) {
	if section.Flags&^uint64(1) != 0 {
		return dst, wireError(wireErrUnsupportedFeature, "unknown section flags 0x%x", section.Flags&^uint64(1))
	}
	dst = binary.AppendUvarint(dst, uint64(section.ID))
	dst = binary.AppendUvarint(dst, section.Flags)
	dst = binary.AppendUvarint(dst, uint64(len(section.Bytes)))
	return append(dst, section.Bytes...), nil
}

func wireSectionEncodedLen(section wireSection) int {
	n := uvarintLen(uint64(section.ID)) + uvarintLen(section.Flags) + uvarintLen(uint64(len(section.Bytes)))
	if len(section.Bytes) > maxInt-n {
		return -1
	}
	return n + len(section.Bytes)
}

func decodeWireSections(src []byte) ([]wireSection, error) {
	sections := make([]wireSection, 0, 8)
	for off := 0; off < len(src); {
		if len(sections) >= wireMaxSections {
			return nil, wireError(wireErrResourceExhausted, "section count exceeds limit %d", wireMaxSections)
		}
		id, n, err := readWireUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		flags, n, err := readWireUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		if flags&^uint64(1) != 0 {
			return nil, wireError(wireErrUnsupportedFeature, "unknown section flags 0x%x", flags&^uint64(1))
		}
		sectionLen, n, err := readWireUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		if sectionLen > wireMaxSectionLen {
			return nil, wireError(wireErrResourceExhausted, "section %d length %d exceeds limit %d", id, sectionLen, wireMaxSectionLen)
		}
		if sectionLen > uint64(maxInt) || sectionLen > uint64(len(src)-off) {
			return nil, wireError(wireErrMalformedFrame, "section %d length %d exceeds remaining body %d", id, sectionLen, len(src)-off)
		}
		next := off + int(sectionLen)
		sections = append(sections, wireSection{ID: wireSectionID(id), Flags: flags, Bytes: src[off:next]})
		off = next
	}
	return sections, nil
}

func singletonWireSection(sections []wireSection, id wireSectionID) ([]byte, bool, error) {
	var out []byte
	found := false
	for _, section := range sections {
		if section.ID != id {
			continue
		}
		if found {
			return nil, false, wireError(wireErrInvalidCommand, "duplicate section %d", id)
		}
		out = section.Bytes
		found = true
	}
	return out, found, nil
}

func decodeWireError(body []byte) error {
	sections, err := decodeWireSections(body)
	if err != nil {
		return err
	}
	payload, ok, err := singletonWireSection(sections, wireSectionError)
	if err != nil {
		return err
	}
	if !ok {
		return wireError(wireErrMalformedFrame, "error frame missing error section")
	}
	code, off, err := readWireUvarint(payload)
	if err != nil {
		return err
	}
	if off >= len(payload) {
		return wireError(wireErrMalformedFrame, "missing error retryable flag")
	}
	var retryable bool
	switch payload[off] {
	case 0:
		retryable = false
	case 1:
		retryable = true
	default:
		return wireError(wireErrMalformedFrame, "invalid error retryable flag %d", payload[off])
	}
	off++
	message, err := readWireString(payload, &off)
	if err != nil {
		return err
	}
	if off != len(payload) {
		return wireError(wireErrMalformedFrame, "error payload has %d trailing bytes", len(payload)-off)
	}
	return &wireRemoteError{Code: wireErrorCode(code), Retryable: retryable, Message: message}
}

func appendWireByteVector(dst []byte, items ...[]byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(items)))
	for _, item := range items {
		dst = binary.AppendUvarint(dst, uint64(len(item)))
	}
	for _, item := range items {
		dst = append(dst, item...)
	}
	return dst
}

func decodeWireByteVector(src []byte) ([][]byte, error) {
	count64, lengthsOff, err := readWireUvarint(src)
	if err != nil {
		return nil, err
	}
	if count64 > wireMaxVectorItems {
		return nil, wireError(wireErrResourceExhausted, "byte-vector count %d exceeds limit %d", count64, wireMaxVectorItems)
	}
	if count64 > uint64(maxInt) {
		return nil, wireError(wireErrResourceExhausted, "byte-vector count exceeds int capacity")
	}
	count := int(count64)
	off := lengthsOff
	total := uint64(0)
	lengths := make([]int, count)
	for i := 0; i < count; i++ {
		length, n, err := readWireUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		if length > wireMaxVectorBytes || total+length < total || total+length > wireMaxVectorBytes {
			return nil, wireError(wireErrResourceExhausted, "byte-vector payload exceeds limit")
		}
		if length > uint64(maxInt) {
			return nil, wireError(wireErrResourceExhausted, "byte-vector item length exceeds int capacity")
		}
		total += length
		lengths[i] = int(length)
	}
	if total != uint64(len(src)-off) {
		return nil, wireError(wireErrMalformedFrame, "byte-vector declared payload %d does not match remaining %d", total, len(src)-off)
	}
	out := make([][]byte, count)
	payloadOff := off
	next := 0
	for i, length := range lengths {
		start := payloadOff + next
		next += length
		out[i] = src[start : payloadOff+next]
	}
	return out, nil
}

func decodePresenceBitmap(src []byte, count int) ([]bool, error) {
	want := (count + 7) / 8
	if len(src) != want {
		return nil, wireError(wireErrMalformedFrame, "presence bitmap length %d want %d", len(src), want)
	}
	out := make([]bool, count)
	for i := range out {
		out[i] = src[i/8]&(1<<uint(i%8)) != 0
	}
	return out, nil
}

func decodeIDsAndTruncated(sections []wireSection) ([][]byte, bool, error) {
	rawIDs, ok, err := singletonWireSection(sections, wireSectionDocumentIDs)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, wireError(wireErrMalformedFrame, "missing document_ids")
	}
	ids, err := decodeWireByteVector(rawIDs)
	if err != nil {
		return nil, false, err
	}
	truncated := false
	if raw, ok, err := singletonWireSection(sections, wireSectionTruncated); err != nil {
		return nil, false, err
	} else if ok {
		truncated, err = decodeWireBoolPayload(raw, "truncated")
		if err != nil {
			return nil, false, err
		}
	}
	return ids, truncated, nil
}

type wireCursorMeta struct {
	CursorID uint64
	Items    int
	Bytes    int
	HasMore  bool
}

type wireDocumentsResult struct {
	IDs       [][]byte
	Docs      [][]byte
	Cursor    wireCursorMeta
	Truncated bool
}

func decodeDocumentsResult(sections []wireSection) (wireDocumentsResult, error) {
	var out wireDocumentsResult
	if raw, ok, err := singletonWireSection(sections, wireSectionDocumentIDs); err != nil {
		return out, err
	} else if ok {
		out.IDs, err = decodeWireByteVector(raw)
		if err != nil {
			return out, err
		}
	}
	if raw, ok, err := singletonWireSection(sections, wireSectionDocuments); err != nil {
		return out, err
	} else if ok {
		out.Docs, err = decodeWireByteVector(raw)
		if err != nil {
			return out, err
		}
	}
	if raw, ok, err := singletonWireSection(sections, wireSectionCursorMeta); err != nil {
		return out, err
	} else if ok {
		out.Cursor, err = decodeCursorMeta(raw)
		if err != nil {
			return out, err
		}
	}
	if raw, ok, err := singletonWireSection(sections, wireSectionTruncated); err != nil {
		return out, err
	} else if ok {
		out.Truncated, err = decodeWireBoolPayload(raw, "truncated")
		if err != nil {
			return out, err
		}
	}
	if out.IDs != nil && out.Docs != nil && len(out.IDs) != len(out.Docs) {
		return out, wireError(wireErrMalformedFrame, "document_ids length %d does not match documents length %d", len(out.IDs), len(out.Docs))
	}
	return out, nil
}

func decodeCursorMeta(src []byte) (wireCursorMeta, error) {
	cursorID, off, err := readWireUvarint(src)
	if err != nil {
		return wireCursorMeta{}, err
	}
	items, n, err := readWireUvarint(src[off:])
	if err != nil {
		return wireCursorMeta{}, err
	}
	off += n
	bytesValue, n, err := readWireUvarint(src[off:])
	if err != nil {
		return wireCursorMeta{}, err
	}
	off += n
	hasMore, err := readWireBool(src, &off)
	if err != nil {
		return wireCursorMeta{}, err
	}
	if off != len(src) {
		return wireCursorMeta{}, wireError(wireErrMalformedFrame, "cursor_meta has trailing bytes")
	}
	if items > uint64(maxInt) || bytesValue > uint64(maxInt) {
		return wireCursorMeta{}, wireError(wireErrResourceExhausted, "cursor_meta exceeds int capacity")
	}
	return wireCursorMeta{CursorID: cursorID, Items: int(items), Bytes: int(bytesValue), HasMore: hasMore}, nil
}

func decodeWireBoolPayload(raw []byte, name string) (bool, error) {
	off := 0
	value, err := readWireBool(raw, &off)
	if err != nil {
		return false, err
	}
	if off != len(raw) {
		return false, wireError(wireErrMalformedFrame, "%s bool has %d trailing bytes", name, len(raw)-off)
	}
	return value, nil
}

func appendWireString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readWireString(src []byte, off *int) (string, error) {
	if off == nil || *off > len(src) {
		return "", wireError(wireErrMalformedFrame, "invalid string offset")
	}
	n, read, err := readWireUvarint(src[*off:])
	if err != nil {
		return "", err
	}
	*off += read
	if n > uint64(len(src)-*off) {
		return "", wireError(wireErrMalformedFrame, "string length exceeds remaining payload")
	}
	out := string(src[*off : *off+int(n)])
	*off += int(n)
	return out, nil
}

func appendWireBool(dst []byte, value bool) []byte {
	if value {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func readWireBool(src []byte, off *int) (bool, error) {
	if off == nil || *off < 0 || *off >= len(src) {
		return false, wireError(wireErrMalformedFrame, "missing bool")
	}
	value := src[*off]
	*off = *off + 1
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, wireError(wireErrMalformedFrame, "invalid bool %d", value)
	}
}

func decodeWireStringMap(src []byte) (map[string]string, error) {
	count, off, err := readWireUvarint(src)
	if err != nil {
		return nil, err
	}
	if count > uint64(maxInt) || count > wireMaxStringMap {
		return nil, wireError(wireErrResourceExhausted, "string map count %d exceeds limit", count)
	}
	out := make(map[string]string, int(count))
	for i := uint64(0); i < count; i++ {
		key, err := readWireString(src, &off)
		if err != nil {
			return nil, err
		}
		value, err := readWireString(src, &off)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	if off != len(src) {
		return nil, wireError(wireErrMalformedFrame, "string map has %d trailing bytes", len(src)-off)
	}
	return out, nil
}

func readWireUvarint(src []byte) (uint64, int, error) {
	value, n := binary.Uvarint(src)
	switch {
	case n > 0:
		if n != uvarintLen(value) {
			return 0, 0, wireError(wireErrMalformedFrame, "non-minimal uvarint")
		}
		return value, n, nil
	case n == 0:
		return 0, 0, wireError(wireErrMalformedFrame, "invalid uvarint")
	default:
		return 0, 0, wireError(wireErrMalformedFrame, "uvarint overflow")
	}
}

func uvarintLen(value uint64) int {
	if value == 0 {
		return 1
	}
	return (bits.Len64(value) + 6) / 7
}

type wireIndexDefinition struct {
	Name   string
	Field  string
	Unique bool
}

func decodeIndexDefinition(src []byte) (wireIndexDefinition, error) {
	off := 0
	version, n, err := readWireUvarint(src)
	if err != nil {
		return wireIndexDefinition{}, err
	}
	if version != 1 {
		return wireIndexDefinition{}, wireError(wireErrUnsupportedVersion, "index_definition version %d", version)
	}
	off += n
	name, err := readWireString(src, &off)
	if err != nil {
		return wireIndexDefinition{}, err
	}
	field, err := readWireString(src, &off)
	if err != nil {
		return wireIndexDefinition{}, err
	}
	_, n, err = readWireUvarint(src[off:])
	if err != nil {
		return wireIndexDefinition{}, err
	}
	off += n
	unique, err := readWireBool(src, &off)
	if err != nil {
		return wireIndexDefinition{}, err
	}
	_, err = readWireBool(src, &off)
	if err != nil {
		return wireIndexDefinition{}, err
	}
	_, n, err = readWireUvarint(src[off:])
	if err != nil {
		return wireIndexDefinition{}, err
	}
	off += n
	if off != len(src) {
		return wireIndexDefinition{}, wireError(wireErrMalformedFrame, "index_definition has %d trailing bytes", len(src)-off)
	}
	return wireIndexDefinition{Name: name, Field: field, Unique: unique}, nil
}
