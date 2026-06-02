package treedbnative

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

func TestParseDocumentFormatDefaultsToBSON(t *testing.T) {
	for _, raw := range []string{"", "bson", "BSON", "binary"} {
		got, err := parseDocumentFormat(raw)
		if err != nil {
			t.Fatalf("parseDocumentFormat(%q): %v", raw, err)
		}
		if got != wireDocumentFormatBSON {
			t.Fatalf("parseDocumentFormat(%q)=%d want BSON", raw, got)
		}
	}

	got, err := parseDocumentFormat("json")
	if err != nil {
		t.Fatalf("parseDocumentFormat(json): %v", err)
	}
	if got != wireDocumentFormatJSON {
		t.Fatalf("parseDocumentFormat(json)=%d want JSON", got)
	}

	if _, err := parseDocumentFormat("template-v1"); err == nil {
		t.Fatal("parseDocumentFormat(template-v1) succeeded, want error")
	}
}

func TestTreeDBRowEncodingBSONRoundTrip(t *testing.T) {
	db := &treedbDB{keyField: treedbDefaultKeyField, documentFormat: wireDocumentFormatBSON}
	values := map[string][]byte{
		"field0":              []byte("alpha"),
		"field1":              []byte{0, 1, 2, 3},
		treedbDefaultKeyField: []byte("ignored"),
	}

	doc, err := db.encodeRow("user123", values)
	if err != nil {
		t.Fatalf("encodeRow: %v", err)
	}
	raw := bson.Raw(doc)
	if err := raw.Validate(); err != nil {
		t.Fatalf("encoded BSON did not validate: %v", err)
	}
	if got := raw.Lookup(treedbDefaultKeyField).StringValue(); got != "user123" {
		t.Fatalf("encoded key=%q want user123", got)
	}

	got, err := db.decodeRow(doc, nil)
	if err != nil {
		t.Fatalf("decodeRow: %v", err)
	}
	want := map[string][]byte{
		"field0": []byte("alpha"),
		"field1": []byte{0, 1, 2, 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decodeRow=%v want %v", got, want)
	}

	projected, err := db.decodeRow(doc, []string{"field1"})
	if err != nil {
		t.Fatalf("decodeRow projected: %v", err)
	}
	wantProjected := map[string][]byte{"field1": []byte{0, 1, 2, 3}}
	if !reflect.DeepEqual(projected, wantProjected) {
		t.Fatalf("decodeRow projected=%v want %v", projected, wantProjected)
	}
}

func TestTreeDBBSONSetFieldsForUpdate(t *testing.T) {
	db := &treedbDB{keyField: treedbDefaultKeyField, documentFormat: wireDocumentFormatBSON}
	fields, ok, err := db.bsonSetFieldsForUpdate(map[string][]byte{
		"field0":              []byte("alpha"),
		treedbDefaultKeyField: []byte("ignored"),
	})
	if err != nil {
		t.Fatalf("bsonSetFieldsForUpdate: %v", err)
	}
	if !ok {
		t.Fatal("bsonSetFieldsForUpdate ok=false, want true")
	}
	if len(fields) != 1 || fields[0].Key != "field0" {
		t.Fatalf("fields=%+v want one field0", fields)
	}
	raw := fields[0].RawValue
	if len(raw) != 1+4+1+len("alpha") {
		t.Fatalf("raw len=%d want %d", len(raw), 1+4+1+len("alpha"))
	}
	if raw[0] != byte(bsontype.Binary) {
		t.Fatalf("raw type=%d want BSON binary", raw[0])
	}
	if got := binary.LittleEndian.Uint32(raw[1:5]); got != uint32(len("alpha")) {
		t.Fatalf("binary len=%d want %d", got, len("alpha"))
	}
	if raw[5] != 0 || string(raw[6:]) != "alpha" {
		t.Fatalf("binary subtype/value=%d/%q want 0/alpha", raw[5], raw[6:])
	}
}

func TestTreeDBBSONSetFieldsForUpdateFallbacks(t *testing.T) {
	jsonDB := &treedbDB{keyField: treedbDefaultKeyField, documentFormat: wireDocumentFormatJSON}
	if _, ok, err := jsonDB.bsonSetFieldsForUpdate(map[string][]byte{"field0": []byte("alpha")}); err != nil || ok {
		t.Fatalf("JSON bsonSetFieldsForUpdate ok=%v err=%v want fallback", ok, err)
	}

	db := &treedbDB{keyField: treedbDefaultKeyField, documentFormat: wireDocumentFormatBSON}
	for _, values := range []map[string][]byte{
		{treedbDefaultKeyField: []byte("ignored")},
		{"": []byte("alpha")},
		{"_id": []byte("alpha")},
		{"$set": []byte("alpha")},
		{"a.b": []byte("alpha")},
		{"a\x00b": []byte("alpha")},
	} {
		if _, ok, err := db.bsonSetFieldsForUpdate(values); err != nil || ok {
			t.Fatalf("bsonSetFieldsForUpdate(%v) ok=%v err=%v want fallback", values, ok, err)
		}
	}
}

func TestAppendUpdateBSONSetBody(t *testing.T) {
	fields := []wireBSONSetField{{Key: "field0", RawValue: mustBSONBinaryRawValue(t, []byte("alpha"))}}
	body, err := appendUpdateBSONSetBody(nil, 99, []byte("user1"), fields, wireAckVisible, []wireSection{
		{ID: wireSectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: wireSectionExpectedCatalog, Bytes: binary.AppendUvarint(nil, 7)},
	})
	if err != nil {
		t.Fatalf("appendUpdateBSONSetBody: %v", err)
	}
	if id, err := testDecodeCommandID(body); err != nil || id != wireCommandUpdateBSONSet {
		t.Fatalf("command id=%d err=%v want update_bson_set", id, err)
	}
	sections, err := decodeWireSections(body)
	if err != nil {
		t.Fatalf("decodeWireSections: %v", err)
	}
	if handle, err := testDecodeCollectionHandle(body); err != nil || handle != 99 {
		t.Fatalf("collection handle=%d err=%v want 99", handle, err)
	}
	rawIDs, ok, err := singletonWireSection(sections, wireSectionDocumentIDs)
	if err != nil || !ok {
		t.Fatalf("document ids section ok=%v err=%v", ok, err)
	}
	ids, err := decodeWireByteVector(rawIDs)
	if err != nil {
		t.Fatalf("decode ids: %v", err)
	}
	if len(ids) != 1 || string(ids[0]) != "user1" {
		t.Fatalf("ids=%q want user1", ids)
	}
	rawNames, ok, err := singletonWireSection(sections, wireSectionUpdateFieldNames)
	if err != nil || !ok {
		t.Fatalf("field names section ok=%v err=%v", ok, err)
	}
	names, err := decodeWireByteVector(rawNames)
	if err != nil {
		t.Fatalf("decode names: %v", err)
	}
	if len(names) != 1 || string(names[0]) != "field0" {
		t.Fatalf("names=%q want field0", names)
	}
	rawValues, ok, err := singletonWireSection(sections, wireSectionUpdateFieldValues)
	if err != nil || !ok {
		t.Fatalf("field values section ok=%v err=%v", ok, err)
	}
	values, err := decodeWireByteVector(rawValues)
	if err != nil {
		t.Fatalf("decode values: %v", err)
	}
	if len(values) != 1 || !reflect.DeepEqual(values[0], fields[0].RawValue) {
		t.Fatalf("values=%v want %v", values, fields[0].RawValue)
	}
}

func TestTreeDBRowEncodingJSONOverrideRoundTrip(t *testing.T) {
	db := &treedbDB{keyField: treedbDefaultKeyField, documentFormat: wireDocumentFormatJSON}
	values := map[string][]byte{
		"field0": []byte("alpha"),
		"field1": []byte{0, 1, 2, 3},
	}

	doc, err := db.encodeRow("user123", values)
	if err != nil {
		t.Fatalf("encodeRow: %v", err)
	}
	got, err := db.decodeRow(doc, nil)
	if err != nil {
		t.Fatalf("decodeRow: %v", err)
	}
	if !reflect.DeepEqual(got, values) {
		t.Fatalf("decodeRow=%v want %v", got, values)
	}
}

func TestEnsureCollectionDocumentFormatMeta(t *testing.T) {
	db := &treedbDB{documentFormat: wireDocumentFormatBSON}
	if err := db.ensureCollectionDocumentFormatMeta("usertable", wireCollectionMeta{Name: "usertable", DocumentFormat: wireDocumentFormatBSON}); err != nil {
		t.Fatalf("BSON collection rejected: %v", err)
	}

	err := db.ensureCollectionDocumentFormatMeta("usertable", wireCollectionMeta{Name: "usertable", DocumentFormat: wireDocumentFormatJSON})
	if err == nil {
		t.Fatal("JSON collection accepted by BSON client, want mismatch")
	}
	for _, want := range []string{"document_format=json", "treedb.document_format=bson", "-p treedb.document_format=json"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("mismatch error %q does not contain %q", err, want)
		}
	}

	jsonDB := &treedbDB{documentFormat: wireDocumentFormatJSON}
	if err := jsonDB.ensureCollectionDocumentFormatMeta("legacy", wireCollectionMeta{Name: "legacy", DocumentFormat: wireDocumentFormatDefault}); err != nil {
		t.Fatalf("default-format collection should normalize to JSON: %v", err)
	}
}

func TestClientAndHandleClosesHandleOnDocumentFormatMismatch(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	closeCalls := make(chan uint64, 1)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveFormatMismatchNativeWire(serverConn, closeCalls)
	}()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	db := &treedbDB{documentFormat: wireDocumentFormatBSON}
	state := &treedbState{
		client:          &nativeWireClient{conn: clientConn},
		handles:         make(map[string]uint64),
		scanIndexBroken: make(map[string]bool),
	}
	ctx := context.WithValue(context.Background(), stateContextKey, state)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	_, _, _, err := db.clientAndHandle(ctx, "usertable")
	if err == nil {
		t.Fatal("clientAndHandle succeeded, want document format mismatch")
	}
	if !strings.Contains(err.Error(), "document_format=json") {
		t.Fatalf("clientAndHandle error %q missing document format mismatch", err)
	}

	select {
	case handle := <-closeCalls:
		if handle != 99 {
			t.Fatalf("closed handle %d, want 99", handle)
		}
	case <-time.After(time.Second):
		t.Fatal("clientAndHandle did not close opened handle after mismatch")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("fake server: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fake server did not exit")
	}
	if _, ok := state.handle("usertable"); ok {
		t.Fatal("mismatched handle was cached")
	}
}

func serveFormatMismatchNativeWire(conn net.Conn, closeCalls chan<- uint64) error {
	defer conn.Close()
	for {
		header, body, err := readWireFrameInto(conn, nil)
		if err != nil {
			if errorsIsClosed(err) {
				return nil
			}
			return err
		}
		commandID, err := testDecodeCommandID(body)
		if err != nil {
			return err
		}
		switch commandID {
		case wireCommandOpenCollection:
			rawHandle := binary.AppendUvarint(nil, 99)
			if err := testWriteResponse(conn, header.RequestID, []wireSection{{ID: wireSectionCollectionHandle, Bytes: rawHandle}}); err != nil {
				return err
			}
		case wireCommandListCollections:
			meta := encodeCollectionMeta("usertable", true, treedbDefaultKeyField, wireDocumentFormatJSON)
			if err := testWriteResponse(conn, header.RequestID, []wireSection{{ID: wireSectionCollectionMeta, Bytes: appendWireByteVector(nil, meta)}}); err != nil {
				return err
			}
		case wireCommandCloseCollection:
			handle, err := testDecodeCollectionHandle(body)
			if err != nil {
				return err
			}
			closeCalls <- handle
			if err := testWriteResponse(conn, header.RequestID, nil); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("unexpected command %d", commandID)
		}
	}
}

func testDecodeCommandID(body []byte) (wireCommandID, error) {
	sections, err := decodeWireSections(body)
	if err != nil {
		return 0, err
	}
	raw, ok, err := singletonWireSection(sections, wireSectionCommandHeader)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("missing command header")
	}
	id, _, err := readWireUvarint(raw)
	if err != nil {
		return 0, err
	}
	return wireCommandID(id), nil
}

func testDecodeCollectionHandle(body []byte) (uint64, error) {
	sections, err := decodeWireSections(body)
	if err != nil {
		return 0, err
	}
	raw, ok, err := singletonWireSection(sections, wireSectionCollectionRef)
	if err != nil {
		return 0, err
	}
	if !ok || len(raw) == 0 || raw[0] != 2 {
		return 0, fmt.Errorf("missing collection handle ref")
	}
	handle, n, err := readWireUvarint(raw[1:])
	if err != nil {
		return 0, err
	}
	if n != len(raw)-1 {
		return 0, fmt.Errorf("collection handle has trailing bytes")
	}
	return handle, nil
}

func testWriteResponse(conn net.Conn, requestID uint64, sections []wireSection) error {
	body, err := appendWireSections(nil, sections)
	if err != nil {
		return err
	}
	return writeWireFrame(conn, wireHeader{Type: wireFrameResponse, RequestID: requestID}, body)
}

func mustBSONBinaryRawValue(t *testing.T, value []byte) []byte {
	t.Helper()
	raw, err := bsonBinaryRawValue(value)
	if err != nil {
		t.Fatalf("bsonBinaryRawValue: %v", err)
	}
	return raw
}

func errorsIsClosed(err error) bool {
	return err == io.EOF || strings.Contains(err.Error(), "closed")
}
