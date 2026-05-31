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

func errorsIsClosed(err error) bool {
	return err == io.EOF || strings.Contains(err.Error(), "closed")
}
