package treedbnative

import (
	"reflect"
	"strings"
	"testing"

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
