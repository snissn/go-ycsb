package treedbnative

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestAppendInsertBatchBodyUsesDocumentFormat(t *testing.T) {
	body, err := appendInsertBatchBody(nil, 42, [][]byte{[]byte("id1")}, [][]byte{[]byte("doc1")}, wireDocumentFormatBSON, wireAckVisible, 0, nil)
	if err != nil {
		t.Fatalf("appendInsertBatchBody: %v", err)
	}
	sections, err := decodeWireSections(body)
	if err != nil {
		t.Fatalf("decodeWireSections: %v", err)
	}
	raw, ok, err := singletonWireSection(sections, wireSectionDocumentFormat)
	if err != nil {
		t.Fatalf("document_format section: %v", err)
	}
	if !ok {
		t.Fatal("missing document_format section")
	}
	got, n, err := readWireUvarint(raw)
	if err != nil {
		t.Fatalf("read document_format: %v", err)
	}
	if n != len(raw) {
		t.Fatalf("document_format has %d trailing bytes", len(raw)-n)
	}
	if got != wireDocumentFormatBSON {
		t.Fatalf("document_format=%d want BSON", got)
	}
}

func TestEncodeCollectionMetaUsesDocumentFormat(t *testing.T) {
	raw := encodeCollectionMeta("usertable", true, treedbDefaultKeyField, wireDocumentFormatBSON)
	off := 0
	if _, n, err := readWireUvarint(raw[off:]); err != nil {
		t.Fatalf("collection_meta version: %v", err)
	} else {
		off += n
	}
	name, err := readWireString(raw, &off)
	if err != nil {
		t.Fatalf("collection_meta name: %v", err)
	}
	if name != "usertable" {
		t.Fatalf("collection_meta name=%q want usertable", name)
	}
	got, n, err := readWireUvarint(raw[off:])
	if err != nil {
		t.Fatalf("collection_meta document_format: %v", err)
	}
	off += n
	if got != wireDocumentFormatBSON {
		t.Fatalf("collection_meta document_format=%d want BSON", got)
	}
	if off >= len(raw) {
		t.Fatal("collection_meta unexpectedly ended after document_format")
	}
}

func TestNativeWireClientCancelDeadlineStopWaitsForRunningCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &blockingDeadlineConn{
		entered: make(chan time.Time, 1),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	client := &nativeWireClient{conn: conn}
	stop := client.interruptDeadlineOnContextCancel(ctx)
	cancel()
	select {
	case deadline := <-conn.entered:
		if deadline.IsZero() {
			t.Fatal("cancel callback set zero deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel callback did not set deadline")
	}
	stopDone := make(chan struct{})
	go func() {
		stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("stop returned before running cancel callback completed")
	case <-time.After(25 * time.Millisecond):
	}
	close(conn.release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not return after cancel callback completed")
	}
	select {
	case <-conn.done:
	case <-time.After(time.Second):
		t.Fatal("cancel callback did not complete")
	}
}

type blockingDeadlineConn struct {
	net.Conn
	entered chan time.Time
	release chan struct{}
	done    chan struct{}
}

func (c *blockingDeadlineConn) SetDeadline(t time.Time) error {
	if !t.IsZero() {
		c.entered <- t
		<-c.release
		close(c.done)
	}
	return nil
}
