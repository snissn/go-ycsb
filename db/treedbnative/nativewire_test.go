package treedbnative

import (
	"context"
	"net"
	"testing"
	"time"
)

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
