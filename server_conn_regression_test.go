package engineio

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/sunmi-OS/go-engine.io/parser"
)

func packetReaderForTest(t *testing.T) (*connReader, <-chan struct{}) {
	t.Helper()
	decoder, err := parser.NewDecoder(bytes.NewBufferString("4payload"))
	if err != nil {
		t.Fatal(err)
	}
	connection := &serverConn{state: stateNormal, readerChan: make(chan *connReader)}
	handled := make(chan struct{})
	go func() {
		connection.OnPacket(decoder)
		close(handled)
	}()
	select {
	case reader := <-connection.readerChan:
		return reader, handled
	case <-time.After(time.Second):
		t.Fatal("packet reader was not delivered")
		return nil, nil
	}
}

func TestConnReaderPacketCompletion(t *testing.T) {
	reader, handled := packetReaderForTest(t)
	payload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "payload" {
		t.Fatalf("unexpected payload: %q", payload)
	}
	select {
	case <-handled:
		t.Fatal("packet handler returned before reader closed")
	default:
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("packet handler did not receive reader completion")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnReaderPacketTimeout(t *testing.T) {
	reader, handled := packetReaderForTest(t)
	select {
	case <-handled:
	case <-time.After(35 * time.Second):
		t.Fatal("packet timeout cleanup did not finish")
	}
	for attempt := 0; attempt < 100; attempt++ {
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
