package control

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

func readFrameFromBytes(t *testing.T, data []byte, deadline time.Duration) ([]byte, error) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	go client.Write(data)
	br := bufio.NewReader(server)
	return ReadFrame(br, server, deadline)
}

func TestReadFrameOrdinary(t *testing.T) {
	frame, err := readFrameFromBytes(t, []byte("hello\n"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(frame) != "hello" {
		t.Fatalf("got %q", frame)
	}
}

func TestReadFrameStripsOnlyTheTerminatingLF(t *testing.T) {
	frame, err := readFrameFromBytes(t, []byte("{\"a\":1}\n"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(frame) != `{"a":1}` {
		t.Fatalf("got %q", frame)
	}
}

func TestReadFrameRejectsCRLF(t *testing.T) {
	_, err := readFrameFromBytes(t, []byte("hello\r\n"), time.Second)
	if !errors.Is(err, errFrameBadBytes) {
		t.Fatalf("got %v", err)
	}
}

func TestReadFrameRejectsBOM(t *testing.T) {
	data := append(append([]byte{}, utf8BOM...), []byte("hello\n")...)
	_, err := readFrameFromBytes(t, data, time.Second)
	if !errors.Is(err, errFrameBadBytes) {
		t.Fatalf("got %v", err)
	}
}

func TestReadFrameAcceptsExactlyMaxFrameBytes(t *testing.T) {
	content := bytes.Repeat([]byte("a"), MaxFrameBytes-1)
	data := append(content, '\n')
	frame, err := readFrameFromBytes(t, data, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) != MaxFrameBytes-1 {
		t.Fatalf("got frame length %d", len(frame))
	}
}

func TestReadFrameRejectsOverMaxFrameBytes(t *testing.T) {
	content := bytes.Repeat([]byte("a"), MaxFrameBytes)
	data := append(content, '\n')
	_, err := readFrameFromBytes(t, data, 5*time.Second)
	if !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("got %v", err)
	}
}

func TestReadFrameDeadlineExceededBetweenFirstByteAndCompletion(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	go func() {
		client.Write([]byte{'x'})
		time.Sleep(50 * time.Millisecond)
		client.Write([]byte("y\n"))
	}()
	br := bufio.NewReader(server)
	_, err := ReadFrame(br, server, 10*time.Millisecond)
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected a timeout net.Error, got %v", err)
	}
}

func TestReadFrameNoDeadlineWhileWaitingForFirstByte(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	go func() {
		time.Sleep(30 * time.Millisecond) // longer than the deadline below
		client.Write([]byte("late\n"))
	}()
	br := bufio.NewReader(server)
	frame, err := ReadFrame(br, server, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("idle wait for first byte must not itself time out: %v", err)
	}
	if string(frame) != "late" {
		t.Fatalf("got %q", frame)
	}
}
