package control

import (
	"bufio"
	"bytes"
	"errors"
	"time"
)

const (
	// MaxFrameBytes is the profile's per-frame bound, including the
	// terminating LF.
	MaxFrameBytes = 1 << 20 // 1 MiB

	// FrameDeadline is the maximum time allowed between the first byte of a
	// frame arriving and the frame completing (the terminating LF read).
	// There is no deadline while waiting for a frame's first byte; an idle
	// connection between requests is not a framing violation.
	FrameDeadline = 5 * time.Second
)

var (
	errFrameTooLarge = errors.New("control: frame exceeds the 1 MiB bound")
	errFrameBadBytes = errors.New("control: frame carries a BOM or CRLF line ending")
)

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// deadlineSetter is the subset of net.Conn a frame reader needs. Splitting
// it out lets tests exercise the deadline behavior over a net.Pipe without
// a real socket.
type deadlineSetter interface {
	SetReadDeadline(t time.Time) error
}

// ReadFrame reads one LF-terminated frame from br, returning its content
// with the trailing LF stripped. It enforces MaxFrameBytes (content plus
// LF), rejects a UTF-8 BOM or a CR immediately before the LF (no CRLF line
// endings), and applies deadline starting once the first byte of the frame
// has been read -- an idle connection waiting for its next request is not
// itself a timeout. A partial or oversize frame is reported as an error,
// never returned as a truncated parse target.
func ReadFrame(br *bufio.Reader, d deadlineSetter, deadline time.Duration) ([]byte, error) {
	first, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	if err := d.SetReadDeadline(time.Now().Add(deadline)); err != nil {
		return nil, err
	}
	defer d.SetReadDeadline(time.Time{})

	buf := []byte{first}
	for buf[len(buf)-1] != '\n' {
		if len(buf) >= MaxFrameBytes {
			return nil, errFrameTooLarge
		}
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		buf = append(buf, b)
	}
	frame := buf[:len(buf)-1]
	if bytes.HasSuffix(frame, []byte{'\r'}) || bytes.HasPrefix(frame, utf8BOM) {
		return nil, errFrameBadBytes
	}
	return frame, nil
}
