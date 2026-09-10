// Package replymarker implements the explicit bridge-reply selection rule
// described in docs/architecture.md: a peer's ordinary turn output is never
// forwarded on a timing guess. Exactly one machine-parseable marker must be
// present, naming the envelope it replies to and the intended recipient;
// anything else stops delivery rather than guessing.
//
// Concrete syntax: a fenced block opened with "```BRIDGE-REPLY" and closed
// with "```", containing a JSON object with string fields "in_reply_to",
// "to", and "text". Strict JSON rejects ambiguous object members.
package replymarker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/ginsys/parley/internal/store"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	gmtext "github.com/yuin/goldmark/text"
)

var (
	// ErrNoMarker means the turn contained zero BRIDGE-REPLY blocks — this is
	// ordinary conversation, never forwarded, not an error condition for the
	// caller to alarm on by itself.
	ErrNoMarker = errors.New("no BRIDGE-REPLY marker found")
	// ErrMultipleMarkers means more than one block was found; delivery stops
	// rather than picking one arbitrarily (fixture 8).
	ErrMultipleMarkers = errors.New("multiple BRIDGE-REPLY markers found")
	// ErrMalformedMarker means a block was found but failed to parse or is
	// missing a required field (fixture 7).
	ErrMalformedMarker = errors.New("malformed BRIDGE-REPLY marker")
	// ErrWrongRecipient means a well-formed marker's "to" does not match the
	// enrolled peer this adapter forwards to (fixture 9).
	ErrWrongRecipient = errors.New("BRIDGE-REPLY marker addressed to the wrong recipient")
	// ErrStaleReply means a well-formed marker's "in_reply_to" does not name
	// an envelope currently awaiting reply on this conversation (fixture 10).
	ErrStaleReply = errors.New("BRIDGE-REPLY marker references a stale or unknown envelope")
	// ErrWrongReplier means the referenced envelope was never addressed to
	// the peer producing this reply — only the peer an envelope was actually
	// sent to may reply to it, never the peer that sent it or a third party.
	ErrWrongReplier = errors.New("BRIDGE-REPLY marker references an envelope not addressed to the replying peer")
	// ErrDeliveryPending means a well-formed marker names an envelope still
	// in the 'dispatching' state: dispatch's pre-attempt commit has landed
	// (the claim/host/settlement sequence) but the outcome of the actual
	// host call — success or failure — hasn't been recorded yet. The peer
	// may already have genuinely received the message and be replying in
	// good faith; this is a transient condition, not a stale or unknown
	// reference, and the caller should retry Validate for this turn rather
	// than treat it as a permanent rejection.
	ErrDeliveryPending = errors.New("BRIDGE-REPLY marker references an envelope still being dispatched")
)

// Marker is one parsed BRIDGE-REPLY block.
type Marker struct {
	InReplyTo string `json:"in_reply_to"`
	To        string `json:"to"`
	Text      string `json:"text"`
}

// Markdown determines block membership. The wire contract is deliberately
// narrower: direct document-child fences, exactly three backticks at column
// zero, the exact reserved info string, and an explicit closing fence.
var bridgeReplyOpener = regexp.MustCompile("^```BRIDGE-REPLY[ \t]*$")
var bridgeReplyCloser = regexp.MustCompile("^ {0,3}`{3,}[ \t]*$")

const reservedPrefix = "```BRIDGE-REPLY"

type markerScan struct {
	openerCount int
	content     string
	closed      bool
}

func scanForMarker(input string) markerScan {
	source := []byte(input)
	document := goldmark.New().Parser().Parse(gmtext.NewReader(source))
	var result markerScan
	// Never descend into lists, block quotes, HTML or unrelated fenced blocks.
	for node := document.FirstChild(); node != nil; node = node.NextSibling() {
		fence, ok := node.(*ast.FencedCodeBlock)
		if !ok {
			// Backticks in an invalid info string can make a reserved opener a
			// paragraph or heading instead of a fence. It must still fail closed, not vanish.
			if node.Kind() == ast.KindParagraph || node.Kind() == ast.KindHeading {
				for i := 0; i < node.Lines().Len(); i++ {
					segment := node.Lines().At(i)
					start := bytes.LastIndexByte(source[:segment.Start], '\n') + 1
					line := string(source[start:lineEnd(source, start)])
					if strings.HasPrefix(line, reservedPrefix) {
						result.openerCount++
					}
				}
			}
			continue
		}
		if fence.Info == nil {
			continue
		}
		start := bytes.LastIndexByte(source[:fence.Info.Segment.Start], '\n') + 1
		end := lineEnd(source, start)
		opener := string(source[start:end])
		if !strings.HasPrefix(opener, reservedPrefix) {
			continue
		}
		result.openerCount++
		if !bridgeReplyOpener.MatchString(opener) {
			continue
		}
		var content strings.Builder
		closeStart := end + 1
		for i := 0; i < fence.Lines().Len(); i++ {
			segment := fence.Lines().At(i)
			line := string(segment.Value(source))
			if bridgeReplyOpener.MatchString(strings.TrimSuffix(line, "\n")) {
				result.openerCount++
			}
			content.WriteString(line)
			closeStart = segment.Stop
		}
		result.content = content.String()
		result.closed = closeStart < len(source) && bridgeReplyCloser.Match(source[closeStart:lineEnd(source, closeStart)])
	}
	return result
}

func lineEnd(source []byte, start int) int {
	if i := bytes.IndexByte(source[start:], '\n'); i >= 0 {
		return start + i
	}
	return len(source)
}

// Extract finds the single BRIDGE-REPLY marker in a turn's text and parses
// it. It never guesses: zero, more than one, or an unparseable/incomplete
// block are all distinct errors, none of which yield a usable Marker. Line
// endings are normalized first so a marker sent with Windows-style CRLF
// isn't missed by the line-anchored fence patterns.
func Extract(turnText string) (*Marker, error) {
	text := strings.ReplaceAll(turnText, "\r\n", "\n")
	scan := scanForMarker(text)
	switch {
	case scan.openerCount == 0:
		return nil, ErrNoMarker
	case scan.openerCount > 1:
		return nil, ErrMultipleMarkers
	case !scan.closed:
		return nil, fmt.Errorf("%w: fenced block not properly closed", ErrMalformedMarker)
	}

	m, err := decodeMarker([]byte(scan.content))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedMarker, err)
	}
	if m.InReplyTo == "" || m.To == "" || m.Text == "" {
		return nil, fmt.Errorf("%w: in_reply_to, to and text are all required", ErrMalformedMarker)
	}
	return m, nil
}

// decodeMarker parses the marker JSON object by hand, token by token, rather
// than a plain json.Unmarshal into Marker. encoding/json's struct-based
// decoding matches field names case-insensitively and silently keeps the
// last value of a repeated key, so {"to":"a","to":"b"} or {"TO":"a"} would
// otherwise populate a security-relevant field ambiguously. Walking tokens
// lets every key be checked for an exact, case-sensitive, single occurrence
// before it can set anything.
func decodeMarker(raw []byte) (*Marker, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("expected a JSON object")
	}

	seen := make(map[string]bool, 3)
	var m Marker
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected a string object key, got %v", keyTok)
		}
		if seen[key] {
			return nil, fmt.Errorf("duplicate object member %q", key)
		}
		seen[key] = true

		var val string
		if err := dec.Decode(&val); err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		switch key {
		case "in_reply_to":
			m.InReplyTo = val
		case "to":
			m.To = val
		case "text":
			m.Text = val
		default:
			return nil, fmt.Errorf("unrecognized field %q", key)
		}
	}
	if _, err := dec.Token(); err != nil { // consume the closing '}'
		return nil, err
	}
	// Fence extraction captures everything between the fences, so a
	// second JSON value smuggled in after the first object's closing brace
	// (e.g. "{...} {\"to\":\"attacker\"}") would otherwise be silently
	// discarded rather than rejected — decoding only the first value is not
	// the same as requiring the whole block be exactly one value.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("trailing content after marker object")
	}
	return &m, nil
}

// Validate checks a well-formed Marker against bridge state before it may be
// forwarded: the recipient must match the enrolled peer this adapter
// forwards to, in_reply_to must name an envelope on this exact conversation
// that has actually been handed off to replyingPeer — never a still-queued
// envelope (replyingPeer cannot have seen a message the bridge hasn't
// delivered yet, so acking it from Queued would retire a message that was
// never sent), a different conversation, an already-acked envelope, or an
// unknown id — and that envelope must have actually been addressed to
// replyingPeer. Without that last check, a peer could name an envelope it
// sent itself (or one sent to a different peer entirely) as long as the
// conversation and state matched, impersonating a reply it was never asked
// for. A 'dispatching' envelope is neither: the peer could genuinely have
// already received it before dispatch's own second transaction recorded the
// outcome, so it returns ErrDeliveryPending rather than ErrStaleReply — a
// transient condition the caller should retry, not a permanent rejection.
// Returns the envelope the reply resolves against.
func Validate(ctx context.Context, tx *sql.Tx, conversation, replyingPeer, expectedTo string, m *Marker) (*store.Envelope, error) {
	if m.To != expectedTo {
		return nil, fmt.Errorf("%w: marker to=%q, expected %q", ErrWrongRecipient, m.To, expectedTo)
	}

	e, err := store.GetByID(ctx, tx, m.InReplyTo)
	if errors.Is(err, store.ErrEnvelopeNotFound) {
		return nil, fmt.Errorf("%w: envelope %s not found", ErrStaleReply, m.InReplyTo)
	}
	if err != nil {
		return nil, err
	}
	if e.Conversation != conversation {
		return nil, fmt.Errorf("%w: envelope %s belongs to conversation %s, not %s",
			ErrStaleReply, m.InReplyTo, e.Conversation, conversation)
	}
	if e.ToPeer != replyingPeer {
		return nil, fmt.Errorf("%w: envelope %s was addressed to %s, not %s",
			ErrWrongReplier, m.InReplyTo, e.ToPeer, replyingPeer)
	}
	switch e.State {
	case store.HandedOff:
		// eligible
	case store.Dispatching:
		return nil, fmt.Errorf("%w: envelope %s is still dispatching", ErrDeliveryPending, m.InReplyTo)
	default:
		return nil, fmt.Errorf("%w: envelope %s is %s, not awaiting reply", ErrStaleReply, m.InReplyTo, e.State)
	}
	return e, nil
}
