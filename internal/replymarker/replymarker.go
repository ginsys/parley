// Package replymarker implements the explicit bridge-reply selection rule
// from the design plan's §1b: a peer's ordinary turn output is never
// forwarded on a timing guess. Exactly one machine-parseable marker must be
// present, naming the envelope it replies to and the intended recipient;
// anything else stops delivery rather than guessing.
//
// Concrete syntax: a fenced block opened with "```BRIDGE-REPLY" and closed
// with "```", containing a JSON object with string fields "in_reply_to",
// "to", and "text". JSON was chosen over the plan's illustrative
// unquoted-key notation so parsing has no ambiguous edge cases.
package replymarker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/ginsys/parley/internal/store"
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
)

// Marker is one parsed BRIDGE-REPLY block.
type Marker struct {
	InReplyTo string `json:"in_reply_to"`
	To        string `json:"to"`
	Text      string `json:"text"`
}

// Both patterns anchor the opening and closing fences to their own line
// (^...$ under multiline mode). A prior version matched the closing fence
// as a bare "\n```" substring, so two adjacent markers like
// "```BRIDGE-REPLY\n{a}\n```BRIDGE-REPLY\n{b}\n```" had the second block's
// opener consumed as the first block's closer, yielding one match instead
// of a rejected duplicate. Counting openers independently of the
// fence-extraction regex catches that case even if the extraction regex
// itself were ever loosened again.
var (
	openerPattern = regexp.MustCompile("(?m)^```BRIDGE-REPLY[ \\t]*$")
	fencePattern  = regexp.MustCompile("(?sm)^```BRIDGE-REPLY[ \\t]*\\n(.*?)\\n^```[ \\t]*$")
)

// Extract finds the single BRIDGE-REPLY marker in a turn's text and parses
// it. It never guesses: zero, more than one, or an unparseable/incomplete
// block are all distinct errors, none of which yield a usable Marker.
func Extract(turnText string) (*Marker, error) {
	if openers := openerPattern.FindAllString(turnText, -1); len(openers) == 0 {
		return nil, ErrNoMarker
	} else if len(openers) > 1 {
		return nil, ErrMultipleMarkers
	}

	matches := fencePattern.FindAllStringSubmatch(turnText, -1)
	if len(matches) != 1 {
		return nil, fmt.Errorf("%w: fenced block not properly closed", ErrMalformedMarker)
	}

	m, err := decodeMarker([]byte(matches[0][1]))
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
	return &m, nil
}

// Validate checks a well-formed Marker against bridge state before it may be
// forwarded: the recipient must match the enrolled peer this adapter
// forwards to, in_reply_to must name an envelope on this exact conversation
// still awaiting a reply (queued or handed_off) — never a different
// conversation, an already-acked envelope, or an unknown id — and that
// envelope must have actually been addressed to replyingPeer. Without that
// last check, a peer could name an envelope it sent itself (or one sent to
// a different peer entirely) as long as the conversation and state matched,
// impersonating a reply it was never asked for. Returns the envelope the
// reply resolves against.
func Validate(ctx context.Context, tx *store.Tx, conversation, replyingPeer, expectedTo string, m *Marker) (*store.Envelope, error) {
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
	if e.State != store.Queued && e.State != store.HandedOff {
		return nil, fmt.Errorf("%w: envelope %s is %s, not awaiting reply", ErrStaleReply, m.InReplyTo, e.State)
	}
	return e, nil
}
