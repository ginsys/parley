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
	"io"
	"regexp"
	"strings"

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

// bridgeReplyOpener matches our own marker's exact opening fence line,
// anchored to its own line (^...$ under multiline mode) so a longer backtick
// run (e.g. "````BRIDGE-REPLY") never satisfies it.
//
// genericFenceLine matches any Markdown code-fence delimiter line: up to
// three leading spaces, a run of three or more backticks or tildes, then the
// rest of the line (an info string, for an opening line).
var (
	bridgeReplyOpener = regexp.MustCompile(`(?m)^` + "```" + `BRIDGE-REPLY[ \t]*$`)
	genericFenceLine  = regexp.MustCompile("^ {0,3}(`{3,}|~{3,})(.*)$")
)

// closesFence reports whether line closes a fence opened with run: the same
// character, at least as long, and — per Markdown's own closing-fence rule —
// no trailing info string.
func closesFence(line, run string) bool {
	m := genericFenceLine.FindStringSubmatch(line)
	if m == nil {
		return false
	}
	return m[1][0] == run[0] && len(m[1]) >= len(run) && strings.TrimSpace(m[2]) == ""
}

// markerScan is the result of scanning a turn's text for BRIDGE-REPLY
// fences.
type markerScan struct {
	openerCount int
	content     string
	closed      bool
}

// scanForMarker walks text line by line tracking at most one open fence at a
// time — Markdown fences don't nest, so a line that looks like our opener
// while a *different*, unrelated fence (a longer backtick run, a tilde
// fence, or a same-length fence with another info string) is still open is
// just quoted example text, never a live marker. A second BRIDGE-REPLY-
// looking line nested inside our *own* still-open block is a different,
// already-seen case (adjacent markers where the second opener would
// otherwise be consumed as the first block's closer): that still counts
// toward openerCount, so it's rejected as ambiguous rather than silently
// merged into the first block's content.
func scanForMarker(text string) markerScan {
	lines := strings.Split(text, "\n")
	var result markerScan
	var openRun string
	var isOurs bool
	var contentStart int
	for i, line := range lines {
		trimmed := strings.TrimRight(line, " \t")
		if openRun == "" {
			if bridgeReplyOpener.MatchString(trimmed) {
				result.openerCount++
				openRun = "```"
				isOurs = true
				contentStart = i + 1
			} else if m := genericFenceLine.FindStringSubmatch(trimmed); m != nil {
				openRun = m[1]
				isOurs = false
			}
			continue
		}
		if isOurs && bridgeReplyOpener.MatchString(trimmed) {
			result.openerCount++
			continue
		}
		if closesFence(trimmed, openRun) {
			if isOurs {
				result.content = strings.Join(lines[contentStart:i], "\n")
				result.closed = true
			}
			openRun = ""
			isOurs = false
		}
	}
	return result
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
	// The fence-extraction regex captures everything between the fences, so a
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
