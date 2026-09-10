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
	"sort"
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
	// ErrDeliveryPending means a well-formed marker names an envelope still
	// in the 'dispatching' state: dispatch's pre-attempt commit has landed
	// (design plan §3's two-phase dispatch) but the outcome of the actual
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

// bridgeReplyOpener matches our own marker's exact opening fence line,
// anchored to its own line (^...$ under multiline mode) so a longer backtick
// run (e.g. "````BRIDGE-REPLY") never satisfies it.
//
// bridgeReplyPrefix matches any line beginning with the reserved
// "```BRIDGE-REPLY" literal, regardless of what follows — checked before
// genericFenceLine so a malformed variant (e.g. a non-breaking space instead
// of an ASCII space/tab after BRIDGE-REPLY) is recognized as an attempted
// marker opener, not silently swallowed as unrelated quoted fence content.
//
// genericFenceLine matches any Markdown code-fence delimiter line: up to
// three leading spaces, a run of three or more backticks or tildes, then the
// rest of the line (an info string, for an opening line).
var (
	bridgeReplyOpener = regexp.MustCompile(`(?m)^` + "```" + `BRIDGE-REPLY[ \t]*$`)
	bridgeReplyPrefix = regexp.MustCompile(`(?m)^` + "```" + `BRIDGE-REPLY`)
	backtickFenceLine = regexp.MustCompile("^ {0,3}(`{3,})([^`]*)$")
	tildeFenceLine    = regexp.MustCompile("^ {0,3}(~{3,})(.*)$")
)

// matchGenericFence reports whether line opens or closes some Markdown
// fence (ours or unrelated), returning the fence run and the rest of the
// line. Two separate patterns, not one alternation, because CommonMark's
// info-string rule differs by fence character: a backtick fence's info
// string must not itself contain a backtick (ambiguous with an inline code
// span), while a tilde fence's info string has no such restriction.
func matchGenericFence(line string) (run, rest string, ok bool) {
	if m := backtickFenceLine.FindStringSubmatch(line); m != nil {
		return m[1], m[2], true
	}
	if m := tildeFenceLine.FindStringSubmatch(line); m != nil {
		return m[1], m[2], true
	}
	return "", "", false
}

// closesFence reports whether line closes a fence opened with run: the same
// character, at least as long, and — per Markdown's own closing-fence rule —
// no trailing info string. The trailing check trims only ASCII space and tab,
// matching bridgeReplyOpener's own "[ \t]*" — strings.TrimSpace additionally
// strips other Unicode whitespace (e.g. U+00A0 NBSP, '\v'), which would let a
// malformed closer like "``` " be accepted as a clean close instead of
// rejected as ErrMalformedMarker.
func closesFence(line, run string) bool {
	fenceRun, rest, ok := matchGenericFence(line)
	if !ok {
		return false
	}
	return fenceRun[0] == run[0] && len(fenceRun) >= len(run) && strings.Trim(rest, " \t") == ""
}

// markerScan is the result of scanning a turn's text for BRIDGE-REPLY
// fences.
type markerScan struct {
	openerCount int
	content     string
	closed      bool
}

// htmlCommentOpen/htmlCommentClose detect a CommonMark raw-HTML comment span
// at top level. Per CommonMark, everything from an unclosed "<!--" through
// the line containing its matching "-->" is raw HTML, never parsed as
// Markdown — a fenced code block (including our own marker syntax) written
// inside such a comment, e.g. as hidden documentation or prompt metadata,
// never actually opens a live fence and must not be scanned as one.
var (
	htmlCommentOpen  = regexp.MustCompile(`<!--`)
	htmlCommentClose = regexp.MustCompile(`-->`)
)

// commentStateAfterLine returns whether an HTML comment span remains open
// after processing line, given whether one was already open entering it. A
// line can contain several openers/closers, and only their relative order —
// not merely whether each substring is present anywhere in the line —
// determines the state at the end of it. A line like "--> <!--" or
// "<!-- first --> <!-- second" contains both "<!--" and "-->", but the
// trailing unmatched opener still leaves a comment open spanning subsequent
// lines; checking presence alone (the previous implementation) gets exactly
// this case backwards.
func commentStateAfterLine(inComment bool, line string) bool {
	type delim struct {
		pos  int
		open bool
	}
	var delims []delim
	for _, m := range htmlCommentOpen.FindAllStringIndex(line, -1) {
		delims = append(delims, delim{m[0], true})
	}
	for _, m := range htmlCommentClose.FindAllStringIndex(line, -1) {
		delims = append(delims, delim{m[0], false})
	}
	sort.Slice(delims, func(i, j int) bool { return delims[i].pos < delims[j].pos })
	for _, d := range delims {
		inComment = d.open
	}
	return inComment
}

// codeSpanBackticks matches a run of one or more backticks, the delimiter
// CommonMark uses for an inline code span.
var codeSpanBackticks = regexp.MustCompile("`+")

// stripCodeSpans removes the contents of inline code spans from line before
// comment-delimiter detection runs, so a literal "<!--" written as
// inline code (e.g. “ `<!--` “) is never mistaken for a real HTML comment
// opener — CommonMark specifies inline code span content as literal text,
// never parsed as raw HTML. Spans are matched by equal-length backtick runs,
// per CommonMark's own code-span rule; an unmatched trailing backtick run is
// left as-is; it isn't a code span.
//
// A removed span is replaced with a single space, not deleted outright: the
// text immediately before and after the span are otherwise unrelated
// fragments that were never adjacent in the source (e.g. literal "<!" then a
// code span then literal "--"), and closing the gap between them can
// synthesize a comment delimiter ("<!--") that was never actually present.
// A space can't itself participate in either delimiter, so it can't create
// a false one, and it can't destroy a real one either — a genuine "<!--"
// never has a code span spliced into the middle of it.
func stripCodeSpans(line string) string {
	matches := codeSpanBackticks.FindAllStringIndex(line, -1)
	if len(matches) < 2 {
		return line
	}
	var b strings.Builder
	last := 0
	for i := 0; i < len(matches); i++ {
		open := matches[i]
		runLen := open[1] - open[0]
		closeIdx := -1
		for j := i + 1; j < len(matches); j++ {
			if matches[j][1]-matches[j][0] == runLen {
				closeIdx = j
				break
			}
		}
		if closeIdx == -1 {
			break
		}
		b.WriteString(line[last:open[0]])
		b.WriteString(" ")
		last = matches[closeIdx][1]
		i = closeIdx
	}
	b.WriteString(line[last:])
	return b.String()
}

// rawHTMLBlockOpener pairs a CommonMark HTML block start condition with how
// it ends: close matches a specific end token appearing later in the text
// (types 1/3/4/5); a nil close means the block instead ends at the next
// blank line (types 6/7). Per CommonMark, everything between is raw HTML,
// never parsed as Markdown — the same reasoning as the comment span above
// (type 2), generalized to every other raw-HTML-block start condition
// CommonMark defines.
// interruptsParagraph is CommonMark's own distinction: types 1-6 can start a
// raw HTML block even immediately after an open paragraph (no blank line
// needed first); type 7 (a generic complete tag alone on a line) cannot —
// while a paragraph is open, a line that only matches type 7's grammar stays
// ordinary paragraph text instead.
type rawHTMLBlockOpener struct {
	open                *regexp.Regexp
	close               *regexp.Regexp
	interruptsParagraph bool
}

// blockLevelTags is CommonMark's fixed list of tag names that start an HTML
// block type 6 (ends at the next blank line, not a specific closing tag).
const blockLevelTags = `address|article|aside|base|basefont|blockquote|body|caption|center|col|` +
	`colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|` +
	`frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|menu|menuitem|nav|noframes|` +
	`ol|optgroup|option|p|param|section|source|summary|table|tbody|td|tfoot|th|thead|title|tr|track|ul`

var rawHTMLBlockOpeners = []rawHTMLBlockOpener{
	// Type 1: script/pre/style/textarea, ends at its specific closing tag.
	// CommonMark's end condition is the exact literal string (case-
	// insensitive) with no internal whitespace — "</script >" does not
	// close it, unlike type 7's general tag grammar elsewhere in this file.
	{regexp.MustCompile(`(?i)^ {0,3}<script(?:[\s>]|$)`), regexp.MustCompile(`(?i)</script>`), true},
	{regexp.MustCompile(`(?i)^ {0,3}<pre(?:[\s>]|$)`), regexp.MustCompile(`(?i)</pre>`), true},
	{regexp.MustCompile(`(?i)^ {0,3}<style(?:[\s>]|$)`), regexp.MustCompile(`(?i)</style>`), true},
	{regexp.MustCompile(`(?i)^ {0,3}<textarea(?:[\s>]|$)`), regexp.MustCompile(`(?i)</textarea>`), true},
	// Type 3: processing instruction, ends at "?>".
	{regexp.MustCompile(`^ {0,3}<\?`), regexp.MustCompile(`\?>`), true},
	// Type 4: declaration, ends at ">".
	{regexp.MustCompile(`^ {0,3}<![A-Za-z]`), regexp.MustCompile(`>`), true},
	// Type 5: CDATA section, ends at "]]>".
	{regexp.MustCompile(`^ {0,3}<!\[CDATA\[`), regexp.MustCompile(`]]>`), true},
	// Type 6: a fixed list of block-level tag names, ends at a blank line.
	{regexp.MustCompile(`(?i)^ {0,3}</?(?:` + blockLevelTags + `)(?:[\s>]|/>|$)`), nil, true},
	// Type 7: any other complete open/close tag alone on its own line, ends
	// at a blank line. Checked last so types 1-6's more specific tag names
	// take their own termination rule instead of falling through to this
	// blank-line-terminated catch-all. Per CommonMark, type 7 alone cannot
	// interrupt an open paragraph — interruptsParagraph is false only here.
	{regexp.MustCompile(`^ {0,3}(?:` + htmlOpenTag + `|` + htmlCloseTag + `)\s*$`), nil, false},
}

// htmlAttrValue/htmlAttr/htmlOpenTag/htmlCloseTag approximate CommonMark's
// HTML tag grammar for type 7's "complete tag alone on a line" check. A
// quoted attribute value may itself contain '>' or '<' (e.g.
// `<custom title="a > b">`), so an attribute-aware match is required here —
// unlike type 6, whose opener regex only inspects the tag name itself and
// never scans into the attribute list.
const (
	htmlAttrName  = `[A-Za-z_:][A-Za-z0-9_.:-]*`
	htmlAttrValue = `"[^"]*"|'[^']*'|[^\s"'=<>` + "`" + `]+`
	htmlAttr      = `\s+` + htmlAttrName + `(?:\s*=\s*(?:` + htmlAttrValue + `))?`
	htmlOpenTag   = `<[A-Za-z][A-Za-z0-9-]*(?:` + htmlAttr + `)*\s*/?>`
	htmlCloseTag  = `</[A-Za-z][A-Za-z0-9-]*\s*>`
)

// atxHeading, thematicBreak and indentedCodeBlock recognize CommonMark block
// types that are not paragraphs, so scanForMarker's paragraphOpen tracking
// (which gates type 7's interruption rule) doesn't mistake one of these for
// open paragraph text. atxHeading and thematicBreak can themselves interrupt
// a paragraph without a blank line first; indentedCodeBlock deliberately
// cannot (see its use below) — a line this pattern matches straight after an
// open paragraph is CommonMark's lazy-continuation text, not a new block.
var (
	atxHeading        = regexp.MustCompile(`^ {0,3}#{1,6}(?:[ \t]|$)`)
	thematicBreak     = regexp.MustCompile(`^ {0,3}(?:-[ \t]*){3,}$|^ {0,3}(?:_[ \t]*){3,}$|^ {0,3}(?:\*[ \t]*){3,}$`)
	indentedCodeBlock = regexp.MustCompile(`^(?: {4}|\t)`)
)

// matchRawHTMLBlockOpener reports whether line opens one of the raw HTML
// block types above, returning its termination rule: a specific closing
// pattern to watch for, or nil for "ends at the next blank line".
// paragraphOpen gates type 7 only (interruptsParagraph == false): per
// CommonMark, a generic complete tag alone on a line never starts a raw HTML
// block while a paragraph is already open — it's just paragraph text — but
// types 1-6 start one regardless.
func matchRawHTMLBlockOpener(line string, paragraphOpen bool) (close *regexp.Regexp, blankTerminated, ok bool) {
	for _, k := range rawHTMLBlockOpeners {
		if !k.interruptsParagraph && paragraphOpen {
			continue
		}
		if k.open.MatchString(line) {
			return k.close, k.close == nil, true
		}
	}
	return nil, false, false
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
	var inComment bool
	var openRawHTMLClose *regexp.Regexp
	var inRawHTMLBlock bool
	// paragraphOpen tracks CommonMark's paragraph-interruption rule: true
	// once a line of ordinary top-level text has been seen with nothing
	// since to close it (a blank line, or a block that consumes the line).
	// Only used to gate type 7's raw-HTML-block check (matchRawHTMLBlockOpener);
	// types 1-6 and fences can interrupt a paragraph unconditionally, so
	// none of their branches below need to consult it before matching.
	var paragraphOpen bool
	for i, line := range lines {
		trimmed := strings.TrimRight(line, " \t")
		if inComment {
			// Do not strip code spans here: once inside raw HTML, backticks
			// have no Markdown code-span meaning at all — a closer like
			// "`-->`" still closes the comment, and stripping it as if it
			// were a code span would leave inComment stuck true.
			inComment = commentStateAfterLine(inComment, trimmed)
			continue
		}
		if inRawHTMLBlock {
			if openRawHTMLClose != nil {
				if openRawHTMLClose.MatchString(trimmed) {
					inRawHTMLBlock = false
					openRawHTMLClose = nil
				}
			} else if strings.Trim(trimmed, " \t") == "" {
				// ASCII space/tab only, matching closesFence's own rule above:
				// CommonMark defines a blank line as containing nothing but
				// spaces/tabs, not general Unicode whitespace. strings.TrimSpace
				// would also strip e.g. U+00A0 NBSP, wrongly treating a
				// visually-blank-looking line as ending a blank-line-terminated
				// raw HTML block (types 6/7) one line early, exposing a marker
				// on the next line that should still be hidden inside it.
				inRawHTMLBlock = false
			}
			continue
		}
		if openRun == "" {
			if strings.Trim(trimmed, " \t") == "" {
				// ASCII space/tab only — see the matching note in the
				// inRawHTMLBlock branch above for why not strings.TrimSpace.
				paragraphOpen = false
				continue
			}
			// Raw-HTML-block recognition runs before the generic comment
			// check, matching CommonMark's own type-1-before-type-2 start-
			// condition ordering: a self-contained type-1 block whose
			// content happens to contain a literal "<!--" (e.g.
			// <script>const s = "<!--";</script>) must be recognized and
			// closed as type 1 on this same line, not misread as an
			// unclosed HTML comment because the comment check ran first and
			// never saw the type-1 opener at all.
			if closer, blankTerminated, ok := matchRawHTMLBlockOpener(trimmed, paragraphOpen); ok {
				// A closer already present on this same opening line makes
				// the block self-contained (CommonMark: start and end
				// conditions on one line close it immediately) — do not
				// enter the persistent open state, or every later line
				// would stay hidden with nothing left to ever match it.
				paragraphOpen = false
				if blankTerminated {
					inRawHTMLBlock = true
				} else if !closer.MatchString(trimmed) {
					inRawHTMLBlock = true
					openRawHTMLClose = closer
				}
				continue
			}
			scanLine := stripCodeSpans(trimmed)
			if commentStateAfterLine(false, scanLine) {
				inComment = true
				paragraphOpen = false
				continue
			}
			if bridgeReplyOpener.MatchString(trimmed) {
				result.openerCount++
				openRun = "```"
				isOurs = true
				contentStart = i + 1
				paragraphOpen = false
			} else if bridgeReplyPrefix.MatchString(trimmed) {
				// Reserved prefix present but the strict opener pattern
				// didn't match (invalid trailing characters) — a malformed
				// opener attempt, not unrelated fenced content. Counted so
				// the scan can never silently resolve to just some other,
				// well-formed marker elsewhere in the text; still consumes
				// its own "```" fence run so lines up to its close aren't
				// misread as top-level content.
				result.openerCount++
				openRun = "```"
				isOurs = false
				paragraphOpen = false
			} else if run, _, ok := matchGenericFence(trimmed); ok {
				openRun = run
				isOurs = false
				paragraphOpen = false
			} else if atxHeading.MatchString(trimmed) || thematicBreak.MatchString(trimmed) {
				// A heading or thematic break is its own block, not a
				// paragraph, and (per CommonMark) can interrupt an open one
				// without a blank line first — so a following line is never
				// gated by a paragraph this line might have followed.
				paragraphOpen = false
			} else if indentedCodeBlock.MatchString(trimmed) && !paragraphOpen {
				// An indented-looking line only starts a code block when it
				// isn't continuing an already-open paragraph — CommonMark:
				// indented code cannot interrupt a paragraph, so straight
				// after paragraph text this is lazy continuation of that
				// paragraph, not a new block, and paragraphOpen must stay
				// true (the final else branch below handles that case).
				paragraphOpen = false
			} else {
				// Ordinary text, or an indented line continuing an already-
				// open paragraph: continues or opens a paragraph.
				paragraphOpen = true
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
			paragraphOpen = false
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
