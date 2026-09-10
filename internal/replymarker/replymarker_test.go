package replymarker_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/replymarker"
	"github.com/ginsys/parley/internal/store"
)

func openTestDB(t *testing.T) *store.DB {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "parley.db")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func queueOne(t *testing.T, db *store.DB) (conversation string, id string) {
	t.Helper()
	ctx := context.Background()
	ctrl := controller.New(db)
	conversation = "conv-marker"
	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.Bidirectional,
		MaxExchanges: 10,
	}); err != nil {
		t.Fatalf("grant: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		t.Fatalf("current grant: %v", err)
	}
	e := store.Envelope{
		ID:           "env-1",
		Conversation: conversation,
		FromPeer:     "claude-session-a",
		ToPeer:       "codex-thread-b",
		Text:         "please respond",
		GrantVersion: g.GrantVersion,
		State:        store.Queued,
		CreatedAt:    "2026-01-01T00:00:00Z",
		UpdatedAt:    "2026-01-01T00:00:00Z",
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatalf("insert queued: %v", err)
	}
	// Only a handed-off envelope is eligible for a reply to ack (Validate):
	// replyingPeer cannot have seen a message the bridge never delivered.
	if err := store.SetState(ctx, tx, e.ID, store.Queued, store.HandedOff, "2026-01-01T00:00:01Z"); err != nil {
		t.Fatalf("set handed off: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return conversation, e.ID
}

func TestExtractValidMarker(t *testing.T) {
	turn := "some prose\n```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"here you go\"}\n```\nmore prose"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if m.InReplyTo != "env-1" || m.To != "claude-session-a" || m.Text != "here you go" {
		t.Fatalf("unexpected marker: %+v", m)
	}
}

func TestExtractNoMarker(t *testing.T) {
	if _, err := replymarker.Extract("just ordinary conversation, no marker here"); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker, got %v", err)
	}
}

// Fixture 7: malformed reply marker — invalid syntax must stop delivery, not
// fall back to forwarding raw turn content.
func TestExtractMalformedMarker(t *testing.T) {
	cases := []string{
		"```BRIDGE-REPLY\nnot json at all\n```",
		"```BRIDGE-REPLY\n{\"to\": \"claude-session-a\", \"text\": \"missing in_reply_to\"}\n```",
		"```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"text\": \"missing to\"}\n```",
		// Regression for a finding on PR #3: a marker with in_reply_to and to
		// but no text must also be rejected, not accepted as an empty reply.
		"```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\"}\n```",
	}
	for _, turn := range cases {
		if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
			t.Fatalf("turn %q: want ErrMalformedMarker, got %v", turn, err)
		}
	}
}

// Regression for a finding on PR #3: encoding/json's plain struct unmarshal
// keeps the last value of a repeated JSON object key, so a marker repeating
// a security-relevant field (e.g. "to") would resolve ambiguously instead
// of being rejected outright.
func TestExtractRejectsDuplicateObjectMember(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"unexpected\", \"to\": \"expected\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker, got %v", err)
	}
}

// Regression for a finding on PR #3: encoding/json matches struct fields
// case-insensitively, so a marker using "TO" instead of "to" would silently
// populate the same security-relevant field. JSON member names are
// case-sensitive; only the exact declared names are accepted.
func TestExtractRejectsWrongCaseFieldName(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"TO\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker, got %v", err)
	}
}

// Regression for a finding on PR #3: decodeMarker consumed only the first
// JSON value in the fenced block, so a second value smuggled in right after
// the first object's closing brace was silently discarded rather than
// rejected — the block must contain exactly one JSON value, not merely start
// with one.
func TestExtractRejectsTrailingContent(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"} {\"to\": \"attacker\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker, got %v", err)
	}
}

// Regression for a finding on PR #3: before the fences were anchored to
// their own line, a four-backtick fence like "````BRIDGE-REPLY" could still
// satisfy an unanchored opener/closer match by consuming three of its four
// backticks, letting quoted protocol documentation be forwarded as if it
// were a real marker. The current line-anchored patterns must not match a
// four-backtick fence at all — it should read as ordinary conversation.
func TestExtractIgnoresFourBacktickFence(t *testing.T) {
	turn := "discussing the protocol:\n````BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n````"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a four-backtick fence, got %v", err)
	}
}

// Regression for a finding on PR #3: a valid marker followed by a second,
// truncated opener with no closing fence at all is ambiguous and must stop
// delivery — it must not be treated as exactly one valid marker just
// because only one block happens to close.
func TestExtractRejectsTruncatedSecondOpener(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"a\"}\n```\n" +
		"and then i started another one but never finished:\n```BRIDGE-REPLY\noops"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMultipleMarkers) {
		t.Fatalf("want ErrMultipleMarkers, got %v", err)
	}
}

// Regression for a finding on PR #3: quoting the marker's exact syntax
// inside an outer four-backtick (or tilde) fence — e.g. documentation
// showing the protocol by example — must not be extracted as a live marker.
// Markdown fences don't nest, so a shorter same-character fence inside a
// still-open outer one is never itself a real fence boundary; it must be
// treated the same as any other quoted text.
func TestExtractIgnoresMarkerNestedInsideOuterFence(t *testing.T) {
	turn := "discussing the protocol:\n````\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n````"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside an outer fence, got %v", err)
	}
}

func TestExtractIgnoresMarkerNestedInsideOuterTildeFence(t *testing.T) {
	turn := "discussing the protocol:\n~~~\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n~~~"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside an outer tilde fence, got %v", err)
	}
}

// Regression for a finding on PR #3: a marker sent with Windows-style CRLF
// line endings must still be recognized — the fence patterns anchor to line
// boundaries, and an un-normalized "\r" before each "$" broke that anchor.
func TestExtractAcceptsCRLFLineEndings(t *testing.T) {
	turn := "some prose\r\n```BRIDGE-REPLY\r\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"here you go\"}\r\n```\r\nmore prose"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if m.InReplyTo != "env-1" || m.To != "claude-session-a" || m.Text != "here you go" {
		t.Fatalf("unexpected marker: %+v", m)
	}
}

// Regression for a finding on PR #3: a closing fence line with trailing
// non-ASCII whitespace (e.g. U+00A0 NBSP) or a control character outside
// " \t" (e.g. '\v') must not be accepted as a clean close — strings.TrimSpace
// treats both as blank, but the opener's own "[ \t]*" rule permits only
// ASCII space and tab, so a closer this malformed must stop delivery instead
// of silently succeeding.
func TestExtractRejectsClosingFenceWithNonASCIIWhitespace(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n``` "
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker for a closing fence with trailing NBSP, got %v", err)
	}
}

// Fixture 8: duplicate reply markers — two well-formed markers in one turn
// must stop delivery, not pick one arbitrarily.
// Regression for a finding on PR #3: a valid marker followed by a second
// block whose opener has our reserved "```BRIDGE-REPLY" prefix but an
// invalid suffix (e.g. a non-breaking space instead of an ASCII space/tab)
// must still count as an opener attempt, not fall through to genericFenceLine
// and be silently treated as unrelated quoted content — which would let
// Extract return the first marker as if the malformed second attempt never
// existed.
func TestExtractRejectsMalformedReservedOpener(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"first\"}\n```\n" +
		"and then:\n```BRIDGE-REPLY \nnot a real opener\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMultipleMarkers) {
		t.Fatalf("want ErrMultipleMarkers for a well-formed marker plus a malformed reserved-opener attempt, got %v", err)
	}
}

// Regression for a finding on PR #3: a marker's exact syntax placed inside a
// multi-line HTML comment (`<!-- ... -->`) must not be extracted — per
// CommonMark, an HTML comment block is raw HTML, never parsed as Markdown, so
// a fenced code block written inside one never actually opens.
func TestExtractIgnoresMarkerInsideHTMLComment(t *testing.T) {
	turn := "hidden protocol notes:\n<!--\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n-->"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside an HTML comment, got %v", err)
	}
}

// An inline comment opener does not turn a paragraph into an HTML block.
// A type-2 HTML block ends on its first line containing -->; a second
// opener on that consumed line does not extend it onto the next line.
func TestExtractFindsMarkerAfterInlineOrClosedBlockComment(t *testing.T) {
	for _, prefix := range []string{"closed already --> then reopened <!--", "<!-- first --> <!-- second", "Paragraph <!--", "` unmatched ``<!--``"} {
		turn := prefix + "\n```BRIDGE-REPLY\n" + `{"in_reply_to":"env-1","to":"claude-session-a","text":"hi"}` + "\n```\n-->"
		marker, err := replymarker.Extract(turn)
		if err != nil || marker.Text != "hi" {
			t.Fatalf("prefix=%q marker=%+v error=%v", prefix, marker, err)
		}
	}
}

// Regression for a finding on the merge-triggered review: CommonMark defines
// several raw-HTML block start conditions beyond comments — script, pre,
// style, and textarea tags all suppress Markdown parsing (including our own
// fence syntax) until their matching closing tag.
func TestExtractIgnoresMarkerInsideScriptBlock(t *testing.T) {
	turn := "hidden protocol notes:\n<script>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n</script>"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside a <script> block, got %v", err)
	}
}

func TestExtractIgnoresMarkerInsidePreBlock(t *testing.T) {
	turn := "example turn:\n<pre>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n</pre>"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside a <pre> block, got %v", err)
	}
}

// A raw HTML block that opens and closes on the same line is self-contained
// per CommonMark; it must not leave the scanner stuck permanently hidden
// with no closing tag left to ever match.
func TestExtractFindsMarkerAfterSelfContainedScriptLine(t *testing.T) {
	turn := "<script>var x = 1;</script>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after a self-contained <script> line, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// Regression for a finding on review 5160464724's follow-up: CommonMark's
// raw-HTML block types extend beyond script/pre/style/textarea (type 1) to
// block-level tags like <div> (type 6), which suppress Markdown parsing
// until the next blank line rather than a specific closing tag.
func TestExtractIgnoresMarkerInsideDivBlock(t *testing.T) {
	turn := "hidden protocol notes:\n<div>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n</div>"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside a <div> block, got %v", err)
	}
}

// Regression for finding 3974111038 on PR #3's follow-up round: CommonMark's
// type-1 raw HTML block (script/pre/style/textarea) ends only at the exact
// literal closing tag ("</script>", case-insensitive) — internal whitespace
// like "</script >" does not close it, unlike type 7's general tag grammar.
// Verified against the reference CommonMark implementation: the whole rest
// of the document, including a real BRIDGE-REPLY fence, stays inert raw HTML
// when the only "closer" present has that extra space. A permissive \s*
// match would wrongly treat the block as closed there and expose the marker.
func TestExtractIgnoresMarkerAfterScriptBlockWithNonExactClosingTag(t *testing.T) {
	turn := "<script>\nhidden\n</script >\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	_, err := replymarker.Extract(turn)
	if !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker (marker still hidden inside the unclosed <script> block), got %v", err)
	}
}

// A block-level tag block ends at the next blank line, not a specific
// closing tag — a marker after that blank line is live again.
func TestExtractFindsMarkerAfterBlankLineEndsDivBlock(t *testing.T) {
	turn := "<div>\nhidden\n\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after the blank line closing the <div> block, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// Regression for a finding on review 5160464724's follow-up: a literal
// "<!--" written as inline code (backtick-delimited) is CommonMark literal
// text, never a real HTML comment opener, and must not swallow a later,
// genuinely top-level marker.
func TestExtractIgnoresCommentOpenerInsideCodeSpan(t *testing.T) {
	turn := "the opener is `<!--` in Markdown\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after a code-span-quoted comment opener, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// Regression for a finding on review 5160464724's second follow-up: a type-7
// raw HTML tag whose quoted attribute value itself contains '>' (e.g.
// `<custom title="a > b">`) must still be recognized as opening the block —
// the prior `[^<>]*` restriction rejected the opener entirely, letting the
// following marker through as a live top-level one even though CommonMark
// keeps it hidden until a blank line.
func TestExtractIgnoresMarkerInsideCustomTagWithQuotedAngleBracket(t *testing.T) {
	turn := "<custom title=\"a > b\">\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker inside a type-7 block opened by a quoted-'>' attribute tag, got %v", err)
	}
}

// The same type-7 block still ends at the next blank line, same as before.
func TestExtractFindsMarkerAfterBlankLineEndsCustomTagBlock(t *testing.T) {
	turn := "<custom title=\"a > b\">\nhidden\n\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after the blank line closing the custom-tag block, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// Regression for finding 3973918524 on PR #3: CommonMark defines a blank
// line as containing only ASCII space/tab, not general Unicode whitespace.
// A line holding only U+00A0 (NBSP) must not end a blank-line-terminated
// raw HTML block (types 6/7) — the marker must stay hidden inside the still-
// open <div>, not be exposed as if the block had already ended.
func TestExtractIgnoresMarkerAfterNonASCIIWhitespaceOnlyLineInDivBlock(t *testing.T) {
	turn := "<div>\nhidden\n \n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	_, err := replymarker.Extract(turn)
	if !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker (marker still hidden inside the <div> block), got %v", err)
	}
}

// Regression for a finding on review 5160464724's second follow-up: once an
// HTML comment is already open, a closing "-->" surrounded by backticks
// still closes it — inside raw HTML, backticks carry no Markdown code-span
// meaning. Stripping code spans there (the prior implementation) left
// inComment stuck true and swallowed a later, genuinely top-level marker.
func TestExtractFindsMarkerAfterBacktickWrappedCommentCloser(t *testing.T) {
	turn := "<!--\nhidden\n`-->`\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after a backtick-wrapped HTML comment closer, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// Regression for a finding on review 5160464724's second follow-up:
// CommonMark forbids a backtick in a backtick-fence's info string, so a line
// like "``` `weird` info" never opens a fence at all. The prior unrestricted
// "(.*)" info-string pattern accepted it, hiding a later, genuinely
// top-level marker as if it were nested inside that bogus fence.
func TestExtractFindsMarkerAfterBacktickFenceWithBacktickInInfoString(t *testing.T) {
	turn := "``` `weird` info\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after a backtick-fence-lookalike line with a backtick in its info string, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

func TestExtractDuplicateMarkers(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"first\"}\n```\n" +
		"and also\n```BRIDGE-REPLY\n{\"in_reply_to\": \"env-2\", \"to\": \"claude-session-a\", \"text\": \"second\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMultipleMarkers) {
		t.Fatalf("want ErrMultipleMarkers, got %v", err)
	}
}

// Regression for a finding on PR #3: two markers immediately adjacent, with
// no other content between the first block's closing fence position and the
// second block's opening fence, must still be rejected as a duplicate — the
// second opener must never be consumed as the first block's closer.
func TestExtractAdjacentMarkersRejectedAsDuplicate(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"a\"}\n" +
		"```BRIDGE-REPLY\n{\"in_reply_to\": \"env-2\", \"to\": \"claude-session-a\", \"text\": \"b\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMultipleMarkers) {
		t.Fatalf("want ErrMultipleMarkers, got %v", err)
	}
}

// Fixture 9: wrong-recipient reply — a well-formed, parseable marker whose
// "to" does not match the enrolled peer must be rejected, not forwarded on
// the strength of syntax alone.
func TestValidateWrongRecipient(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	m := &replymarker.Marker{InReplyTo: id, To: "some-other-peer", Text: "hi"}
	_, err = replymarker.Validate(ctx, tx, conversation, "codex-thread-b", "claude-session-a", m)
	if !errors.Is(err, replymarker.ErrWrongRecipient) {
		t.Fatalf("want ErrWrongRecipient, got %v", err)
	}
}

// Regression for a finding on PR #4: Validate used to accept a still-queued
// envelope (never handed off to the peer) as eligible for a reply to ack —
// replyingPeer cannot have seen a message the bridge never delivered, so
// acking it would retire a message that was never sent. Only a handed-off
// envelope may be acked.
func TestValidateRejectsQueuedEnvelope(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	ctrl := controller.New(db)
	conversation := "conv-never-dispatched"
	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.Bidirectional,
		MaxExchanges: 10,
	}); err != nil {
		t.Fatalf("grant: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		t.Fatalf("current grant: %v", err)
	}
	e := store.Envelope{
		ID:           "env-never-dispatched",
		Conversation: conversation,
		FromPeer:     "claude-session-a",
		ToPeer:       "codex-thread-b",
		Text:         "please respond",
		GrantVersion: g.GrantVersion,
		State:        store.Queued,
		CreatedAt:    "2026-01-01T00:00:00Z",
		UpdatedAt:    "2026-01-01T00:00:00Z",
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatalf("insert queued: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx2.Rollback()
	m := &replymarker.Marker{InReplyTo: e.ID, To: "claude-session-a", Text: "hi"}
	if _, err := replymarker.Validate(ctx, tx2, conversation, "codex-thread-b", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
		t.Fatalf("want ErrStaleReply for a still-queued (never handed off) envelope, got %v", err)
	}
}

// Regression for a finding on PR #3: a reply arriving while the envelope is
// still 'dispatching' (dispatch's pre-attempt commit has landed but the
// second transaction recording the host call's outcome hasn't) must not be
// treated the same as a permanently stale reference — the peer could
// genuinely have already received it. Validate must distinguish this
// transient case (ErrDeliveryPending) from a real stale/unknown envelope
// (ErrStaleReply).
func TestValidateDispatchingIsPendingNotStale(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	ctrl := controller.New(db)
	conversation := "conv-mid-dispatch"
	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.Bidirectional,
		MaxExchanges: 10,
	}); err != nil {
		t.Fatalf("grant: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		t.Fatalf("current grant: %v", err)
	}
	e := store.Envelope{
		ID:           "env-mid-dispatch",
		Conversation: conversation,
		FromPeer:     "claude-session-a",
		ToPeer:       "codex-thread-b",
		Text:         "please respond",
		GrantVersion: g.GrantVersion,
		State:        store.Queued,
		CreatedAt:    "2026-01-01T00:00:00Z",
		UpdatedAt:    "2026-01-01T00:00:00Z",
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatalf("insert queued: %v", err)
	}
	if err := store.SetState(ctx, tx, e.ID, store.Queued, store.Dispatching, "2026-01-01T00:00:01Z"); err != nil {
		t.Fatalf("set dispatching: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx2.Rollback()
	m := &replymarker.Marker{InReplyTo: e.ID, To: "claude-session-a", Text: "hi"}
	_, err = replymarker.Validate(ctx, tx2, conversation, "codex-thread-b", "claude-session-a", m)
	if !errors.Is(err, replymarker.ErrDeliveryPending) {
		t.Fatalf("want ErrDeliveryPending for a still-dispatching envelope, got %v", err)
	}
	if errors.Is(err, replymarker.ErrStaleReply) {
		t.Fatalf("a still-dispatching envelope must not also be classified as permanently stale")
	}
}

// Fixture 10: stale reply — a well-formed marker whose in_reply_to names an
// envelope that isn't currently handed off and awaiting reply on that
// conversation must be rejected, not forwarded.
func TestValidateStaleReply(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()

	t.Run("unknown envelope", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback()
		m := &replymarker.Marker{InReplyTo: "does-not-exist", To: "claude-session-a", Text: "hi"}
		if _, err := replymarker.Validate(ctx, tx, conversation, "codex-thread-b", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
			t.Fatalf("want ErrStaleReply, got %v", err)
		}
	})

	t.Run("different conversation", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback()
		m := &replymarker.Marker{InReplyTo: id, To: "claude-session-a", Text: "hi"}
		if _, err := replymarker.Validate(ctx, tx, "a-different-conversation", "codex-thread-b", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
			t.Fatalf("want ErrStaleReply, got %v", err)
		}
	})

	t.Run("already acked", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := store.SetState(ctx, tx, id, store.HandedOff, store.Acked, "2026-01-01T00:01:00Z"); err != nil {
			tx.Rollback()
			t.Fatalf("set acked: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}

		tx2, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx2.Rollback()
		m := &replymarker.Marker{InReplyTo: id, To: "claude-session-a", Text: "hi"}
		if _, err := replymarker.Validate(ctx, tx2, conversation, "codex-thread-b", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
			t.Fatalf("want ErrStaleReply, got %v", err)
		}
	})
}

func TestValidateAccepted(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	m := &replymarker.Marker{InReplyTo: id, To: "claude-session-a", Text: "hi"}
	e, err := replymarker.Validate(ctx, tx, conversation, "codex-thread-b", "claude-session-a", m)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if e.ID != id {
		t.Fatalf("want envelope %s, got %s", id, e.ID)
	}
}

// Regression for a finding on PR #3: Validate must reject a marker
// referencing an envelope that was never addressed to the peer producing
// the reply — otherwise a peer could name an envelope it sent itself (or
// one sent to a different peer) as long as the conversation and state
// happened to match, forging a reply nobody asked it for.
func TestValidateRejectsReplyFromNonAddressee(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	// queueOne's envelope was sent from claude-session-a to codex-thread-b.
	// claude-session-a (the sender, not the addressee) must not be able to
	// reply to its own outgoing envelope.
	m := &replymarker.Marker{InReplyTo: id, To: "someone-else", Text: "hi"}
	_, err = replymarker.Validate(ctx, tx, conversation, "claude-session-a", "someone-else", m)
	if !errors.Is(err, replymarker.ErrWrongReplier) {
		t.Fatalf("want ErrWrongReplier, got %v", err)
	}
}

// Regression for finding 3974355741 on PR #3's third review round:
// stripCodeSpans used to delete a matched code span's delimiters and
// content outright, concatenating the literal text immediately before and
// after it. Those two fragments were never adjacent in the source — here,
// literal "<!" then an inline code span "`x`" then literal "--" — so
// closing the gap between them could synthesize a comment delimiter
// ("<!--") that was never actually present, hiding a following valid
// marker as if it were inside an HTML comment.
func TestExtractFindsMarkerAfterCodeSpanSplitCommentLookalike(t *testing.T) {
	turn := "<!`x`--\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want the marker to be found (no real HTML comment was opened), got %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text hi, got %q", m.Text)
	}
}

// Regression for finding 3974355748 on PR #3's third review round:
// CommonMark's type 7 (a generic complete tag alone on a line) cannot
// interrupt an open paragraph — only types 1-6 and fenced code blocks can.
// A custom tag immediately following ordinary paragraph text, with no blank
// line between them, must stay live paragraph text; a following reply fence
// still interrupts the paragraph and must be found, not hidden as if the
// custom tag had opened a blank-line-terminated raw HTML block.
func TestExtractFindsMarkerAfterCustomTagCannotInterruptParagraph(t *testing.T) {
	turn := "some ordinary paragraph text\n<custom>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want the marker to be found (type 7 cannot interrupt the open paragraph), got %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text hi, got %q", m.Text)
	}
}

// Regression for finding 3974355752 on PR #3's third review round: a
// self-contained type-1 block (script/pre/style/textarea opened and closed
// on the same line) must be recognized as that specific raw HTML block type
// even when its content contains a literal comment-opener-like substring
// ("<!--"). The previous ordering checked for an HTML comment first, so a
// line like `<script>const s = "<!--";</script>` was misread as opening an
// unterminated comment, hiding every following line — including a valid
// reply fence — until an unrelated "-->" happened to appear.
func TestExtractFindsMarkerAfterSelfContainedScriptBlockContainingCommentLookalike(t *testing.T) {
	turn := `<script>const s = "<!--";</script>` + "\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want the marker to be found (the script block closes on its own line), got %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text hi, got %q", m.Text)
	}
}

// Regression for a finding on PR #3's fourth review round: <source> is part
// of CommonMark's fixed type-6 block-level tag list, missing from
// blockLevelTags. A <source> block hides its content (including a following
// marker) until the next blank line, same as any other type-6 tag.
func TestExtractIgnoresMarkerInsideSourceBlock(t *testing.T) {
	turn := "<source>\nhidden\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside a <source> block, got %v", err)
	}
}

func TestExtractFindsMarkerAfterBlankLineEndsSourceBlock(t *testing.T) {
	turn := "<source>\nhidden\n\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after the blank line closing the <source> block, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// Regression for a finding on PR #3's fourth review round: scanForMarker's
// paragraphOpen fallback previously marked every unrecognized nonblank line
// as opening a paragraph, including headings, thematic breaks and indented
// code — none of which are CommonMark paragraphs. That wrongly gated a
// following type-7 tag as "cannot interrupt a paragraph" (skipping the
// raw-HTML-block check), leaving it as ordinary text and exposing a marker
// that should instead have stayed hidden inside the type-7 block until a
// blank line, exactly as it would with nothing before the tag at all.
func TestExtractIgnoresMarkerInsideCustomTagAfterHeading(t *testing.T) {
	turn := "# heading\n<custom>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker inside a type-7 block opened right after a heading (not a paragraph), got %v", err)
	}
}

func TestExtractIgnoresMarkerInsideCustomTagAfterThematicBreak(t *testing.T) {
	turn := "---\n<custom>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker inside a type-7 block opened right after a thematic break (not a paragraph), got %v", err)
	}
}

func TestExtractFindsMarkerAfterBlankLineEndsCustomTagAfterHeading(t *testing.T) {
	turn := "# heading\n<custom>\nhidden\n\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after the blank line closing the custom-tag block, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// A type-7 tag immediately after genuine paragraph text is the unchanged
// case (TestExtractFindsMarkerAfterCustomTagCannotInterruptParagraph above):
// it still cannot interrupt the paragraph, unlike the heading/thematic-break
// cases just added.
func TestExtractFindsMarkerAfterCustomTagCannotInterruptIndentedCodeContinuation(t *testing.T) {
	turn := "some paragraph text\n    still part of the paragraph (lazy continuation)\n<custom>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want the marker to be found (the indented line is a lazy paragraph continuation, still an open paragraph), got %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text hi, got %q", m.Text)
	}
}

// Regression for a finding on PR #3's fifth review round: <search> is part
// of CommonMark's fixed type-6 block-level tag list, missing from
// blockLevelTags even after the earlier <source> fix. Same pattern as
// TestExtractIgnoresMarkerInsideSourceBlock, for the other missing tag.
func TestExtractIgnoresMarkerInsideSearchBlock(t *testing.T) {
	turn := "<search>\nhidden\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside a <search> block, got %v", err)
	}
}

func TestExtractFindsMarkerAfterBlankLineEndsSearchBlock(t *testing.T) {
	turn := "<search>\nhidden\n\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want a live marker after the blank line closing the <search> block, got err %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text %q, got %q", "hi", m.Text)
	}
}

// Regression for a finding on PR #3's fifth review round: CommonMark's type-4
// declaration start condition requires an uppercase ASCII letter after "<!"
// specifically — a lowercase "<!foo" never opens a type-4 block, and must
// stay ordinary text, letting a following marker on the very next line (no
// blank line needed, since it was never a raw HTML block at all) be found.
func TestExtractFindsMarkerAfterLowercaseDeclarationLookalike(t *testing.T) {
	turn := "<!foo\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("want the marker to be found (\"<!foo\" is not a type-4 declaration opener, lowercase doesn't qualify), got %v", err)
	}
	if m.Text != "hi" {
		t.Fatalf("want text hi, got %q", m.Text)
	}
}

// An uppercase declaration opener still behaves as type 4, ending only at
// its own ">" — unchanged behavior, asserted here as the companion case to
// the lowercase fix above.
func TestExtractIgnoresMarkerInsideUppercaseDeclaration(t *testing.T) {
	turn := "<!FOO\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker inside an unterminated type-4 declaration, got %v", err)
	}
}

// Regression for a finding on PR #3's fifth review round: a Setext heading's
// "=" underline (e.g. "title\n===") closes the paragraph it turns into a
// heading — the underline itself is never a paragraph, so a following type-7
// tag must still be gated as "cannot interrupt a paragraph" only by the text
// line before the underline, not by the underline line itself remaining
// "open". A following reply fence must therefore be found: the custom tag
// opens a fresh type-7 block (no paragraph precedes it), hiding its own
// content but not touching the marker on the line after it.
func TestExtractIgnoresMarkerInsideCustomTagAfterSetextHeading(t *testing.T) {
	turn := "title\n===\n<custom>\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker inside a type-7 block opened right after a Setext heading underline (not a paragraph), got %v", err)
	}
}

func TestExtractIgnoresMarkerInHTMLAfterShortSetextHeading(t *testing.T) {
	for _, underline := range []string{"-", "--"} {
		turn := "Heading\n" + underline + "\n<custom>\n```BRIDGE-REPLY\n{\"in_reply_to\":\"env-1\",\"to\":\"claude-session-a\",\"text\":\"hidden\"}\n```\n</custom>"
		if marker, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
			t.Fatalf("underline=%q marker=%+v error=%v", underline, marker, err)
		}
	}
}

func TestExtractRequiresTopLevelColumnZeroFence(t *testing.T) {
	marker := "```BRIDGE-REPLY\n" + `{"in_reply_to":"env-1","to":"claude-session-a","text":"hi"}` + "\n```"
	for _, prefix := range []string{"> ", "  ", "    "} {
		wrapped := prefix + strings.ReplaceAll(marker, "\n", "\n"+prefix)
		if _, err := replymarker.Extract(wrapped); !errors.Is(err, replymarker.ErrNoMarker) {
			t.Fatalf("prefix=%q error=%v", prefix, err)
		}
	}
	nested := "- item\n\n  " + strings.ReplaceAll(marker, "\n", "\n  ")
	if _, err := replymarker.Extract(nested); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("list marker: %v", err)
	}
}

func FuzzExtractQuotedContentNeverBecomesReply(f *testing.F) {
	f.Add("```BRIDGE-REPLY\n{}\n```")
	f.Add("Heading\n-\n<custom>\n```BRIDGE-REPLY\n{}\n```")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 65536 {
			t.Skip()
		}
		input = strings.ReplaceAll(input, "\r", "\n")
		quoted := "> " + strings.ReplaceAll(input, "\n", "\n> ")
		if marker, err := replymarker.Extract(quoted); !errors.Is(err, replymarker.ErrNoMarker) {
			t.Fatalf("quoted content extracted: %+v %v", marker, err)
		}
	})
}

func TestMalformedReservedHeadingCannotHideBeforeValidMarker(t *testing.T) {
	turn := "```BRIDGE-REPLY`\n---\n\n```BRIDGE-REPLY\n" + `{"in_reply_to":"env-1","to":"claude-session-a","text":"hi"}` + "\n```"
	if marker, err := replymarker.Extract(turn); err == nil {
		t.Fatalf("malformed reserved heading ignored: %+v", marker)
	}
}

func TestIndentedMalformedReservedPrefixRemainsIneligible(t *testing.T) {
	prefix := "  ```BRIDGE-REPLY`\n\n"
	if _, err := replymarker.Extract(prefix); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("indented prefix: %v", err)
	}
	turn := prefix + "```BRIDGE-REPLY\n" + `{"in_reply_to":"env-1","to":"claude-session-a","text":"hi"}` + "\n```"
	if marker, err := replymarker.Extract(turn); err != nil || marker.Text != "hi" {
		t.Fatalf("marker=%+v error=%v", marker, err)
	}
}
