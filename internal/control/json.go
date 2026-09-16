// Package control implements the parley-control/1 human administration
// protocol: configuration, wire framing/profile, the Linux Unix-socket
// listener and the server.hello/operation.get methods wired for PR1. See
// docs/specifications/control.md for the accepted contract; this package
// does not claim a complete or production-ready endpoint.
package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// maxDepth is the profile's nesting bound: 32 levels of object/array nesting.
const maxDepth = 32

// errParse marks a raw frame that is not valid JSON, or that violates a
// structural rule (duplicate keys, unpaired surrogate escape, excess depth,
// invalid UTF-8) a lenient decoder would otherwise silently accept. Both
// classes map to the JSON-RPC -32700 parse error; the caller does not need
// to distinguish them further.
var errParse = errors.New("control: invalid json")

// parseJSON decodes exactly one JSON value from data, rejecting anything a
// standard-library round trip would accept but the profile forbids:
// duplicate object keys (encoding/json keeps only the last), nesting past
// maxDepth, invalid UTF-8, unpaired \u surrogate escapes (encoding/json
// silently substitutes U+FFFD for these instead of erroring), and trailing
// bytes after the value. It does not by itself enforce the envelope shape;
// see classifyEnvelope for that.
//
// The returned value uses: map[string]any for objects (all keys unique),
// []any for arrays, json.Number for numbers (preserving exact source
// digits for the decimal-string codec rules), string, bool, and nil.
func parseJSON(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, errParse
	}
	if err := scanSurrogates(data); err != nil {
		return nil, errParse
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	value, err := decodeValue(dec, 0)
	if err != nil {
		return nil, errParse
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errParse // trailing content after the single top-level value
	}
	return value, nil
}

func decodeValue(dec *json.Decoder, depth int) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			return decodeObject(dec, depth+1)
		case '[':
			return decodeArray(dec, depth+1)
		default:
			// '}' or ']' cannot appear where a value is expected.
			return nil, errParse
		}
	case string, bool, json.Number, nil:
		return tok, nil
	default:
		return nil, errParse
	}
}

func decodeObject(dec *json.Decoder, depth int) (map[string]any, error) {
	if depth > maxDepth {
		return nil, errParse
	}
	result := map[string]any{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, errParse
		}
		if _, dup := result[key]; dup {
			return nil, errParse
		}
		value, err := decodeValue(dec, depth)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	// Consume the closing '}'.
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeArray(dec *json.Decoder, depth int) ([]any, error) {
	if depth > maxDepth {
		return nil, errParse
	}
	var result []any
	for dec.More() {
		value, err := decodeValue(dec, depth)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return result, nil
}

// scanSurrogates rejects a lone (unpaired) UTF-16 surrogate escape anywhere
// in the raw frame. encoding/json accepts these and silently substitutes
// U+FFFD; the profile requires rejecting the frame instead. Escapes only
// have meaning inside JSON string literals, and a backslash is never valid
// JSON outside one, so a linear byte scan is sufficient: any input where
// this scan would misfire is already invalid JSON for an unrelated reason,
// which parseJSON's subsequent decode step still catches.
func scanSurrogates(data []byte) error {
	i := 0
	for i < len(data) {
		if data[i] != '\\' {
			i++
			continue
		}
		if i+1 >= len(data) {
			return nil // truncated escape; let the JSON decoder reject it
		}
		switch data[i+1] {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			i += 2
		case 'u':
			unit, ok := hex4(data, i+2)
			if !ok {
				i += 2 // malformed hex; let the JSON decoder reject it
				continue
			}
			switch {
			case unit >= 0xD800 && unit <= 0xDBFF: // high surrogate
				if i+8 > len(data) || data[i+6] != '\\' || data[i+7] != 'u' {
					return fmt.Errorf("%w: unpaired high surrogate", errParse)
				}
				low, ok := hex4(data, i+8)
				if !ok || low < 0xDC00 || low > 0xDFFF {
					return fmt.Errorf("%w: invalid surrogate pair", errParse)
				}
				i += 12
			case unit >= 0xDC00 && unit <= 0xDFFF: // low surrogate, unpaired
				return fmt.Errorf("%w: unpaired low surrogate", errParse)
			default:
				i += 6
			}
		default:
			i += 2 // invalid escape char; let the JSON decoder reject it
		}
	}
	return nil
}

func hex4(data []byte, at int) (rune, bool) {
	if at+4 > len(data) {
		return 0, false
	}
	var v rune
	for _, c := range data[at : at+4] {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			v |= rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v |= rune(c-'A') + 10
		default:
			return 0, false
		}
	}
	return v, true
}
