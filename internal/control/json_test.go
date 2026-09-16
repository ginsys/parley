package control

import (
	"encoding/json"
	"testing"
)

func TestParseJSONAcceptsOrdinaryValues(t *testing.T) {
	v, err := parseJSON([]byte(`{"a":1,"b":[true,false,null,"x"],"c":{"d":1.5}}`))
	if err != nil {
		t.Fatal(err)
	}
	obj, ok := v.(map[string]any)
	if !ok || obj["a"].(json.Number).String() != "1" {
		t.Fatalf("%#v", v)
	}
	arr, ok := obj["b"].([]any)
	if !ok || len(arr) != 4 {
		t.Fatalf("%#v", obj["b"])
	}
}

func TestParseJSONRejectsDuplicateKeysAtAnyLevel(t *testing.T) {
	for _, input := range []string{
		`{"a":1,"a":2}`,
		`{"outer":{"a":1,"a":2}}`,
		`[{"a":1,"a":2}]`,
	} {
		if _, err := parseJSON([]byte(input)); err == nil {
			t.Fatalf("accepted duplicate key: %s", input)
		}
	}
}

func TestParseJSONRejectsExcessDepth(t *testing.T) {
	open, close := "", ""
	for range maxDepth + 1 {
		open += "["
		close += "]"
	}
	if _, err := parseJSON([]byte(open + "1" + close)); err == nil {
		t.Fatal("accepted depth beyond the bound")
	}
	// Exactly maxDepth nested arrays (the outermost array is depth 1) must
	// still be accepted.
	open, close = "", ""
	for range maxDepth {
		open += "["
		close += "]"
	}
	if _, err := parseJSON([]byte(open + "1" + close)); err != nil {
		t.Fatalf("rejected depth at the bound: %v", err)
	}
}

func TestParseJSONRejectsTrailingContent(t *testing.T) {
	if _, err := parseJSON([]byte(`{"a":1} {"b":2}`)); err == nil {
		t.Fatal("accepted trailing content after the value")
	}
	if _, err := parseJSON([]byte(`{"a":1}garbage`)); err == nil {
		t.Fatal("accepted trailing garbage")
	}
}

func TestParseJSONRejectsInvalidUTF8(t *testing.T) {
	if _, err := parseJSON([]byte("{\"a\":\"\xff\"}")); err == nil {
		t.Fatal("accepted invalid UTF-8")
	}
}

func TestParseJSONRejectsMalformedSyntax(t *testing.T) {
	for _, input := range []string{`{`, `{"a":}`, `NaN`, `Infinity`, `{'a':1}`, ``} {
		if _, err := parseJSON([]byte(input)); err == nil {
			t.Fatalf("accepted malformed input: %q", input)
		}
	}
}

func TestScanSurrogatesPairing(t *testing.T) {
	cases := []struct {
		name  string
		input string
		valid bool
	}{
		{"valid pair", `"𐀀"`, true},
		{"ordinary escape", `"𐀀 extra \n text"`, true},
		{"high without low", `"\uD800"`, false},
		{"high followed by ordinary char", `"\uD800x"`, false},
		{"low without high", `"\uDC00"`, false},
		{"escaped backslash before u is not an escape", `"\\uD800"`, true}, // literal backslash + "uD800"
		{"non-surrogate unicode escape", `"A"`, true},
		{"two highs in a row", `"\uD800𐀀"`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := scanSurrogates([]byte(tc.input))
			if tc.valid && err != nil {
				t.Fatalf("unexpected rejection: %v", err)
			}
			if !tc.valid && err == nil {
				t.Fatal("expected rejection, got none")
			}
		})
	}
}

func TestParseJSONRejectsTopLevelUnpairedSurrogate(t *testing.T) {
	if _, err := parseJSON([]byte(`{"id":"\uD800"}`)); err == nil {
		t.Fatal("accepted an unpaired surrogate encoding/json would silently replace")
	}
}
