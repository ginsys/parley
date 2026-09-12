package bridgetext_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ginsys/parley/internal/bridgetext"
)

func TestIdentifierByteAlphabet(t *testing.T) {
	for b := 0; b <= 255; b++ {
		t.Run(fmt.Sprintf("%02x", b), func(t *testing.T) {
			value := " a" + string([]byte{byte(b)}) + " "
			want := b >= 0x20 && b <= 0x7e
			if got := bridgetext.ValidateMetadata(value) == nil; got != want {
				t.Fatalf("identifier %x accepted=%v, want %v", value, got, want)
			}
			if want {
				wrapped, err := bridgetext.Wrap("id", value, "Unicode payload: 世界\n\x00")
				if err != nil || !strings.Contains(wrapped, "[Parley message from "+value+"]") || !strings.Contains(wrapped, "世界\n\x00") {
					t.Fatalf("accepted bytes or payload changed: %q, %v", wrapped, err)
				}
			}
		})
	}
	for _, value := range []string{"", " ", "    ", "café", "世界", "a\ufffd", "a\xff", "a\xfe"} {
		if bridgetext.ValidateMetadata(value) == nil {
			t.Errorf("accepted incompatible identifier %x", value)
		}
		if _, err := bridgetext.Wrap("id", value, "payload"); err == nil {
			t.Errorf("wrapped incompatible sender %x", value)
		}
	}
}

func TestMalformedKeysCannotReachLossyJSONSerialization(t *testing.T) {
	a, b := "a\xff", "a\xfe"
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if a == b || string(ja) != string(jb) {
		t.Fatal("fixture must reproduce distinct keys collapsing in JSON")
	}
	for _, key := range []string{a, b} {
		if bridgetext.ValidateMetadata(key) == nil {
			t.Errorf("lossy JSON key %x passed validation", key)
		}
	}
}
