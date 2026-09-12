package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"
)

// Fields describes an already schema-validated logical request, not wire JSON.
// Endpoint handlers must reject unknown fields/tags and resolve schema defaults
// before constructing it. No raw JSON/custom marshaler can enter the digest.
type Field struct {
	Name  string
	Value any
}
type Fields []Field

// Set marks only arrays whose order the operation contract declares immaterial.
// Ordinary []any arrays retain their order. Duplicate set entries are rejected.
type Set []any

type CommandRequest struct {
	kind, id string
	digest   [32]byte
}

// NewCommandRequest excludes correlation IDs and secrets. Field names reserved
// for authentication material are also rejected as a defense against accidents;
// operation handlers remain responsible for selecting only logical payload data.
func NewCommandRequest(kind, id string, fields ...Field) (CommandRequest, error) {
	if !validUUID(id) || len(kind) == 0 || len(kind) > 64 || strings.HasPrefix(kind, "rpc.") {
		return CommandRequest{}, InvalidRequest
	}
	for _, r := range kind {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_') {
			return CommandRequest{}, InvalidRequest
		}
	}
	budget := 1 << 20
	v, err := canonicalValue(Fields(fields), 0, &budget)
	if err != nil {
		return CommandRequest{}, err
	}
	data, err := json.Marshal([]any{kind, v})
	if err != nil || len(data) > 1<<20 {
		return CommandRequest{}, InvalidRequest
	}
	return CommandRequest{kind: kind, id: id, digest: sha256.Sum256(data)}, nil
}
func canonicalValue(v any, depth int, budget *int) (any, error) {
	*budget -= 1
	if depth > 32 || *budget < 0 {
		return nil, InvalidRequest
	}
	switch x := v.(type) {
	case nil, bool, int64, uint32:
		return x, nil
	case string:
		*budget -= len(x)
		if *budget < 0 || !utf8.ValidString(x) {
			return nil, InvalidRequest
		}
		return x, nil
	case Fields:
		if len(x) > *budget {
			return nil, InvalidRequest
		}
		obj := make(map[string]any, len(x))
		for _, f := range x {
			if f.Name == "" || !utf8.ValidString(f.Name) {
				return nil, InvalidRequest
			}
			switch f.Name {
			case "secret", "credential", "verifier", "correlation_id", "operation_id":
				return nil, InvalidRequest
			}
			if _, ok := obj[f.Name]; ok {
				return nil, InvalidRequest
			}
			*budget -= len(f.Name)
			child, err := canonicalValue(f.Value, depth+1, budget)
			if err != nil {
				return nil, err
			}
			obj[f.Name] = child
		}
		return obj, nil
	case []any:
		if len(x) > *budget {
			return nil, InvalidRequest
		}
		list := make([]any, len(x))
		for i, item := range x {
			child, err := canonicalValue(item, depth+1, budget)
			if err != nil {
				return nil, err
			}
			list[i] = child
		}
		return list, nil
	case Set:
		value, err := canonicalValue([]any(x), depth+1, budget)
		if err != nil {
			return nil, err
		}
		list := value.([]any)
		encoded := make([]json.RawMessage, len(list))
		for i, item := range list {
			encoded[i], err = json.Marshal(item)
			if err != nil {
				return nil, InvalidRequest
			}
		}
		sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i], encoded[j]) < 0 })
		for i := 1; i < len(encoded); i++ {
			if bytes.Equal(encoded[i-1], encoded[i]) {
				return nil, InvalidRequest
			}
		}
		return encoded, nil
	default:
		return nil, InvalidRequest
	}
}
