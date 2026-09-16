package control

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/google/uuid"
)

// Config is the server-side control endpoint configuration, immutable for
// the server's lifetime once constructed by NewConfig.
type Config struct {
	AdminSocket string // absolute path to the Unix listener
	ServerUID   uint32 // UID the server itself runs as

	// administrators maps each administrator's canonical UUID to the UID of
	// the one account permitted to connect as that administrator. Private:
	// use Administrators() for a defensive copy. NewConfig guarantees every
	// key is a canonical UUID and every value is unique.
	administrators map[string]uint32
}

var (
	errRelativeSocketPath = errors.New("control: admin_socket must be an absolute, clean path")
	errNoAdministrators   = errors.New("control: at least one administrator is required")
	errDuplicateAdminUID  = errors.New("control: a UID cannot be shared by two administrators")
	errInvalidAdminID     = errors.New("control: administrator ID must be a canonical UUID")
	errInvalidUID         = errors.New("control: UID must be a platform UID, not the reserved (uid_t)-1 sentinel")
)

// reservedUID is the POSIX (uid_t)-1 sentinel meaning "no such user" / "do
// not change" -- never a legitimate connecting or serving account. UID 0
// (root) is a legitimately supported administrator/server identity and is
// deliberately not rejected here (docs/specifications/control.md: "UIDs
// are JSON integers in the platform UID range, rejecting reserved/
// unmapped identities").
const reservedUID = ^uint32(0)

func validUID(uid uint32) bool { return uid != reservedUID }

// ParseUID parses text as a decimal UID, rejecting anything that does not
// fit a 32-bit platform UID (negative, non-numeric, empty, >= 2^32) via
// strconv's own bitSize=32 bound, and the reserved sentinel above -- one
// validator reused by every UID-bearing flag/environment/config input
// across parleyd and parleyctl (administrator UIDs, the server's own UID,
// and the client's resolved server UID) so a single place governs the
// whole class instead of each caller narrowing an unbounded value on its
// own (mandate R2).
func ParseUID(text string) (uint32, error) {
	v, err := strconv.ParseUint(text, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("control: invalid UID %q: %w", text, err)
	}
	uid := uint32(v)
	if !validUID(uid) {
		return 0, fmt.Errorf("%w: %d", errInvalidUID, uid)
	}
	return uid, nil
}

// NewConfig validates and returns an immutable server Config. SO_PEERCRED
// can only prove a connecting UID, so a UID shared by two administrator
// entries would make them indistinguishable at authentication time; that
// is rejected here rather than left as a runtime ambiguity.
func NewConfig(adminSocket string, serverUID uint32, administrators map[string]uint32) (Config, error) {
	if !filepath.IsAbs(adminSocket) || filepath.Clean(adminSocket) != adminSocket {
		return Config{}, fmt.Errorf("%w: %q", errRelativeSocketPath, adminSocket)
	}
	if len(administrators) == 0 {
		return Config{}, errNoAdministrators
	}
	if !validUID(serverUID) {
		return Config{}, fmt.Errorf("%w: %d", errInvalidUID, serverUID)
	}
	seenUID := make(map[uint32]string, len(administrators))
	copied := make(map[string]uint32, len(administrators))
	for id, uid := range administrators {
		if !validAdminID(id) {
			return Config{}, fmt.Errorf("%w: %q", errInvalidAdminID, id)
		}
		if !validUID(uid) {
			return Config{}, fmt.Errorf("%w: %d", errInvalidUID, uid)
		}
		if other, dup := seenUID[uid]; dup {
			return Config{}, fmt.Errorf("%w: uid %d claimed by both %q and %q", errDuplicateAdminUID, uid, other, id)
		}
		seenUID[uid] = id
		copied[id] = uid
	}
	return Config{AdminSocket: adminSocket, ServerUID: serverUID, administrators: copied}, nil
}

// Administrators returns a defensive copy of the administrator UUID->UID
// map; callers cannot mutate the Config's own copy through it.
func (c Config) Administrators() map[string]uint32 {
	copied := make(map[string]uint32, len(c.administrators))
	for id, uid := range c.administrators {
		copied[id] = uid
	}
	return copied
}

func validAdminID(s string) bool { return canonicalUUID(s) }

// canonicalUUID reports whether s is exactly the lowercase, hyphenated
// canonical rendering of a non-nil UUID -- the wire profile's identifier
// form (see docs/specifications/control.md's UUID/exact-key rules).
func canonicalUUID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id != uuid.Nil && id.String() == s
}

// ClientConfig is the resolved client-side connection target.
type ClientConfig struct {
	Endpoint  string
	ServerUID uint32
}

// ClientOptions carries the raw, unresolved client inputs precedence is
// applied over. Getenv is injectable for tests; production callers leave
// it nil and get os.Getenv.
type ClientOptions struct {
	EndpointFlag  string
	ServerUIDFlag string
	Getenv        func(string) string
}

var errClientDBRefused = errors.New("control: PARLEY_DB is not a client configuration source; the client never opens a database directly")

// ResolveClientConfig applies the client's endpoint/server-UID precedence:
// an explicit flag, then the PARLEY_ENDPOINT/PARLEY_SERVER_UID environment
// variables, then a trusted client configuration file. The trusted client
// configuration file is not implemented in PR1: an endpoint or UID left
// unresolved by flag or environment is reported as missing rather than
// invented a format for. PARLEY_DB is refused outright wherever it is set:
// it is a server-only variable, and its presence in a client's environment
// is a misconfiguration, never a fallback source.
func ResolveClientConfig(opts ClientOptions) (ClientConfig, error) {
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	if getenv("PARLEY_DB") != "" {
		return ClientConfig{}, errClientDBRefused
	}
	endpoint := opts.EndpointFlag
	if endpoint == "" {
		endpoint = getenv("PARLEY_ENDPOINT")
	}
	if endpoint == "" {
		return ClientConfig{}, errors.New("control: no endpoint configured; use --endpoint or PARLEY_ENDPOINT (a trusted client configuration file is not implemented yet)")
	}
	if !filepath.IsAbs(endpoint) || filepath.Clean(endpoint) != endpoint {
		return ClientConfig{}, fmt.Errorf("control: endpoint must be an absolute, clean path: %q", endpoint)
	}
	uidStr := opts.ServerUIDFlag
	if uidStr == "" {
		uidStr = getenv("PARLEY_SERVER_UID")
	}
	if uidStr == "" {
		return ClientConfig{}, errors.New("control: no server UID configured; use --server-uid or PARLEY_SERVER_UID (a trusted client configuration file is not implemented yet)")
	}
	uid, err := ParseUID(uidStr)
	if err != nil {
		return ClientConfig{}, err
	}
	return ClientConfig{Endpoint: endpoint, ServerUID: uid}, nil
}
