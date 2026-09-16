package control

import (
	"errors"
	"testing"
)

const adminA = "3f6e6c8e-8c1b-4a3a-9c2e-2f6b6e6c0a01"
const adminB = "3f6e6c8e-8c1b-4a3a-9c2e-2f6b6e6c0a02"

func TestNewConfigAcceptsValidInput(t *testing.T) {
	cfg, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{adminA: 1001, adminB: 1002})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AdminSocket != "/run/parley/admin.sock" || cfg.ServerUID != 1000 {
		t.Fatalf("%#v", cfg)
	}
	if got := cfg.Administrators(); len(got) != 2 || got[adminA] != 1001 || got[adminB] != 1002 {
		t.Fatalf("%#v", got)
	}
}

func TestNewConfigRejectsRelativeSocketPath(t *testing.T) {
	_, err := NewConfig("relative/admin.sock", 1000, map[string]uint32{adminA: 1001})
	if !errors.Is(err, errRelativeSocketPath) {
		t.Fatalf("got %v", err)
	}
}

func TestNewConfigRejectsUncleanSocketPath(t *testing.T) {
	_, err := NewConfig("/run/parley/../admin.sock", 1000, map[string]uint32{adminA: 1001})
	if !errors.Is(err, errRelativeSocketPath) {
		t.Fatalf("got %v", err)
	}
}

func TestNewConfigRejectsEmptyAdministrators(t *testing.T) {
	_, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{})
	if !errors.Is(err, errNoAdministrators) {
		t.Fatalf("got %v", err)
	}
}

func TestNewConfigRejectsDuplicateUID(t *testing.T) {
	_, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{adminA: 1001, adminB: 1001})
	if !errors.Is(err, errDuplicateAdminUID) {
		t.Fatalf("got %v", err)
	}
}

func TestNewConfigRejectsInvalidAdminID(t *testing.T) {
	for _, id := range []string{"not-a-uuid", "00000000-0000-0000-0000-000000000000", "3F6E6C8E-8C1B-4A3A-9C2E-2F6B6E6C0A01"} {
		_, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{id: 1001})
		if !errors.Is(err, errInvalidAdminID) {
			t.Fatalf("id %q: got %v", id, err)
		}
	}
}

func TestConfigAdministratorsIsADefensiveCopy(t *testing.T) {
	cfg, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{adminA: 1001})
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.Administrators()
	got[adminA] = 9999
	if cfg.Administrators()[adminA] != 1001 {
		t.Fatal("mutating the returned map affected the Config")
	}
}

func TestConfigAdministratorsUnaffectedByCallerMapMutation(t *testing.T) {
	input := map[string]uint32{adminA: 1001}
	cfg, err := NewConfig("/run/parley/admin.sock", 1000, input)
	if err != nil {
		t.Fatal(err)
	}
	input[adminA] = 9999
	if cfg.Administrators()[adminA] != 1001 {
		t.Fatal("mutating the caller's map after construction affected the Config")
	}
}

func fakeGetenv(values map[string]string) func(string) string {
	return func(k string) string { return values[k] }
}

func TestResolveClientConfigFlagTakesPrecedenceOverEnv(t *testing.T) {
	cfg, err := ResolveClientConfig(ClientOptions{
		EndpointFlag:  "/run/parley/admin.sock",
		ServerUIDFlag: "1000",
		Getenv:        fakeGetenv(map[string]string{"PARLEY_ENDPOINT": "/other.sock", "PARLEY_SERVER_UID": "2000"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint != "/run/parley/admin.sock" || cfg.ServerUID != 1000 {
		t.Fatalf("%#v", cfg)
	}
}

func TestResolveClientConfigFallsBackToEnv(t *testing.T) {
	cfg, err := ResolveClientConfig(ClientOptions{
		Getenv: fakeGetenv(map[string]string{"PARLEY_ENDPOINT": "/run/parley/admin.sock", "PARLEY_SERVER_UID": "1000"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint != "/run/parley/admin.sock" || cfg.ServerUID != 1000 {
		t.Fatalf("%#v", cfg)
	}
}

func TestResolveClientConfigMissingEndpointIsAnError(t *testing.T) {
	_, err := ResolveClientConfig(ClientOptions{Getenv: fakeGetenv(nil)})
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestResolveClientConfigMissingServerUIDIsAnError(t *testing.T) {
	_, err := ResolveClientConfig(ClientOptions{
		EndpointFlag: "/run/parley/admin.sock",
		Getenv:       fakeGetenv(nil),
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestResolveClientConfigRejectsPARLEY_DB(t *testing.T) {
	_, err := ResolveClientConfig(ClientOptions{
		EndpointFlag:  "/run/parley/admin.sock",
		ServerUIDFlag: "1000",
		Getenv:        fakeGetenv(map[string]string{"PARLEY_DB": "/some/db.sqlite"}),
	})
	if !errors.Is(err, errClientDBRefused) {
		t.Fatalf("got %v", err)
	}
}

func TestResolveClientConfigRejectsRelativeEndpoint(t *testing.T) {
	_, err := ResolveClientConfig(ClientOptions{
		EndpointFlag:  "relative.sock",
		ServerUIDFlag: "1000",
		Getenv:        fakeGetenv(nil),
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestResolveClientConfigRejectsInvalidServerUID(t *testing.T) {
	_, err := ResolveClientConfig(ClientOptions{
		EndpointFlag:  "/run/parley/admin.sock",
		ServerUIDFlag: "not-a-number",
		Getenv:        fakeGetenv(nil),
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}

// TestParseUIDBoundaries is mandate R2's regression matrix for the shared
// UID validator: the exact supported boundary, the reserved sentinel,
// 2^32, 2^32+a valid-looking UID, a very large value, and malformed input
// must all be handled correctly -- none of this narrows through an
// unchecked conversion the way fs.Uint's uint32(*serverUID) once did.
func TestParseUIDBoundaries(t *testing.T) {
	valid := []struct {
		text string
		want uint32
	}{
		{"0", 0},                   // root: a legitimately supported identity, not rejected
		{"1000", 1000},             // an ordinary synthetic/current UID
		{"4294967294", 4294967294}, // the supported upper boundary (2^32-2)
	}
	for _, tc := range valid {
		got, err := ParseUID(tc.text)
		if err != nil || got != tc.want {
			t.Fatalf("ParseUID(%q) = %d, %v; want %d, nil", tc.text, got, err, tc.want)
		}
	}
	invalid := []string{
		"4294967295",           // reserved (uid_t)-1 sentinel
		"4294967296",           // 2^32: the exact silent-narrowing overflow
		"4294967296000000001",  // 2^32 + a valid-looking UID, still overflow
		"18446744073709551615", // a very large value (max uint64)
		"-1",                   // negative
		"not-a-number",         // malformed
		"",                     // empty
		"1.5",                  // non-integer
	}
	for _, text := range invalid {
		if _, err := ParseUID(text); err == nil {
			t.Fatalf("ParseUID(%q): expected rejection", text)
		}
	}
}

// TestNewConfigValidatesServerUID sweeps the same UID class onto the
// server's own UID field (mandate R2), not just administrator entries.
func TestNewConfigValidatesServerUID(t *testing.T) {
	if _, err := NewConfig("/run/parley/admin.sock", ^uint32(0), map[string]uint32{adminA: 1001}); !errors.Is(err, errInvalidUID) {
		t.Fatalf("got %v", err)
	}
	// UID 0 (root) must remain accepted as the server's own UID.
	if _, err := NewConfig("/run/parley/admin.sock", 0, map[string]uint32{adminA: 1001}); err != nil {
		t.Fatalf("unexpected rejection of server UID 0: %v", err)
	}
}

// TestNewConfigRejectsReservedAdministratorUID sweeps the reserved
// sentinel onto an administrator entry, alongside the existing duplicate/
// invalid-ID coverage above.
func TestNewConfigRejectsReservedAdministratorUID(t *testing.T) {
	_, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{adminA: ^uint32(0)})
	if !errors.Is(err, errInvalidUID) {
		t.Fatalf("got %v", err)
	}
}

// TestResolveClientConfigRejectsOverflowServerUID exercises R2's sweep
// onto the client's resolved server UID: an overflowing or reserved value
// must be rejected as a configuration error before ClientConfig is ever
// returned, never silently narrowed.
func TestResolveClientConfigRejectsOverflowServerUID(t *testing.T) {
	for _, bad := range []string{"4294967296", "4294967295"} {
		_, err := ResolveClientConfig(ClientOptions{
			EndpointFlag:  "/run/parley/admin.sock",
			ServerUIDFlag: bad,
			Getenv:        fakeGetenv(nil),
		})
		if err == nil {
			t.Fatalf("uid=%q: expected rejection", bad)
		}
	}
}
