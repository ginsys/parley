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
