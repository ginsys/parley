package control

import (
	"errors"
	"testing"

	"github.com/ginsys/parley/internal/store"
)

func TestDomainCodePassesThroughStoreCode(t *testing.T) {
	if got := domainCode(store.RecoveryRequired); got != DomainCode(store.RecoveryRequired) {
		t.Fatalf("got %q", got)
	}
}

func TestDomainCodePassesThroughDomainCode(t *testing.T) {
	if got := domainCode(ProtocolMismatch); got != ProtocolMismatch {
		t.Fatalf("got %q", got)
	}
	if got := domainCode(OperationNotFound); got != OperationNotFound {
		t.Fatalf("got %q", got)
	}
}

func TestDomainCodeDefaultsUnknownErrorToTemporarilyUnavailable(t *testing.T) {
	if got := domainCode(errors.New("some unwrapped internal failure")); got != DomainCode(store.TemporarilyUnavailable) {
		t.Fatalf("got %q", got)
	}
}

func TestDomainCodeWrappedStoreCode(t *testing.T) {
	wrapped := errors.Join(errors.New("context"), store.Forbidden)
	if got := domainCode(wrapped); got != DomainCode(store.Forbidden) {
		t.Fatalf("got %q", got)
	}
}
