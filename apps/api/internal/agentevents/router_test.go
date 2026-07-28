package agentevents

import (
	"errors"
	"testing"
)

func TestNewProductionMux_NilStoreReturnsError(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open command store: %v", err)
	}
	defer store.Close()
	gateway, err := OpenDurableGateway(t.TempDir(), store)
	if err != nil {
		t.Fatalf("open durable gateway: %v", err)
	}

	_, _, _, err = NewProductionMux(nil, gateway, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("expected NewProductionMux to return an error for nil store")
	}
	if !errors.Is(err, errCommandAppenderRequired) {
		t.Fatalf("expected errCommandAppenderRequired, got %v", err)
	}
}
