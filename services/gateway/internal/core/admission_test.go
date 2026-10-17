package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

func TestLocalAdmissionBoundsEndpointWork(t *testing.T) {
	a := newLocalAdmission()
	release, err := a.acquire(context.Background(), "model", "ep", 2, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.acquire(context.Background(), "model", "ep", 2, 1, 0, 0); !errors.Is(err, gwerr.ErrOverloaded) {
		t.Fatalf("expected endpoint overload, got %v", err)
	}
	release()
	release, err = a.acquire(context.Background(), "model", "ep", 2, 1, 0, 0)
	if err != nil {
		t.Fatalf("capacity was not released: %v", err)
	}
	release()
}

func TestLocalAdmissionWaitTimeout(t *testing.T) {
	a := newLocalAdmission()
	release, err := a.acquire(context.Background(), "model", "ep", 1, 1, 1, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = a.acquire(context.Background(), "model", "ep", 1, 1, 1, 10*time.Millisecond)
	if !errors.Is(err, gwerr.ErrOverloaded) {
		t.Fatalf("expected bounded wait timeout, got %v", err)
	}
}
