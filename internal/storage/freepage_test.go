package storage_test

import (
	"fmt"
	"testing"

	"go.store/internal/engine"
)

func TestFreePage(t *testing.T) {
	db, err := engine.Open("TestPageAllocation.db")
	if err != nil {
		t.Fatal(err)
	}

	const N = 10000

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%08d", i)
		if err := db.Set(k, []byte("x")); err != nil {
			t.Fatalf("Set %s failed: %v", k, err)
		}
	}

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%08d", i)
		if err := db.Delete(k); err != nil {
			t.Fatalf("Delete %s failed: %v", k, err)
		}
	}

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%08d", i)
		if err := db.Set(k, []byte("x")); err != nil {
			t.Fatalf("Set %s failed: %v", k, err)
		}
	}
}
