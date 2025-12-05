package storage_test

import (
	"bytes"
	"fmt"
	"testing"

	"go.store/internal/engine"
)

func TestAscendingInsertAndGet(t *testing.T) {
	db, err := engine.Open("test_insert.db")
	if err != nil {
		t.Fatal(err)
	}

	const N = 10000

	// Insert ascending
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%08d", i)
		if err := db.Set(k, []byte("x")); err != nil {
			t.Fatalf("Set %s failed: %v", k, err)
		}
	}

	// Delete descending
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%08d", i)
		if _, err := db.Get(k); err != nil {
			t.Fatalf("Get %s failed: %v", k, err)
		}

	}
}

func TestAscendingInsertDescendingDelete(t *testing.T) {
	db, err := engine.Open("test_ordered.db")
	if err != nil {
		t.Fatal(err)
	}

	const N = 10000

	// Insert ascending
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%08d", i)
		if err := db.Set(k, []byte("x")); err != nil {
			t.Fatalf("Set %s failed: %v", k, err)
		}
	}

	// Delete descending
	for i := N - 1; i >= 0; i-- {
		k := fmt.Sprintf("%08d", i)
		if err := db.Delete(k); err != nil {
			t.Fatalf("Delete %s failed: %v", k, err)
		}

		// Check that previously deleted keys do not return values
		if i%500 == 0 {
			_, err := db.Get(k)
			if err == nil {
				t.Fatalf("Expected Get(%s) to fail after delete", k)
			}
		}
	}
}

func TestDuplicateKeys(t *testing.T) {
	db, err := engine.Open("test_dup.db")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Set("dup", []byte("1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Second insert should fail
	if err := db.Set("dup", []byte("2")); err == nil {
		t.Fatalf("Duplicate Set failed: %v", err)
	}

	v, err := db.Get("dup")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("Expected overwrite to store 2, got %s", v)
	}
}
