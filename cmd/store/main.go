package main

import (
	"fmt"

	"go.store/internal/engine"
)

type testData struct {
	Key   string
	value string
}

func main() {
	db, err := engine.Open("test.db")
	if err != nil {
		fmt.Printf("Oops.. %s\n", err)
	} else {
		fmt.Println("All good.")
	}

	strings := []string{"Test", "second", "Third", "another", "fourth", "anotherone", "test"}

	// Create some keys
	for _, s := range strings {
		cErr := db.Set(s, []byte(s))
		if cErr != nil {
			fmt.Printf("Oops... %s", err)
			break
		}
	}

	// Try some gets
	for _, s := range strings {
		val, gErr := db.Get(s)
		if gErr != nil {
			fmt.Printf("Oops.. %s", err)
			break
		}
		fmt.Printf("%s: %s\n", s, string(val))
	}
}
