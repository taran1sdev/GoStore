package main

import (
	"fmt"

	"go.store/internal/engine"
)

func main() {
	_, err := engine.Open("test.db")
	if err != nil {
		fmt.Printf("Oops.. %w\n", err)
	} else {
		fmt.Println("All good.")
	}
}
