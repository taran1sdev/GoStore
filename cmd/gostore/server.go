package main

import "go.store/internal/server"

func main() {
	s := server.New("localhost:57083")

	s.Listen()
}
