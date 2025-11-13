package storage

type Record struct {
	Key   []byte
	Value []byte
	Flags uint8
}
