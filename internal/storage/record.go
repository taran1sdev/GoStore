package storage

type Flag uint8

const (
	FlagSet Flag = iota
	FlagDel
)

type Record struct {
	Key   []byte
	Value []byte
	Flag  Flag
}
