package engine

import (
	"fmt"
	"os"

	"go.store/internal/logger"
	"go.store/internal/storage"
)

func Open(path string) (*Database, error) {
	logPath := path + ".log"
	logFile, lErr := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if lErr != nil {
		return nil, fmt.Errorf("failed to open log file: %w", lErr)
	}

	log := logger.New(logFile, logger.INFO)

	pager, pErr := storage.Open(path, log)
	if pErr != nil {
		return nil, pErr
	}

	tree, tErr := storage.NewBTree(pager, log)
	if tErr != nil {
		return nil, tErr
	}

	eng := NewEngine(tree, log)

	return &Database{
		engine: eng,
		sync:   true,
	}, nil
}
