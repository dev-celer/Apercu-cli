package warning

import "fmt"

const (
	CodeFailedToOpenSeedFile Code = "SEED_FILE_OPEN_ERR"
	CodeSeedFileNotFound     Code = "SEED_FILE_NOT_FOUND"
)

type SeedingError struct {
	path string
	code Code
}

func (w *SeedingError) GetText() string {
	switch w.code {
	case CodeFailedToOpenSeedFile:
		return fmt.Sprintf("Failed to open seed file (%s)", w.path)
	case CodeSeedFileNotFound:
		return fmt.Sprintf("Seed file not found (%s)", w.path)
	default:
		return ""
	}
}

func (w *SeedingError) GetTextLong() string { return w.GetText() }

func (w *SeedingError) GetLevel() Level {
	return WarningLevelLow
}

func (w *SeedingError) GetCode() Code {
	return w.code
}

func (w *SeedingError) GetIsIdempotent() bool {
	return true
}

func (w *SeedingError) GetKeys() []string {
	return []string{w.path}
}

func NewSeedingError(code Code, filepath string) *SeedingError {
	switch code {
	case CodeFailedToOpenSeedFile:
	case CodeSeedFileNotFound:
	default:
		return nil
	}

	return &SeedingError{
		code: code,
		path: filepath,
	}
}
