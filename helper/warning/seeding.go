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

func (s SeedingError) GetWarningText() string {
	switch s.code {
	case CodeFailedToOpenSeedFile:
		return fmt.Sprintf("Failed to open seed file (%s)", s.path)
	case CodeSeedFileNotFound:
		return fmt.Sprintf("Seed file not found (%s)", s.path)
	default:
		return ""
	}
}

func (s SeedingError) GetWarningTextLong() string { return s.GetWarningText() }

func (s SeedingError) GetWarningLevel() Level {
	return WarningLevelLow
}

func (s SeedingError) GetWarningCode() Code {
	return s.code
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
