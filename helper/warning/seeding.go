package warning

import "fmt"

const (
	CodeFailedToOpenSeedFile Code = "SEED_FILE_OPEN_ERR"
	CodeSeedFileNotFound     Code = "SEED_FILE_NOT_FOUND"
)

type SeedingError struct {
	Msg  string
	Code Code
}

func (s SeedingError) GetWarningText() string {
	return s.Msg
}

func (s SeedingError) GetWarningLevel() Level {
	return WarningLevelLow
}

func (s SeedingError) GetWarningCode() Code {
	return s.Code
}

func NewSeedingError(code Code, filepath string) *SeedingError {
	var msg string
	switch code {
	case CodeFailedToOpenSeedFile:
		msg = fmt.Sprintf("Failed to open seed file (%s)", filepath)
	case CodeSeedFileNotFound:
		msg = fmt.Sprintf("Seed file not found (%s)", filepath)
	default:
		return nil
	}

	return &SeedingError{
		Code: code,
		Msg:  msg,
	}
}
