package warning_interface

import "encoding/json"

type Warning interface {
	GetText() string
	GetTextLong() string
	GetLevel() Level
	GetCode() Code
	GetFullCode() string
	GetIsIdempotent() bool
	GetStateValues() (json.RawMessage, error)
}

type Code string

type Level uint8

func (l Level) String() string {
	switch l {
	case WarningLevelLow:
		return "low"
	case WarningLevelMedium:
		return "medium"
	case WarningLevelHigh:
		return "high"
	}
	return "unknown"
}

const (
	WarningLevelLow    Level = 1
	WarningLevelMedium Level = 2
	WarningLevelHigh   Level = 3
)
