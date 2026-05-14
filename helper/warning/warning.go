package warning

import (
	"fmt"
	"log"
)

type Warning interface {
	GetWarningText() string
	GetWarningLevel() Level
	GetWarningCode() Code
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

func PrintWarning(w Warning) {
	_, _ = fmt.Fprintln(log.Writer(), fmt.Sprintf("WARNING: %s", w.GetWarningText()))
}
