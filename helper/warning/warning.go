package warning

import (
	"fmt"
	"log"
	"reflect"
)

type Warning interface {
	GetText() string
	GetTextLong() string
	GetLevel() Level
	GetCode() Code
	GetIsIdempotent() bool
	GetKeys() []string
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
	if w == nil {
		return
	}
	// Guard against a typed-nil pointer wrapped in a non-nil interface
	if v := reflect.ValueOf(w); v.Kind() == reflect.Ptr && v.IsNil() {
		return
	}
	_, _ = fmt.Fprintln(log.Writer(), fmt.Sprintf("WARNING: %s", w.GetTextLong()))
}

type WarningState struct {
	Code         Code
	Level        Level
	Keys         []string
	IsIdempotent bool
	WarningText  string
}

func NewWarningState(w Warning) WarningState {
	return WarningState{
		Code:         w.GetCode(),
		Level:        w.GetLevel(),
		IsIdempotent: w.GetIsIdempotent(),
		Keys:         w.GetKeys(),
		WarningText:  w.GetText(),
	}
}
