package warning

import (
	"apercu-cli/helper"
	"apercu-cli/helper/warning_interface"
	"encoding/json"
	"fmt"
)

const (
	CodeLockTimeoutNotSet Code = "LOCK_TIMEOUT_NOT_SET"
)

type LockTimeout struct {
	table helper.FullTableName
}

func NewLockTimeoutWarning(table helper.FullTableName) *LockTimeout {
	return &LockTimeout{table: table}
}

func (w *LockTimeout) GetText() string {
	return fmt.Sprintf("The lock_timeout value wasn't set while a locking statement was sent on table %s, this can cause lock queue regardless of the statement duration", w.table.String())
}

func (w *LockTimeout) GetTextLong() string {
	return w.GetText()
}

func (w *LockTimeout) GetLevel() warning_interface.Level {
	return WarningLevelHigh
}

func (w *LockTimeout) GetCode() warning_interface.Code {
	return CodeLockTimeoutNotSet
}

func (w *LockTimeout) GetFullCode() string {
	return fmt.Sprintf("%s.%s", w.GetCode(), FormatKey(w.table.String()))
}

func (w *LockTimeout) GetIsIdempotent() bool {
	return false
}

func (w *LockTimeout) GetTable() helper.FullTableName {
	return w.table
}

type LockTimeoutState struct {
	Table helper.FullTableName `json:"table"`
}

func (w *LockTimeout) GetStateValues() (json.RawMessage, error) {
	v := LockTimeoutState{
		Table: w.table,
	}
	return json.Marshal(v)
}

type LockTimeoutCollapsed struct {
	tables []helper.FullTableName
}

func (w *LockTimeoutCollapsed) GetText() string {
	return fmt.Sprintf("The lock_timeout value wasn't set while a locking statement was sent on %d tables, this can cause lock queue regardless of the statement duration", len(w.tables))
}

func (w *LockTimeoutCollapsed) GetTextLong() string {
	return w.GetText()
}

func (w *LockTimeoutCollapsed) GetLevel() warning_interface.Level {
	return WarningLevelHigh
}

func (w *LockTimeoutCollapsed) GetCode() warning_interface.Code {
	return CodeLockTimeoutNotSet
}

func (w *LockTimeoutCollapsed) GetFullCode() string {
	return string(w.GetCode())
}

func (w *LockTimeoutCollapsed) GetIsIdempotent() bool {
	return false
}

func (w *LockTimeoutCollapsed) GetStateValues() (json.RawMessage, error) {
	return json.Marshal("{}")
}

func NewLockTimeoutCollapsed(warnings ...*LockTimeout) *LockTimeoutCollapsed {
	var tables []helper.FullTableName
	for _, w := range warnings {
		tables = append(tables, w.table)
	}

	return &LockTimeoutCollapsed{tables: tables}
}
