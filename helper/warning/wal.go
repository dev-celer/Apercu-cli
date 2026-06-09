package warning

import (
	"apercu-cli/helper/format"
	"fmt"
)

const (
	CodeHighWALVolume Code = "HIGH_WAL_VOLUME"
)

type WALSizeWarning struct {
	estimatedProdWAL int64
	prodDatabaseSize int64
}

func NewWALSizeWarning(estimatedProdWAL, prodDatabaseSize int64) *WALSizeWarning {
	w := WALSizeWarning{
		estimatedProdWAL: estimatedProdWAL,
		prodDatabaseSize: prodDatabaseSize,
	}

	if !w.IsWarning() {
		return nil
	}

	return &w
}

func (w *WALSizeWarning) GetText() string {
	if w.prodDatabaseSize <= 0 {
		return fmt.Sprintf("Migration is estimated to generate %s of WAL on production", format.BytesSizePretty(w.estimatedProdWAL))
	}
	return fmt.Sprintf("Migration is estimated to generate %s of WAL on production (%.0f%% of production database size)", format.BytesSizePretty(w.estimatedProdWAL), w.ratio()*100)
}

func (w *WALSizeWarning) GetTextLong() string {
	return w.GetText()
}

func (w *WALSizeWarning) IsWarning() bool {
	if w.levelByAbsolute() == nil && w.levelByRatio() == nil {
		return false
	}
	return true
}

func (w *WALSizeWarning) GetLevel() Level {
	byAbsolute := w.levelByAbsolute()
	byRatio := w.levelByRatio()

	// If one is nil, return the other
	if byAbsolute == nil && byRatio == nil {
		return WarningLevelLow
	}
	if byAbsolute == nil {
		return *byRatio
	}
	if byRatio == nil {
		return *byAbsolute
	}

	// If none are nil, return the highest
	if *byAbsolute > *byRatio {
		return *byAbsolute
	}
	return *byRatio
}

func (w *WALSizeWarning) GetCode() Code {
	return CodeHighWALVolume
}

func (w *WALSizeWarning) levelByAbsolute() *Level {
	// If > 10 GiB - Med
	if w.estimatedProdWAL > 10*1024*1024*1024 {
		return new(WarningLevelMedium)
	}

	// If > 1 GiB - Low
	if w.estimatedProdWAL > 1024*1024*1024 {
		return new(WarningLevelLow)
	}
	return nil
}

func (w *WALSizeWarning) levelByRatio() *Level {
	// Cannot compute ratio without prod size - Low
	if w.prodDatabaseSize <= 0 {
		return nil
	}

	r := w.ratio()

	// If WAL >= 100% of prod database size - High
	if r >= 1.0 {
		return new(WarningLevelHigh)
	}

	// If WAL >= 25% of prod database size - Med
	if r >= 0.25 {
		return new(WarningLevelMedium)
	}

	return nil
}

func (w *WALSizeWarning) ratio() float64 {
	if w.prodDatabaseSize <= 0 {
		return 0
	}
	return float64(w.estimatedProdWAL) / float64(w.prodDatabaseSize)
}

func (w *WALSizeWarning) GetIsIdempotent() bool {
	return false
}

func (w *WALSizeWarning) GetKeys() []string {
	return nil
}
