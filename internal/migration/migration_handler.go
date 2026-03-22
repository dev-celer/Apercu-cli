package migration

import "time"

type HandlerInterface interface {
	GetCount() (int, error)
	Apply() error
	GetDuration() (time.Duration, error)
	GetStatus() (bool, error)
	GetOutput() (string, error)
}
