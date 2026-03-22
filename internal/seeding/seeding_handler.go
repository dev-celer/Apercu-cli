package seeding

import "time"

type HandlerInterface interface {
	Apply() error
	GetDuration() (time.Duration, error)
	GetStatus() (bool, error)
	GetOutput() (string, error)
}
