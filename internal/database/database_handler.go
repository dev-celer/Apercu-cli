package database

type HandlerInterface interface {
	Apply() error
	Cleanup() error
	Reset() error
	GetDatabaseUrl() string
}
