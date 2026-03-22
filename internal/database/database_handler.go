package database

type HandlerInterface interface {
	Create() error
	Drop() error
	Reset() error
	GetDatabaseUrl() string
}
