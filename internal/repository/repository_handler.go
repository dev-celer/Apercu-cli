package repository

type HandlerInterface interface {
	GetOpenedPullRequestsNumber() ([]string, error)
}
