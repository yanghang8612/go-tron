package node

type Lifecycle interface {
	Start() error
	Stop() error
}
