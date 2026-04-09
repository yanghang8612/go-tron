package node

import "sync"

type Node struct {
	config     *Config
	lifecycles []Lifecycle
	running    bool
	lock       sync.Mutex
	stop       chan struct{}
}

func New(config *Config) (*Node, error) {
	return &Node{
		config: config,
		stop:   make(chan struct{}),
	}, nil
}

func (n *Node) Config() *Config {
	return n.config
}

func (n *Node) RegisterLifecycle(lc Lifecycle) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.lifecycles = append(n.lifecycles, lc)
}

func (n *Node) Start() error {
	n.lock.Lock()
	defer n.lock.Unlock()

	var started []Lifecycle
	for _, lc := range n.lifecycles {
		if err := lc.Start(); err != nil {
			for i := len(started) - 1; i >= 0; i-- {
				started[i].Stop()
			}
			return err
		}
		started = append(started, lc)
	}
	n.running = true
	return nil
}

func (n *Node) Stop() {
	n.lock.Lock()
	defer n.lock.Unlock()

	for i := len(n.lifecycles) - 1; i >= 0; i-- {
		n.lifecycles[i].Stop()
	}
	n.running = false
	close(n.stop)
}

func (n *Node) Wait() {
	<-n.stop
}
