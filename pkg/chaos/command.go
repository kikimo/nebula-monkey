package chaos

import "sync/atomic"

type ChaosCommand interface {
	Execute()
	Stop()
}

type BaseCommand struct {
	stopped int32
}

func (c *BaseCommand) Stop() {
	atomic.StoreInt32(&c.stopped, 1)
}

func (c *BaseCommand) Stopped() bool {
	return atomic.LoadInt32(&c.stopped) == 1
}
