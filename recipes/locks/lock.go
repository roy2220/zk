package locks

import (
	"context"
	"sort"

	"github.com/let-z-go/zk"
	"github.com/let-z-go/zk/recipes/utils"
)

type Lock struct {
	lockBase
}

func (self *Lock) Initialize(client *zk.Client, path string) *Lock {
	self.lockBase.initialize(client, path)
	return self
}

func (self *Lock) Acquire(context_ context.Context) error {
	return self.doAcquire(context_, "", func(waiterNames []string, myWaiterName string) int {
		sort.Sort(utils.SequentialNodeNames(waiterNames))
		var i int

		for i = 0; i < len(waiterNames); i++ {
			if waiterNames[i] == myWaiterName {
				break
			}
		}

		return i - 1
	})
}
