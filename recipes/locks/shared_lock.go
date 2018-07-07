package locks

import (
	"context"
	"sort"
	"strings"

	"github.com/let-z-go/zk"
	"github.com/let-z-go/zk/recipes/utils"
)

type SharedLock struct {
	lockBase
}

func (self *SharedLock) Initialize(client *zk.Client, path string) *SharedLock {
	self.lockBase.initialize(client, path)
	return self
}

func (self *SharedLock) Acquire(context_ context.Context) error {
	return self.doAcquire(context_, sharedWaiterNamePrefix, func(waiterNames []string, myWaiterName string) int {
		sort.Sort(utils.SequentialNodeNames(waiterNames))
		var i int

		for i = 0; i < len(waiterNames); i++ {
			if waiterNames[i] == myWaiterName {
				break
			}
		}

		var j int

		for j = i - 1; j >= 0; j-- {
			if !strings.HasPrefix(waiterNames[j], sharedWaiterNamePrefix) {
				break
			}
		}

		return j
	})
}

const sharedWaiterNamePrefix = "shared!"
