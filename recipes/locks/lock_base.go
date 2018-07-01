package locks

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/let-z-go/toolkit/uuid"
	"github.com/let-z-go/zk"
	"github.com/let-z-go/zk/recipes/utils"
)

type lockBase struct {
	client       *zk.Client
	path         string
	mutex        sync.Mutex
	myWaiterPath string
	ownerCount   int
}

func (self *lockBase) Initialize(client *zk.Client, path string) {
	self.client = client
	self.path = client.NormalizePath(path)
}

func (self *lockBase) doAcquire(context_ context.Context, waiterNamePrefix string, priorWaiterLocator func([]string, string) int) error {
	if e := context_.Err(); e != nil {
		return e
	}

	self.mutex.Lock()

	if self.ownerCount == 0 {
		uuid_, e := uuid.GenerateUUID4()

		if e != nil {
			self.mutex.Unlock()
			return e
		}

		myWaiterNamePrefix := waiterNamePrefix + uuid_.Base64() + string(utils.SequenceNumberDelimiter)
		myWaiterPathPrefix := self.path + "/" + myWaiterNamePrefix
		var myWaiterPath string
		var waiterNames []string

		for {
			response, e := self.client.Create(nil, myWaiterPathPrefix, []byte{}, nil, zk.CreateEphemeralSequential, false)

			if e == nil {
				myWaiterPath = response.Path
				response2, e := self.client.GetChildren(nil, self.path, true)

				if e != nil {
					self.mutex.Unlock()
					return e
				}

				waiterNames = response2.Children
				break
			}

			if e2, ok := e.(zk.Error); !(ok && e2.GetCode() == zk.ErrorConnectionLoss) {
				self.mutex.Unlock()
				return e
			}

			response2, e := self.client.GetChildren(nil, self.path, true)

			if e != nil {
				self.mutex.Unlock()
				return e
			}

			waiterNames = response2.Children
			ok := false

			for _, waiterName := range waiterNames {
				if strings.HasPrefix(waiterName, myWaiterNamePrefix) {
					myWaiterPath = self.path + "/" + waiterName
					ok = true
					break
				}
			}

			if ok {
				break
			}
		}

		myWaiterName := utils.GetNodeName(myWaiterPath)

		for {
			i := priorWaiterLocator(waiterNames, myWaiterName)

			if i < 0 {
				break
			}

			priorWaiterPath := self.path + "/" + waiterNames[i]
			response, watcher, e := self.client.ExistsW(nil, priorWaiterPath, true)

			if e != nil {
				self.mutex.Unlock()
				return e
			}

			if response == nil {
				watcher.Remove()
			} else {
				select {
				case <-watcher.Event():
				case <-context_.Done():
					self.client.Delete(nil, myWaiterPath, -1, true)
					self.mutex.Unlock()
					return context_.Err()
				}
			}

			response2, e := self.client.GetChildren(nil, self.path, true)

			if e != nil {
				self.mutex.Unlock()
				return e
			}

			waiterNames = response2.Children
		}

		self.myWaiterPath = myWaiterPath
	}

	self.ownerCount++
	self.mutex.Unlock()
	return nil
}

func (self *lockBase) Release() error {
	self.mutex.Lock()

	if self.ownerCount == 0 {
		self.mutex.Unlock()
		return LockNotOwnedError
	}

	if self.ownerCount == 1 {
		if e := self.client.Delete(nil, self.myWaiterPath, -1, true); e != nil {
			self.mutex.Unlock()
			return e
		}

		self.myWaiterPath = ""
	}

	self.ownerCount--
	self.mutex.Unlock()
	return nil
}

var LockNotOwnedError = errors.New("zk: lock not owned")
