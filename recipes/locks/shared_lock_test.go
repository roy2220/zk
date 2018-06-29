package locks

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/let-z-go/toolkit/logger"
	"github.com/let-z-go/zk"
)

func TestSharedLock(t *testing.T) {
	var c1 zk.Client
	var c2 zk.Client
	c1.Initialize(sessionPolicy2, serverAddresses2, nil, nil, "/")
	c2.Initialize(sessionPolicy2, serverAddresses2, nil, nil, "/")
	var l1 SharedLock
	var l2 SharedLock
	l1.Initialize(&c1, "/locktest")
	l2.Initialize(&c2, "/locktest")
	var wg sync.WaitGroup
	wg.Add(2)
	s := int32(0)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			if _, e := c2.Create(ctx, "/locktest", []byte{}, nil, zk.CreatePersistent, true); e != nil {
				if e, ok := e.(zk.Error); !(ok && e.GetCode() == zk.ErrorNodeExists) {
					t.Errorf("%v", e)
					cancel()
					return
				}
			}

			ch := make(chan int)

			for i := 0; i < 3; i++ {
				go func(i int) {
					if e := l1.Acquire(ctx); e != nil {
						t.Errorf("%v", e)
						<-ch
					}

					atomic.AddInt32(&s, 1)
					time.Sleep(time.Second)
					atomic.AddInt32(&s, -1)

					if e := l1.Release(); e != nil {
						t.Errorf("%v", e)
					}

					ch <- i
				}(i)
			}

			for i := 0; i < 3; i++ {
				<-ch
			}

			c1.Delete(ctx, "/locktest", -1, true)
			cancel()
		}()

		if e := c1.Run(ctx); e != context.Canceled {
			t.Errorf("%v", e)
		}

		wg.Done()
	}()

	go func() {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			if _, e := c2.Create(ctx, "/locktest", []byte{}, nil, zk.CreatePersistent, true); e != nil {
				if e, ok := e.(zk.Error); !(ok && e.GetCode() == zk.ErrorNodeExists) {
					t.Errorf("%v", e)
					cancel()
					return
				}
			}

			ch := make(chan int)
			time.Sleep(time.Second / 2)

			for i := 3; i < 6; i++ {
				go func(i int) {
					if e := l2.Acquire(ctx); e != nil {
						t.Errorf("%v", e)
						<-ch
					}

					if s := atomic.AddInt32(&s, 1); s < 3 {
						t.Errorf("%#v", s)
					}

					time.Sleep(time.Second / 2)
					atomic.AddInt32(&s, -1)

					if e := l2.Release(); e != nil {
						t.Errorf("%v", e)
					}

					ch <- i
				}(i)
			}

			for i := 0; i < 3; i++ {
				<-ch
			}

			c2.Delete(ctx, "/locktest", -1, true)
			cancel()
		}()

		if e := c2.Run(ctx); e != context.Canceled {
			t.Errorf("%v", e)
		}

		wg.Done()
	}()

	wg.Wait()
}

var sessionPolicy2 *zk.SessionPolicy
var serverAddresses2 = []string{"192.168.33.1:2181", "192.168.33.1:2182", "192.168.33.1:2183"}

func init() {
	sessionPolicy2 = &zk.SessionPolicy{}
	sessionPolicy2.Logger.Initialize("zk.recipes.locks", logger.SeverityInfo, os.Stdout, os.Stderr)
}
