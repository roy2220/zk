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

func TestLock(t *testing.T) {
	var c1 zk.Client
	var c2 zk.Client
	c1.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	c2.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	var l1 Lock
	var l2 Lock
	l1.Initialize(&c1, "/locktest")
	l2.Initialize(&c2, "/locktest")
	var wg sync.WaitGroup
	wg.Add(2)
	var su sync.WaitGroup
	su.Add(3)
	s1 := int32(0)
	s2 := int32(0)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			if _, e := c1.Create(context.Background(), "/locktest", nil, nil, zk.CreatePersistent, true); e != nil {
				if e, ok := e.(zk.Error); !(ok && e.GetCode() == zk.ErrorNodeExists) {
					t.Errorf("%v", e)
					cancel()
					return
				}
			}

			ch := make(chan int)

			for i := 0; i < 3; i++ {
				go func(i int) {
					su.Done()

					if e := l1.Acquire(context.Background()); e != nil {
						t.Errorf("%v", e)
						ch <- i
						return
					}

					atomic.AddInt32(&s1, 1)
					time.Sleep(time.Second)

					if s1 := atomic.LoadInt32(&s1); s1 != 3 {
						t.Errorf("%#v", s1)
					}

					if e := l1.Release(); e != nil {
						t.Errorf("%v", e)
					}

					ch <- i
				}(i)
			}

			for i := 0; i < 3; i++ {
				<-ch
			}

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
			su.Wait()
			ch := make(chan int)
			time.Sleep(time.Second / 2)

			for i := 3; i < 6; i++ {
				go func(i int) {
					if e := l2.Acquire(context.Background()); e != nil {
						t.Errorf("%v", e)
						ch <- i
						return
					}

					if s1 := atomic.LoadInt32(&s1); s1 != 3 {
						t.Errorf("%#v", s1)
					}

					atomic.AddInt32(&s2, 1)
					time.Sleep(time.Second / 2)

					if s2 := atomic.LoadInt32(&s2); s2 != 3 {
						t.Errorf("%#v", s2)
					}

					if e := l2.Release(); e != nil {
						t.Errorf("%v", e)
					}

					ch <- i
				}(i)
			}

			for i := 0; i < 3; i++ {
				<-ch
			}

			c2.Delete(context.Background(), "/locktest", -1, true)
			cancel()
		}()

		if e := c2.Run(ctx); e != context.Canceled {
			t.Errorf("%v", e)
		}

		wg.Done()
	}()

	wg.Wait()
}

var sessionPolicy *zk.SessionPolicy
var serverAddresses = []string{"192.168.33.1:2181", "192.168.33.1:2182", "192.168.33.1:2183"}

func init() {
	sessionPolicy = &zk.SessionPolicy{}
	sessionPolicy.Logger.Initialize("zk.recipes.locks", logger.SeverityInfo, os.Stdout, os.Stderr)
}
