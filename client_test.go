package zk

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/let-z-go/toolkit/logger"
)

func TestClient1(t *testing.T) {
	{
		var c Client
		c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if e := c.Run(ctx); e != context.Canceled {
			t.Errorf("%v", e)
		}
	}

	{
		var c Client
		c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(time.Second / 2)
			cancel()
		}()

		if e := c.Run(ctx); e != context.Canceled {
			t.Errorf("%v", e)
		}
	}
}

func TestClient2(t *testing.T) {
	{
		var c Client
		c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
		ctx, cancel := context.WithTimeout(context.Background(), 0)

		if e := c.Run(ctx); e != context.DeadlineExceeded {
			t.Errorf("%v", e)
		}

		cancel()
	}

	{
		var c Client
		c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second/2)

		if e := c.Run(ctx); e != context.DeadlineExceeded {
			t.Errorf("%v", e)
		}

		cancel()
	}
}

func TestClientCreateAndDelete(t *testing.T) {
	var c Client
	c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		{
			response, e := c.Create(context.Background(), "foo", []byte("bar"), nil, CreatePersistent, true)

			if e != nil {
				t.Errorf("%v", e)
			} else {
				if response.Path != "/foo" {
					t.Errorf("%#v", response)
				}
			}
		}

		{
			e := c.Delete(context.Background(), "foo", -1, true)

			if e != nil {
				t.Errorf("%v", e)
			}
		}

		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientExists(t *testing.T) {
	var c Client
	c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		rsp, w, e := c.ExistsW(context.Background(), "foo", true)

		if e != nil {
			t.Fatalf("%v", e)
		}

		if rsp != nil {
			t.Errorf("%#v", rsp)
		}

		c.session.transport.connection.Close()

		go func() {
			_, e := c.Create(context.Background(), "foo", []byte("bar"), nil, CreatePersistent, true)

			if e != nil {
				if e, ok := e.(*Error); !ok || e.GetCode() != ErrorSessionExpired {
					t.Errorf("%v", e)
				}
			}
		}()

		ev := <-w.Event()

		if ev.Type != WatcherEventNodeCreated {
			t.Errorf("%#v", ev)
		}

		rsp, w, e = c.ExistsW(context.Background(), "foo", true)

		if e != nil {
			t.Fatalf("%v", e)
		}

		if rsp == nil {
			t.Error()
		}

		c.session.transport.connection.Close()

		go func() {
			e := c.Delete(context.Background(), "foo", -1, true)

			if e != nil {
				if e, ok := e.(*Error); !ok || e.GetCode() != ErrorSessionExpired {
					t.Errorf("%v", e)
				}
			}
		}()

		ev = <-w.Event()

		if ev.Type != WatcherEventNodeDeleted {
			t.Errorf("%#v", ev)
		}

		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientGetSetACL(t *testing.T) {
	var c Client
	c.Initialize(sessionPolicy, serverAddresses,
		[]AuthInfo{AuthInfo{"digest", []byte("test:123")}}, []ACL{CreatorAllACL}, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, e := c.Create(context.Background(), "foo", []byte("bar"), nil, CreatePersistent, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, e := c.GetACL(context.Background(), "foo", true)

		if e != nil {
			t.Errorf("%v", e)
		}

		if len(rsp.ACL) != 1 {
			t.Errorf("%#v", rsp.ACL)
		}

		acl := rsp.ACL[0]

		if acl.Perms != CreatorAllACL.Perms {
			t.Errorf("%#v != %#v", acl.Perms, OpenACLUnsafe.Perms)
		}

		if acl.Id.Scheme != "digest" {
			t.Errorf("%#v", acl.Id.Scheme)
		}

		_, e = c.SetACL(context.Background(), "foo", []ACL{OpenACLUnsafe}, -1, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, e = c.GetACL(context.Background(), "foo", true)

		if e != nil {
			t.Errorf("%v", e)
		}

		if len(rsp.ACL) != 1 {
			t.Errorf("%#v", rsp.ACL)
		}

		acl = rsp.ACL[0]

		if acl.Perms != OpenACLUnsafe.Perms {
			t.Errorf("%#v != %#v", acl.Perms, OpenACLUnsafe.Perms)
		}

		if acl.Id != OpenACLUnsafe.Id {
			t.Errorf("%#v != %#v", acl.Id, OpenACLUnsafe.Id)
		}

		c.Delete(context.Background(), "foo", -1, true)
		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientGetChildren(t *testing.T) {
	var c Client
	c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, e := c.Create(context.Background(), "foo", []byte("bar"), nil, CreatePersistent, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, w, e := c.GetChildrenW(context.Background(), "foo", true)

		if e != nil {
			t.Fatalf("%v", e)
		}

		if len(rsp.Children) != 0 {
			t.Errorf("%#v", rsp.Children)
		}

		c.session.transport.connection.Close()

		go func() {
			_, e := c.Create(context.Background(), "foo/son", []byte("son"), nil, CreatePersistent, true)

			if e != nil {
				if e, ok := e.(*Error); !ok || e.GetCode() != ErrorSessionExpired {
					t.Errorf("%v", e)
				}
			}
		}()

		ev := <-w.Event()

		if ev.Type != WatcherEventNodeChildrenChanged {
			t.Errorf("%#v", ev)
		}

		rsp2, w, e := c.GetChildren2W(context.Background(), "foo", true)

		if e != nil {
			t.Fatalf("%v", e)
		}

		if !(len(rsp2.Children) == 1 && rsp2.Children[0] == "son") {
			t.Errorf("%#v", rsp2.Children)
		}

		c.session.transport.connection.Close()

		go func() {
			e := c.Delete(context.Background(), "foo/son", -1, true)

			if e != nil {
				if e, ok := e.(*Error); !ok || e.GetCode() != ErrorSessionExpired {
					t.Errorf("%v", e)
				}
			}
		}()

		ev = <-w.Event()

		if ev.Type != WatcherEventNodeChildrenChanged {
			t.Errorf("%#v", ev)
		}

		c.Delete(context.Background(), "foo", -1, true)
		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientGetSetData(t *testing.T) {
	var c Client
	c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, e := c.Create(context.Background(), "foo", []byte("bar"), nil, CreatePersistent, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, w, e := c.GetDataW(context.Background(), "foo", true)

		if e != nil {
			t.Fatalf("%v", e)
		}

		if !bytes.Equal(rsp.Data, []byte("bar")) {
			t.Errorf("%#v", rsp.Data)
		}

		c.session.transport.connection.Close()

		go func() {
			_, e := c.SetData(context.Background(), "foo", []byte("bar2"), -1, true)

			if e != nil {
				if e, ok := e.(*Error); !ok || e.GetCode() != ErrorSessionExpired {
					t.Errorf("%v", e)
				}
			}
		}()

		ev := <-w.Event()

		if ev.Type != WatcherEventNodeDataChanged {
			t.Errorf("%#v", ev)
		}

		rsp, w, e = c.GetDataW(context.Background(), "foo", true)

		if e != nil {
			t.Fatalf("%v", e)
		}

		if !bytes.Equal(rsp.Data, []byte("bar2")) {
			t.Errorf("%#v", rsp.Data)
		}

		c.session.transport.connection.Close()

		go func() {
			e := c.Delete(context.Background(), "foo", -1, true)

			if e != nil {
				if e, ok := e.(*Error); !ok || e.GetCode() != ErrorSessionExpired {
					t.Errorf("%v", e)
				}
			}
		}()

		ev = <-w.Event()

		if ev.Type != WatcherEventNodeDeleted {
			t.Errorf("%#v", ev)
		}

		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientSync(t *testing.T) {
	var c Client
	c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		rsp, e := c.Sync(context.Background(), "/", true)

		if e != nil {
			t.Errorf("%v", e)
		}

		if rsp.Path != "/" {
			t.Errorf("%#v", rsp)
		}

		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientMulti(t *testing.T) {
	var c Client
	c.Initialize(sessionPolicy, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ops := []Op{
			c.CreateOp("foo", []byte("bar"), nil, CreatePersistent),
			c.SetDataOp("foo", []byte("bar2"), -1),
			c.CheckOp("foo", -1),
			c.DeleteOp("foo", -1),
		}

		rsp, e := c.Multi(context.Background(), ops, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		for i := range rsp.OpResults {
			r := &rsp.OpResults[i]

			if r.Type == OpError {
				t.Errorf("%#v", r)
			}
		}

		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func BenchmarkClient(b *testing.B) {
	sp := &SessionPolicy{
		MaxNumberOfPendingOperations: 65536,
	}

	sp.Logger.Initialize("zktest", logger.SeverityInfo, os.Stdout, os.Stderr)
	var c Client
	c.Initialize(sp, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		wg := sync.WaitGroup{}

		for i := 0; i < 1000; i++ {
			wg.Add(1)

			go func(j int) {
				for i := 0; i < 100; i++ {
					switch (i + j) % 3 {
					case 0:
						c.GetData(context.Background(), "/", true)
					case 1:
						c.Exists(context.Background(), "/", true)
					case 2:
						c.GetChildren(context.Background(), "/", true)
					}
				}
				wg.Done()
			}(i)
		}

		wg.Wait()
		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		b.Errorf("%v", e)
	}
}

var sessionPolicy *SessionPolicy
var serverAddresses = []string{"192.168.33.1:2181", "192.168.33.1:2182", "192.168.33.1:2183"}

func init() {
	sessionPolicy = &SessionPolicy{}
	sessionPolicy.Logger.Initialize("zk", logger.SeverityInfo, os.Stdout, os.Stderr)
}
