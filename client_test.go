package zk

import (
	"bytes"
	"context"
	"testing"
	"time"
	/*
		"fmt"
		"sync"
	*/)

func TestClient1(t *testing.T) {
	{
		var c Client
		c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if e := c.Run(ctx); e != context.Canceled {
			t.Errorf("%v", e)
		}
	}

	{
		var c Client
		c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
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
		c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
		ctx, cancel := context.WithTimeout(context.Background(), 0)

		if e := c.Run(ctx); e != context.DeadlineExceeded {
			t.Errorf("%v", e)
		}

		cancel()
	}

	{
		var c Client
		c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second/2)

		if e := c.Run(ctx); e != context.DeadlineExceeded {
			t.Errorf("%v", e)
		}

		cancel()
	}
}

func TestClientCreateAndDelete(t *testing.T) {
	var c Client
	c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		{
			response, e := c.Create(ctx, "foo", []byte("bar"), nil, CreatePersistent, true)

			if e != nil {
				t.Errorf("%v", e)
			} else {
				if response.Path != "/foo" {
					t.Errorf("%#v", response)
				}
			}
		}

		{
			e := c.Delete(ctx, "foo", -1, true)

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
	c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		rsp, w, e := c.ExistsW(ctx, "foo", true)

		if e != nil {
			t.Fatal("%v", e)
		}

		if rsp != nil {
			t.Errorf("%#v", rsp)
		}

		c.session.transport.connection.Close()

		go func() {
			_, e := c.Create(ctx, "foo", []byte("bar"), nil, CreatePersistent, true)

			if e != nil && e != context.Canceled {
				t.Errorf("%v", e)
			}
		}()

		ev := <-w.Event()

		if ev.Type != WatcherEventNodeCreated {
			t.Errorf("%#v", ev)
		}

		rsp, w, e = c.ExistsW(ctx, "foo", true)

		if e != nil {
			t.Fatal("%v", e)
		}

		if rsp == nil {
			t.Error()
		}

		c.session.transport.connection.Close()

		go func() {
			e := c.Delete(ctx, "foo", -1, true)

			if e != nil && e != context.Canceled {
				t.Errorf("%v", e)
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
	c.Initialize(&SessionPolicy{}, serverAddresses,
		[]AuthInfo{AuthInfo{"digest", []byte("test:123")}}, []ACL{CreatorAllACL}, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, e := c.Create(ctx, "foo", []byte("bar"), nil, CreatePersistent, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, e := c.GetACL(ctx, "foo", true)

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

		_, e = c.SetACL(ctx, "foo", []ACL{OpenACLUnsafe}, -1, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, e = c.GetACL(ctx, "foo", true)

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

		c.Delete(ctx, "foo", -1, true)
		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientGetChildren(t *testing.T) {
	var c Client
	c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, e := c.Create(ctx, "foo", []byte("bar"), nil, CreatePersistent, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, w, e := c.GetChildrenW(ctx, "foo", true)

		if e != nil {
			t.Fatal("%v", e)
		}

		if len(rsp.Children) != 0 {
			t.Errorf("%#v", rsp.Children)
		}

		c.session.transport.connection.Close()

		go func() {
			_, e := c.Create(ctx, "foo/son", []byte("son"), nil, CreatePersistent, true)

			if e != nil && e != context.Canceled {
				t.Errorf("%v", e)
			}
		}()

		ev := <-w.Event()

		if ev.Type != WatcherEventNodeChildrenChanged {
			t.Errorf("%#v", ev)
		}

		rsp2, w, e := c.GetChildren2W(ctx, "foo", true)

		if e != nil {
			t.Fatal("%v", e)
		}

		if !(len(rsp2.Children) == 1 && rsp2.Children[0] == "son") {
			t.Errorf("%#v", rsp2.Children)
		}

		c.session.transport.connection.Close()

		go func() {
			e := c.Delete(ctx, "foo/son", -1, true)

			if e != nil && e != context.Canceled {
				t.Errorf("%v", e)
			}
		}()

		ev = <-w.Event()

		if ev.Type != WatcherEventNodeChildrenChanged {
			t.Errorf("%#v", ev)
		}

		c.Delete(ctx, "foo", -1, true)
		cancel()
	}()

	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}

func TestClientGetSetData(t *testing.T) {
	var c Client
	c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, e := c.Create(ctx, "foo", []byte("bar"), nil, CreatePersistent, true)

		if e != nil {
			t.Errorf("%v", e)
		}

		rsp, w, e := c.GetDataW(ctx, "foo", true)

		if e != nil {
			t.Fatal("%v", e)
		}

		if !bytes.Equal(rsp.Data, []byte("bar")) {
			t.Errorf("%#v", rsp.Data)
		}

		c.session.transport.connection.Close()

		go func() {
			_, e := c.SetData(ctx, "foo", []byte("bar2"), -1, true)

			if e != nil && e != context.Canceled {
				t.Errorf("%v", e)
			}
		}()

		ev := <-w.Event()

		if ev.Type != WatcherEventNodeDataChanged {
			t.Errorf("%#v", ev)
		}

		rsp, w, e = c.GetDataW(ctx, "foo", true)

		if e != nil {
			t.Fatal("%v", e)
		}

		if !bytes.Equal(rsp.Data, []byte("bar2")) {
			t.Errorf("%#v", rsp.Data)
		}

		c.session.transport.connection.Close()

		go func() {
			e := c.Delete(ctx, "foo", -1, true)

			if e != nil && e != context.Canceled {
				t.Errorf("%v", e)
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
	c.Initialize(&SessionPolicy{}, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		rsp, e := c.Sync(ctx, "/", true)

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

/*
func TestClientPerformance(t *testing.T) {
	fmt.Println("---------------------------------------------")
	var c Client
	c.Initialize(&SessionPolicy{
		MaxNumberOfPendingOperations: 65536,
	}, serverAddresses, nil, nil, "/")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		wg := sync.WaitGroup{}

		bt := time.Now()
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(i int) {
				for i := 0; i < 100; i++ {
					c.Exists(ctx, "foo", true)
				}
				// fmt.Println(i)
				wg.Done()
			}(i)
		}
		wg.Wait()
		et := time.Now()
		fmt.Printf("%v\n", et.Sub(bt))
		cancel()
	}()
	if e := c.Run(ctx); e != context.Canceled {
		t.Errorf("%v", e)
	}
}
*/

var serverAddresses = []string{"192.168.33.1:2181", "192.168.33.1:2182", "192.168.33.1:2183"}
