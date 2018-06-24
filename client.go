package zk

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"

	"github.com/let-z-go/toolkit/delay_pool"
)

type Client struct {
	session         session
	serverAddresses delay_pool.DelayPool
	authInfos       []AuthInfo
	defaultACL      []ACL
	pathPrefix      string
}

func (self *Client) Initialize(
	sessionPolicy *SessionPolicy,
	serverAddresses []string,
	authInfos []AuthInfo,
	defaultACL []ACL,
	pathPrefix string,
) {
	self.session.Initialize(sessionPolicy)

	if serverAddresses == nil {
		serverAddresses = []string{"127.0.0.1:2181"}
	} else if len(serverAddresses) == 0 {
		panic(fmt.Errorf("zk: client initialization: serverAddresses=%#v", serverAddresses))
	}

	values := make([]interface{}, len(serverAddresses))

	for i, serverAddress := range serverAddresses {
		values[i] = serverAddress
	}

	self.serverAddresses.Reset(values, 3, self.session.GetTimeout())
	self.authInfos = authInfos

	if defaultACL == nil {
		defaultACL = []ACL{OpenACLUnsafe}
	} else if len(defaultACL) == 0 {
		panic(fmt.Errorf("zk: client initialization: defaultACL=%#v", defaultACL))
	}

	self.defaultACL = defaultACL

	if pathPrefix == "" {
		panic(fmt.Errorf("zk: client initialization: pathPrefix=%#v", pathPrefix))
	}

	self.pathPrefix = "/"
	pathPrefix = self.NormalizePath(pathPrefix) + "/"

	if pathPrefix == "//" {
		self.pathPrefix = "/"
	} else {
		self.pathPrefix = pathPrefix
	}
}

func (self *Client) Run(context_ context.Context) error {
	if self.session.IsClosed() {
		return nil
	}

	var e error

	for {
		var value interface{}
		value, e = self.serverAddresses.GetValue(context_)

		if e != nil {
			// TODO: add log
			break
		}

		context2, cancel := context.WithDeadline(context_, self.serverAddresses.WhenNextValueUsable())
		serverAddress := value.(string)
		e = self.session.Connect(context2, serverAddress, self.authInfos)
		cancel()

		if e != nil {
			if e != io.EOF {
				if e == context.DeadlineExceeded {
					break
				}

				if _, ok := e.(net.Error); !ok {
					break
				}
			}

			continue
		}

		self.serverAddresses.Reset(nil, 9, self.session.GetTimeout())
		e = self.session.Dispatch(context_)

		if e != nil {
			if e != io.EOF {
				if e == context.DeadlineExceeded {
					break
				}

				if _, ok := e.(net.Error); !ok {
					break
				}
			}

			continue
		}
	}

	if !self.session.IsClosed() {
		self.session.Close()
	}

	self.serverAddresses.Collect()
	self.authInfos = nil
	self.defaultACL = nil
	self.pathPrefix = ""
	return e
}

func (self *Client) NormalizePath(path string) string {
	if path == "" {
		panic(fmt.Errorf("zk: path normalization: path=%#v", path))
	}

	runes := append([]rune(path), '/')
	i := 1

	for j := 1; j < len(runes); j++ {
		if runes[j] == '/' && runes[j-1] == '/' {
			continue
		}

		runes[i] = runes[j]
		i++
	}

	if runes[0] == '/' {
		if i >= 2 {
			path = string(runes[:i-1])
		}
	} else {
		path = self.pathPrefix + string(runes[:i-1])
	}

	return path
}

func (self *Client) Create(context_ context.Context, path string, data []byte, acl []ACL, createMode CreateMode, autoRetry bool) (*CreateResponse, error) {
	path = self.NormalizePath(path)

	if acl == nil {
		acl = self.defaultACL
	}

	request := &CreateRequest{
		Path:  path,
		Data:  data,
		ACL:   acl,
		Flags: createMode,
	}

	var response *CreateResponse

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*CreateResponse)
	}

	if e := self.executeOperation(
		context_,
		OpCreate,
		request,
		reflect.TypeOf(CreateResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, e
	}

	return response, nil
}

func (self *Client) Delete(context_ context.Context, path string, version int32, autoRetry bool) error {
	path = self.NormalizePath(path)

	request := &DeleteRequest{
		Path:    path,
		Version: version,
	}

	callback := func(_ interface{}, _ ErrorCode) {}

	if e := self.executeOperation(
		context_,
		OpDelete,
		request,
		reflect.TypeOf(struct{}{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return e
	}

	return nil
}

func (self *Client) Exists(context_ context.Context, path string, autoRetry bool) (*ExistsResponse, error) {
	response, _, e := self.doExists(context_, path, false, autoRetry)
	return response, e
}

func (self *Client) ExistsW(context_ context.Context, path string, autoRetry bool) (*ExistsResponse, *Watcher, error) {
	return self.doExists(context_, path, true, autoRetry)
}

func (self *Client) executeOperation(
	context_ context.Context,
	opCode OpCode,
	request interface{},
	responseType reflect.Type,
	autoRetryOperation bool,
	tolerableErrorCode ErrorCode,
	callback func(interface{}, ErrorCode),
) error {
	error_ := make(chan error, 1)

	callbackWrapper := func(value interface{}, errorCode ErrorCode) {
		if errorCode != 0 && errorCode != tolerableErrorCode {
			error_ <- Error{errorCode, fmt.Sprintf("opCode=%#v, request=%#v", opCode, request)}
			return
		}

		callback(value, errorCode)
		error_ <- nil
	}

	if e := self.session.ExecuteOperation(
		context_,
		opCode,
		request,
		responseType,
		autoRetryOperation,
		callbackWrapper,
	); e != nil {
		return e
	}

	if context_ == nil {
		if e := <-error_; e != nil {
			return e
		}
	} else {
		select {
		case e := <-error_:
			if e != nil {
				return e
			}
		case <-context_.Done():
			return context_.Err()
		}
	}

	return nil
}

func (self *Client) doExists(context_ context.Context, path string, watch bool, autoRetry bool) (*ExistsResponse, *Watcher, error) {
	path = self.NormalizePath(path)

	request := &ExistsRequest{
		Path:  path,
		Watch: watch,
	}

	response := (*ExistsResponse)(nil)
	watcher := (*Watcher)(nil)

	callback := func(value interface{}, toleratedErrorCode ErrorCode) {
		if toleratedErrorCode == ErrorNoNode {
			if watch {
				watcher = self.session.AddWatcher(WatcherExist, path)
			}
		} else {
			response = value.(*ExistsResponse)

			if watch {
				watcher = self.session.AddWatcher(WatcherData, path)
			}
		}
	}

	if e := self.executeOperation(
		context_,
		OpExists,
		request,
		reflect.TypeOf(ExistsResponse{}),
		autoRetry,
		ErrorNoNode,
		callback,
	); e != nil {
		return nil, nil, e
	}

	return response, watcher, nil
}
