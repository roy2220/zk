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

func (self *Client) AddSessionListener(maxNumberOfSessionStateChanges int) (*SessionListener, error) {
	return self.session.AddListener(maxNumberOfSessionStateChanges)
}

func (self *Client) RemoveSessionListener(sessionListener *SessionListener) error {
	return self.session.RemoveListener(sessionListener)
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

func (self *Client) GetACL(context_ context.Context, path string, autoRetry bool) (*GetACLResponse, error) {
	path = self.NormalizePath(path)

	request := &GetACLRequest{
		Path: path,
	}

	var response *GetACLResponse

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*GetACLResponse)
	}

	if e := self.executeOperation(
		context_,
		OpGetACL,
		request,
		reflect.TypeOf(GetACLResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, e
	}

	return response, nil
}

func (self *Client) GetChildren(context_ context.Context, path string, autoRetry bool) (*GetChildrenResponse, error) {
	response, _, e := self.doGetChildren(context_, path, false, autoRetry)
	return response, e
}

func (self *Client) GetChildrenW(context_ context.Context, path string, autoRetry bool) (*GetChildrenResponse, *Watcher, error) {
	return self.doGetChildren(context_, path, true, autoRetry)
}

func (self *Client) GetChildren2(context_ context.Context, path string, autoRetry bool) (*GetChildren2Response, error) {
	response, _, e := self.doGetChildren2(context_, path, false, autoRetry)
	return response, e
}

func (self *Client) GetChildren2W(context_ context.Context, path string, autoRetry bool) (*GetChildren2Response, *Watcher, error) {
	return self.doGetChildren2(context_, path, true, autoRetry)
}

func (self *Client) GetData(context_ context.Context, path string, autoRetry bool) (*GetDataResponse, error) {
	response, _, e := self.doGetData(context_, path, false, autoRetry)
	return response, e
}

func (self *Client) GetDataW(context_ context.Context, path string, autoRetry bool) (*GetDataResponse, *Watcher, error) {
	return self.doGetData(context_, path, true, autoRetry)
}

func (self *Client) SetACL(context_ context.Context, path string, acl []ACL, version int32, autoRetry bool) (*SetACLResponse, error) {
	path = self.NormalizePath(path)

	if acl == nil {
		acl = self.defaultACL
	}

	request := &SetACLRequest{
		Path:    path,
		ACL:     acl,
		Version: version,
	}

	var response *SetACLResponse

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*SetACLResponse)
	}

	if e := self.executeOperation(
		context_,
		OpSetACL,
		request,
		reflect.TypeOf(SetACLResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, e
	}

	return response, nil
}

func (self *Client) SetData(context_ context.Context, path string, data []byte, version int32, autoRetry bool) (*SetDataResponse, error) {
	path = self.NormalizePath(path)

	request := &SetDataRequest{
		Path:    path,
		Data:    data,
		Version: version,
	}

	var response *SetDataResponse

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*SetDataResponse)
	}

	if e := self.executeOperation(
		context_,
		OpSetData,
		request,
		reflect.TypeOf(SetDataResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, e
	}

	return response, nil
}

func (self *Client) Sync(context_ context.Context, path string, autoRetry bool) (*SyncResponse, error) {
	path = self.NormalizePath(path)

	request := &SyncRequest{
		Path: path,
	}

	var response *SyncResponse

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*SyncResponse)
	}

	if e := self.executeOperation(
		context_,
		OpSync,
		request,
		reflect.TypeOf(SyncResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, e
	}

	return response, nil
}

func (self *Client) CreateOp(path string, data []byte, acl []ACL, createMode CreateMode) Op {
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

	return Op{OpCreate, request}
}

func (self *Client) DeleteOp(path string, version int32) Op {
	path = self.NormalizePath(path)

	request := &DeleteRequest{
		Path:    path,
		Version: version,
	}

	return Op{OpDelete, request}
}

func (self *Client) SetDataOp(path string, data []byte, version int32) Op {
	path = self.NormalizePath(path)

	request := &SetDataRequest{
		Path:    path,
		Data:    data,
		Version: version,
	}

	return Op{OpSetData, request}
}

func (self *Client) CheckOp(path string, version int32) Op {
	path = self.NormalizePath(path)

	request := &CheckVersionRequest{
		Path:    path,
		Version: version,
	}

	return Op{OpCheck, request}
}

func (self *Client) Multi(context_ context.Context, ops []Op, autoRetry bool) (*MultiResponse, error) {
	request := &MultiRequest{
		Ops: ops,
	}

	var response *MultiResponse

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*MultiResponse)
	}

	if e := self.executeOperation(
		context_,
		OpMulti,
		request,
		reflect.TypeOf(MultiResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, e
	}

	return response, nil
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

func (self *Client) doGetChildren(context_ context.Context, path string, watch bool, autoRetry bool) (*GetChildrenResponse, *Watcher, error) {
	path = self.NormalizePath(path)

	request := &GetChildrenRequest{
		Path:  path,
		Watch: watch,
	}

	var response *GetChildrenResponse
	watcher := (*Watcher)(nil)

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*GetChildrenResponse)

		if watch {
			watcher = self.session.AddWatcher(WatcherChild, path)
		}
	}

	if e := self.executeOperation(
		context_,
		OpGetChildren,
		request,
		reflect.TypeOf(GetChildrenResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, nil, e
	}

	return response, watcher, nil
}

func (self *Client) doGetChildren2(context_ context.Context, path string, watch bool, autoRetry bool) (*GetChildren2Response, *Watcher, error) {
	path = self.NormalizePath(path)

	request := &GetChildrenRequest{
		Path:  path,
		Watch: watch,
	}

	var response *GetChildren2Response
	watcher := (*Watcher)(nil)

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*GetChildren2Response)

		if watch {
			watcher = self.session.AddWatcher(WatcherChild, path)
		}
	}

	if e := self.executeOperation(
		context_,
		OpGetChildren2,
		request,
		reflect.TypeOf(GetChildren2Response{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, nil, e
	}

	return response, watcher, nil
}

func (self *Client) doGetData(context_ context.Context, path string, watch bool, autoRetry bool) (*GetDataResponse, *Watcher, error) {
	path = self.NormalizePath(path)

	request := &GetDataRequest{
		Path:  path,
		Watch: watch,
	}

	var response *GetDataResponse
	watcher := (*Watcher)(nil)

	callback := func(value interface{}, _ ErrorCode) {
		response = value.(*GetDataResponse)

		if watch {
			watcher = self.session.AddWatcher(WatcherData, path)
		}
	}

	if e := self.executeOperation(
		context_,
		OpGetData,
		request,
		reflect.TypeOf(GetDataResponse{}),
		autoRetry,
		0,
		callback,
	); e != nil {
		return nil, nil, e
	}

	return response, watcher, nil
}
