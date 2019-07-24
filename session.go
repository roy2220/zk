package zk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/let-z-go/intrusive_containers/list"
	"github.com/let-z-go/toolkit/byte_stream"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/logger"
	"github.com/let-z-go/toolkit/semaphore"
)

const (
	SessionEventNo SessionEventType = iota
	SessionEventExpired
	SessionEventDisconnected
	SessionEventConnected
	SessionEventAuthFailed
)

const (
	SessionNo SessionState = iota
	SessionNotConnected
	SessionConnecting // by event: SessionEventNo, SessionEventDisconnected
	SessionConnected  // by event: SessionEventConnected
	SessionClosed     // by event: SessionEventNo, SessionEventExpired
	SessionAuthFailed // by event: SessionEventAuthFailed
)

const (
	WatcherData WatcherType = iota
	WatcherExist
	WatcherChild
)

type SessionPolicy struct {
	Logger                       logger.Logger
	Timeout                      time.Duration
	MaxNumberOfPendingOperations int32
	Transport                    *TransportPolicy

	validateOnce sync.Once
}

func (self *SessionPolicy) Validate() *SessionPolicy {
	self.validateOnce.Do(func() {
		if self.Timeout == 0 {
			self.Timeout = defaultSessionTimeout
		} else {
			if self.Timeout < minSessionTimeout {
				self.Timeout = minSessionTimeout
			} else if self.Timeout > maxSessionTimeout {
				self.Timeout = maxSessionTimeout
			}
		}

		if self.MaxNumberOfPendingOperations == 0 {
			self.MaxNumberOfPendingOperations = defaultMaxNumberOfPendingOperations
		} else {
			if self.MaxNumberOfPendingOperations < minMaxNumberOfPendingOperations {
				self.MaxNumberOfPendingOperations = minMaxNumberOfPendingOperations
			} else if self.MaxNumberOfPendingOperations > maxMaxNumberOfPendingOperations {
				self.MaxNumberOfPendingOperations = maxMaxNumberOfPendingOperations
			}
		}

		if self.Transport == nil {
			self.Transport = &defaultTransportPolicy
		}
	})

	return self
}

type SessionListener struct {
	stateChanges chan SessionStateChange
}

func (self *SessionListener) StateChanges() <-chan SessionStateChange {
	return self.stateChanges
}

type SessionStateChange struct {
	EventType SessionEventType
	State     SessionState
}

type SessionEventType uint8

func (self SessionEventType) GoString() string {
	switch self {
	case SessionEventNo:
		return "<SessionEventNo>"
	case SessionEventExpired:
		return "<SessionEventExpired>"
	case SessionEventDisconnected:
		return "<SessionEventDisconnected>"
	case SessionEventConnected:
		return "<SessionEventConnected>"
	case SessionEventAuthFailed:
		return "<SessionEventAuthFailed>"
	default:
		return fmt.Sprintf("<SessionEventType:%d>", self)
	}
}

type SessionState uint8

func (self SessionState) GoString() string {
	switch self {
	case SessionNo:
		return "<SessionNo>"
	case SessionNotConnected:
		return "<SessionNotConnected>"
	case SessionConnecting:
		return "<SessionConnecting>"
	case SessionConnected:
		return "<SessionConnected>"
	case SessionClosed:
		return "<SessionClosed>"
	case SessionAuthFailed:
		return "<SessionAuthFailed>"
	default:
		return fmt.Sprintf("<SessionState:%d>", self)
	}
}

type Watcher struct {
	type_     WatcherType
	path      string
	event     chan WatcherEvent
	isRemoved int32
}

func (self *Watcher) GetType() WatcherType {
	return self.type_
}

func (self *Watcher) GetPath() string {
	return self.path
}

func (self *Watcher) Event() <-chan WatcherEvent {
	return self.event
}

func (self *Watcher) Remove() error {
	if !atomic.CompareAndSwapInt32(&self.isRemoved, 0, -1) {
		return WatcherRemovedError
	}

	close(self.event)
	return nil
}

func (self *Watcher) IsRemoved() bool {
	return atomic.LoadInt32(&self.isRemoved) != 0
}

func (self *Watcher) fireEvent(event WatcherEvent) error {
	if !atomic.CompareAndSwapInt32(&self.isRemoved, 0, -1) {
		return WatcherRemovedError
	}

	self.event <- event
	close(self.event)
	return nil
}

type WatcherType uint8

func (self WatcherType) GoString() string {
	switch self {
	case WatcherData:
		return "<WatcherData>"
	case WatcherExist:
		return "<WatcherExist>"
	case WatcherChild:
		return "<WatcherChild>"
	default:
		return fmt.Sprintf("<WatcherType:%d>", self)
	}
}

type WatcherEvent struct {
	Type  WatcherEventType
	Error error
}

type AuthInfo struct {
	Scheme string
	Auth   []byte
}

var WatcherRemovedError = errors.New("zk: watcher removed")
var SessionClosedError = errors.New("zk: session closed")

const defaultSessionTimeout = 6 * time.Second
const minSessionTimeout = 4 * time.Second
const maxSessionTimeout = 40 * time.Second
const defaultMaxNumberOfPendingOperations = 1 << 10
const minMaxNumberOfPendingOperations = 1 << 4
const maxMaxNumberOfPendingOperations = 1 << 16
const protocolVersion = 0
const maxSetWatchesSize = 1 << 17
const setWatchesOverheadSize = 28
const stringOverheadSize = 4
const closeTimeoutOfSession = 200 * time.Millisecond

type session struct {
	policy            *SessionPolicy
	state             int32
	lockOfListeners   sync.Mutex
	listeners         map[*SessionListener]struct{}
	lastZxid          int64
	timeout           time.Duration
	id                int64
	password          []byte
	lastXid           int32
	transport         transport
	dequeOfOperations deque.Deque
	pendingOperations sync.Map
	watchers          [3]map[string]map[*Watcher]struct{}
}

func (self *session) Initialize(policy *SessionPolicy) *session {
	if self.state != 0 {
		panic(errors.New("zk: session already initialized"))
	}

	self.policy = policy.Validate()
	self.state = int32(SessionNotConnected)
	self.timeout = policy.Timeout
	self.password = nil
	self.dequeOfOperations.Initialize(policy.MaxNumberOfPendingOperations)

	for i := range self.watchers {
		self.watchers[i] = map[string]map[*Watcher]struct{}{}
	}

	return self
}

func (self *session) Close() {
	self.setState(SessionEventNo, SessionClosed)
}

func (self *session) AddListener(maxNumberOfStateChanges int) (*SessionListener, error) {
	if self.IsClosed() {
		return nil, SessionClosedError
	}

	self.lockOfListeners.Lock()

	if self.IsClosed() {
		self.lockOfListeners.Unlock()
		return nil, SessionClosedError
	}

	listener := &SessionListener{
		stateChanges: make(chan SessionStateChange, maxNumberOfStateChanges),
	}

	if self.listeners == nil {
		self.listeners = map[*SessionListener]struct{}{}
	}

	self.listeners[listener] = struct{}{}
	self.lockOfListeners.Unlock()
	return listener, nil
}

func (self *session) RemoveListener(listener *SessionListener) error {
	if self.IsClosed() {
		return SessionClosedError
	}

	close(listener.stateChanges)
	self.lockOfListeners.Lock()

	if self.IsClosed() {
		self.lockOfListeners.Unlock()
		return SessionClosedError
	}

	delete(self.listeners, listener)

	if len(self.listeners) == 0 {
		self.listeners = nil
	}

	self.lockOfListeners.Unlock()
	return nil
}

func (self *session) Connect(context_ context.Context, serverAddress string, authInfos []AuthInfo) error {
	if self.id == 0 {
		self.policy.Logger.Infof("session connection: serverAddress=%#v", serverAddress)
	} else {
		self.policy.Logger.Infof("session connection: sessionID=%#x, serverAddress=%#v", self.id, serverAddress)
	}

	var eventType SessionEventType

	if self.getState() == SessionNotConnected {
		eventType = SessionEventNo
	} else {
		eventType = SessionEventDisconnected
	}

	self.setState(eventType, SessionConnecting)

	if e := self.connectTransport(context_, serverAddress, func(transport_ *transport) error {
		if e := self.doConnect(context_, transport_, func() error {
			if e := self.authenticate(context_, transport_, authInfos); e != nil {
				return e
			}

			if e := self.rewatch(context_, transport_); e != nil {
				return e
			}

			return nil
		}); e != nil {
			return e
		}

		return nil
	}); e != nil {
		return e
	}

	self.setState(SessionEventConnected, SessionConnected)
	self.policy.Logger.Infof("session establishment: serverAddress=%#v, sessionID=%#x, timeout=%#v", serverAddress, self.id, self.timeout/time.Millisecond)
	return nil
}

func (self *session) Dispatch(context_ context.Context, cancel context.CancelFunc) error {
	if state := self.getState(); state != SessionConnected {
		panic(&invalidSessionStateError{fmt.Sprintf("state=%#v", state)})
	}

	error_ := make(chan error, 1)

	go func() {
		error_ <- self.sendRequests(context_, cancel)
	}()

	e := self.receiveResponses(context_)
	cancel()

	if e2 := <-error_; e2 != context_.Err() {
		e = e2
	}

	return e
}

func (self *session) ExecuteOperation(
	context_ context.Context,
	opCode OpCode,
	request interface{},
	responseType reflect.Type,
	autoRetryOperation bool,
	callback func(interface{}, ErrorCode),
) error {
	if self.IsClosed() {
		return &Error{self.getErrorCode(), fmt.Sprintf("opCode=%#v, request=%#v", opCode, request)}
	}

	operation_ := operation{
		OpCode:       opCode,
		Request:      request,
		ResponseType: responseType,
		AutoRetry:    autoRetryOperation,
		Callback:     callback,
	}

	if e := self.dequeOfOperations.AppendNode(context_, &operation_.ListNode); e != nil {
		if e == semaphore.SemaphoreClosedError {
			e = &Error{self.getErrorCode(), fmt.Sprintf("opCode=%#v, request=%#v", opCode, request)}
		}

		return e
	}

	return nil
}

func (self *session) AddWatcher(watcherType WatcherType, path string) *Watcher {
	watcher := &Watcher{
		type_: watcherType,
		path:  path,
		event: make(chan WatcherEvent, 1),
	}

	path2Watchers := self.watchers[watcherType]
	watchers := path2Watchers[path]

	if watchers == nil {
		watchers = map[*Watcher]struct{}{}
		path2Watchers[path] = watchers
	}

	watchers[watcher] = struct{}{}
	return watcher
}

func (self *session) IsClosed() bool {
	return self.getErrorCode() != 0
}

func (self *session) GetTimeout() time.Duration {
	return self.timeout
}

func (self *session) setState(eventType SessionEventType, newState SessionState) {
	oldState := self.getState()
	errorCode := ErrorCode(0)

	switch oldState {
	case SessionNotConnected:
		switch newState {
		case SessionConnecting:
		default:
			panic(&invalidSessionStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case SessionConnecting:
		switch newState {
		case SessionConnecting:
			return
		case SessionConnected:
		case SessionClosed:
			if eventType == SessionEventExpired {
				errorCode = ErrorSessionExpired
			} else {
				errorCode = ErrorConnectionLoss
			}
		case SessionAuthFailed:
			errorCode = ErrorAuthFailed
		default:
			panic(&invalidSessionStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case SessionConnected:
		switch newState {
		case SessionConnecting:
			errorCode = ErrorConnectionLoss
		case SessionClosed:
			errorCode = ErrorConnectionLoss
		default:
			panic(&invalidSessionStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})

		}
	default:
		panic(&invalidSessionStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
	}

	atomic.StoreInt32(&self.state, int32(newState))
	self.policy.Logger.Infof("session state change: sessionID=%#x, eventType=%#v, oldState=%#v, newState=%#v", self.id, eventType, oldState, newState)
	self.lockOfListeners.Lock()

	for listener := range self.listeners {
		listener.stateChanges <- SessionStateChange{eventType, newState}
	}

	self.lockOfListeners.Unlock()

	if errorCode != 0 {
		operationsAreRetriable := errorCode == ErrorConnectionLoss

		if errorCode2 := self.getErrorCode(); errorCode2 == 0 {
			list_ := (&list.List{}).Initialize()
			retriedOperationCount := int32(0)
			completedOperationCount := int32(0)

			self.pendingOperations.Range(func(key interface{}, value interface{}) bool {
				self.pendingOperations.Delete(key)
				operation_ := value.(*operation)

				if operationsAreRetriable && operation_.AutoRetry {
					list_.AppendNode(&operation_.ListNode)
					retriedOperationCount++
				} else {
					operation_.Callback(nil, errorCode)
					completedOperationCount++
				}

				return true
			})

			self.dequeOfOperations.DiscardNodeRemovals(list_, retriedOperationCount)
			self.dequeOfOperations.CommitNodeRemovals(completedOperationCount)
		} else {
			self.policy = nil

			{
				self.lockOfListeners.Lock()
				listeners := self.listeners
				self.listeners = nil
				self.lockOfListeners.Unlock()

				for listener := range listeners {
					close(listener.stateChanges)
				}
			}

			self.password = nil

			if !self.transport.IsClosed() {
				if self.id != 0 {
					self.doClose(&self.transport)
				}

				self.transport.Close()
			}

			{
				list_ := (&list.List{}).Initialize()
				self.dequeOfOperations.Close(list_)
				getListNode := list_.GetNodes()

				for listNode := getListNode(); listNode != nil; listNode = getListNode() {
					operation_ := (*operation)(listNode.GetContainer(unsafe.Offsetof(operation{}.ListNode)))
					operation_.Callback(nil, errorCode2)
				}
			}

			self.pendingOperations.Range(func(key interface{}, value interface{}) bool {
				self.pendingOperations.Delete(key)
				operation_ := value.(*operation)

				if operationsAreRetriable && operation_.AutoRetry {
					operation_.Callback(nil, errorCode2)
				} else {
					operation_.Callback(nil, errorCode)
				}

				return true
			})

			{
				watcherEvent := WatcherEvent{0, &Error{errorCode2, ""}}

				for watcherType, path2Watchers := range self.watchers {
					self.watchers[watcherType] = nil

					for _, watchers := range path2Watchers {
						for watcher := range watchers {
							watcher.fireEvent(watcherEvent)
						}
					}
				}
			}
		}
	}
}

func (self *session) connectTransport(context_ context.Context, serverAddress string, callback func(*transport) error) error {
	var transport_ transport

	if e := transport_.Connect(context_, self.policy.Transport, serverAddress); e != nil {
		return e
	}

	if e := callback(&transport_); e != nil {
		transport_.Close()
		return e
	}

	self.transport.Close()
	self.transport = transport_
	return nil
}

func (self *session) doConnect(context_ context.Context, transport_ *transport, callback func() error) error {
	request := connectRequest{
		ProtocolVersion: protocolVersion,
		LastZxidSeen:    self.lastZxid,
		TimeOut:         int32(self.policy.Timeout / time.Millisecond),
		SessionId:       self.id,
		Passwd:          self.password,
	}

	if e := transport_.Write(func(byteStream *byte_stream.ByteStream) error {
		serializeRecord(&request, byteStream)
		return nil
	}); e != nil {
		return e
	}

	if e := transport_.Flush(context_, minSessionTimeout); e != nil {
		return e
	}

	data, e := transport_.Peek(context_, minSessionTimeout)

	if e != nil {
		return e
	}

	var response connectResponse
	dataOffset := 0

	if e := deserializeRecord(&response, data, &dataOffset); e != nil {
		return e
	}

	if e := transport_.Skip(data); e != nil {
		return e
	}

	if response.TimeOut < 1 {
		self.setState(SessionEventExpired, SessionClosed)
		return &Error{ErrorSessionExpired, ""}
	}

	if e := callback(); e != nil {
		if self.id == 0 {
			self.doClose(transport_)
		}

		return e
	}

	self.timeout = time.Duration(response.TimeOut) * time.Millisecond
	self.id = response.SessionId
	self.password = response.Passwd
	return nil
}

func (self *session) doClose(transport_ *transport) error {
	requestHeader_ := requestHeader{
		Xid:  0,
		Type: OpCloseSession,
	}

	if e := transport_.Write(func(byteStream *byte_stream.ByteStream) error {
		serializeRecord(&requestHeader_, byteStream)
		return nil
	}); e != nil {
		return e
	}

	if e := transport_.Flush(context.Background(), closeTimeoutOfSession); e != nil {
		return e
	}

	return nil
}

func (self *session) authenticate(context_ context.Context, transport_ *transport, authInfos []AuthInfo) error {
	for i := range authInfos {
		authInfo := &authInfos[i]

		request := authPacket{
			Type:   0,
			Scheme: authInfo.Scheme,
			Auth:   authInfo.Auth,
		}

		if _, e := self.executeOperationSync(
			context_,
			transport_,
			0,
			OpAuth,
			&request,
			reflect.TypeOf(struct{}{}),
		); e != nil {
			if e2, ok := e.(*Error); ok && e2.code == ErrorAuthFailed {
				self.setState(SessionEventAuthFailed, SessionAuthFailed)
			}

			return e
		}
	}

	return nil
}

func (self *session) rewatch(context_ context.Context, transport_ *transport) error {
	requests := []setWatches(nil)
	paths := [...][]string{nil, nil, nil}
	requestSize := setWatchesOverheadSize

	for watcherType, path2Watchers := range self.watchers {
		for path, watchers := range path2Watchers {
			for watcher := range watchers {
				if watcher.IsRemoved() {
					delete(watchers, watcher)
				}
			}

			if len(watchers) == 0 {
				delete(path2Watchers, path)
				continue
			}

			pathSize := stringOverheadSize + utf8.RuneCountInString(path)

			if requestSize+pathSize > maxSetWatchesSize {
				requests = append(requests, setWatches{
					RelativeZxid: self.lastZxid,
					DataWatches:  paths[WatcherData],
					ExistWatches: paths[WatcherExist],
					ChildWatches: paths[WatcherChild],
				})

				paths = [...][]string{nil, nil, nil}
				requestSize = setWatchesOverheadSize
			}

			paths[watcherType] = append(paths[watcherType], path)
			requestSize += pathSize
		}
	}

	if requestSize > setWatchesOverheadSize {
		requests = append(requests, setWatches{
			RelativeZxid: self.lastZxid,
			DataWatches:  paths[WatcherData],
			ExistWatches: paths[WatcherExist],
			ChildWatches: paths[WatcherChild],
		})
	}

	for i := range requests {
		request := &requests[i]

		if _, e := self.executeOperationSync(
			context_,
			transport_,
			-8,
			OpSetWatches,
			request,
			reflect.TypeOf(struct{}{}),
		); e != nil {
			return e
		}
	}

	return nil
}

func (self *session) executeOperationSync(
	context_ context.Context,
	transport_ *transport,
	xid int32,
	opCode OpCode,
	request interface{},
	responseType reflect.Type,
) (interface{}, error) {
	requestHeader_ := requestHeader{
		Xid:  xid,
		Type: opCode,
	}

	if e := transport_.Write(func(byteStream *byte_stream.ByteStream) error {
		serializeRecord(&requestHeader_, byteStream)
		serializeRecord(request, byteStream)
		return nil
	}); e != nil {
		return nil, e
	}

	if e := transport_.Flush(context_, minSessionTimeout); e != nil {
		return nil, e
	}

	var replyHeader_ replyHeader

	for {
		data, e := transport_.Peek(context_, minSessionTimeout)

		if e != nil {
			return nil, e
		}

		dataOffset := 0

		if e := deserializeRecord(&replyHeader_, data, &dataOffset); e != nil {
			return nil, e
		}

		if replyHeader_.Zxid >= 1 {
			self.lastZxid = replyHeader_.Zxid
		}

		if replyHeader_.Xid != xid {
			switch replyHeader_.Xid {
			case -1: // -1 means notification
				var watcherEvent_ watcherEvent

				if e := deserializeRecord(&watcherEvent_, data, &dataOffset); e != nil {
					return nil, e
				}

				self.fireWatcherEvent(watcherEvent_.Type, watcherEvent_.Path)
			case -2: // -2 is the xid for pings
			default:
				self.policy.Logger.Warningf("ignored reply: sessionID=%#x, replyHeader=%#v", self.id, replyHeader_)
			}

			if e := transport_.Skip(data); e != nil {
				return nil, e
			}

			continue
		}

		if replyHeader_.Err != 0 {
			return nil, &Error{replyHeader_.Err, fmt.Sprintf("opCode=%#v, request=%#v", opCode, request)}
		}

		response := reflect.New(responseType).Interface()

		if e := deserializeRecord(response, data, &dataOffset); e != nil {
			return nil, e
		}

		if e := transport_.Skip(data); e != nil {
			return nil, e
		}

		return response, nil
	}
}

func (self *session) fireWatcherEvent(watcherEventType WatcherEventType, path string) {
	watcherEvent := WatcherEvent{watcherEventType, nil}
	watcherTypes := watcherEventType2WatcherTypes[watcherEventType]
	watcherCount := 0

	for _, watcherType := range watcherTypes {
		path2Watchers := self.watchers[watcherType]
		watchers, ok := path2Watchers[path]

		if !ok {
			continue
		}

		delete(path2Watchers, path)

		for watcher := range watchers {
			watcher.fireEvent(watcherEvent)
		}

		watcherCount += len(watchers)
	}

	if watcherCount == 0 {
		self.policy.Logger.Warningf("missing watchers: sessionID=%#x, watcherEventType=%#v, path=%#v", self.id, watcherEventType, path)
	}
}

func (self *session) sendRequests(context_ context.Context, cancel context.CancelFunc) error {
	error_ := make(chan error, 2)

	type Task struct {
		List              *list.List
		NumberOfListNodes int32
	}

	tasks := make(chan Task)

	go func() {
		list1 := (&list.List{}).Initialize()
		list2 := (&list.List{}).Initialize()

		for {
			numberOfListNodes, e := self.dequeOfOperations.RemoveAllNodes(context_, false, list1)

			if e != nil {
				error_ <- e
				return
			}

			select {
			case tasks <- Task{list1, numberOfListNodes}:
			case <-context_.Done():
				self.dequeOfOperations.DiscardNodeRemovals(list1, numberOfListNodes)
				error_ <- context_.Err()
				return
			}

			list1, list2 = list2, list1
			list1.Initialize()
		}
	}()

	var task Task

	cleanup := func(ok bool) {
		if task.List != nil {
			if ok {
				getListNode := task.List.GetNodesSafely()

				for listNode := getListNode(); listNode != nil; listNode = getListNode() {
					listNode.Reset()
					operation_ := (*operation)(listNode.GetContainer(unsafe.Offsetof(operation{}.ListNode)))

					if !operation_.AutoRetry {
						operation_.Request = nil
					}

					self.pendingOperations.Store(operation_.Xid, operation_)
				}
			} else {
				self.dequeOfOperations.DiscardNodeRemovals(task.List, task.NumberOfListNodes)
			}
		}

		if !ok {
			cancel()
			<-error_
		}
	}

	for {
		task = Task{nil, 0}

		select {
		case e := <-error_:
			return e
		case task = <-tasks:
		case <-time.After(self.getMinPingInterval()):
			select {
			case task = <-tasks:
			default:
			}
		}

		if task.List == nil {
			requestHeader_ := requestHeader{
				Xid:  -2,
				Type: OpPing,
			}

			if e := self.transport.Write(func(byteStream *byte_stream.ByteStream) error {
				serializeRecord(&requestHeader_, byteStream)
				return nil
			}); e != nil {
				cleanup(false)
				return e
			}
		} else {
			getListNode := task.List.GetNodes()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				operation_ := (*operation)(listNode.GetContainer(unsafe.Offsetof(operation{}.ListNode)))
				operation_.Xid = self.getXid()

				requestHeader_ := requestHeader{
					Xid:  operation_.Xid,
					Type: operation_.OpCode,
				}

				if e := self.transport.Write(func(byteStream *byte_stream.ByteStream) error {
					serializeRecord(&requestHeader_, byteStream)
					serializeRecord(operation_.Request, byteStream)
					return nil
				}); e != nil {
					cleanup(false)
					return e
				}
			}
		}

		cleanup(true)

		if e := self.transport.Flush(context_, 0); e != nil {
			cancel()
			<-error_
			return e
		}
	}
}

func (self *session) receiveResponses(context_ context.Context) error {
	var replyHeader_ replyHeader

	for {
		data, e := self.transport.PeekInBatch(context_, 2*self.getMinPingInterval())

		if e != nil {
			return e
		}

		completedOperationCount := int32(0)

		for _, data2 := range data {
			dataOffset := 0

			if e := deserializeRecord(&replyHeader_, data2, &dataOffset); e != nil {
				return e
			}

			if replyHeader_.Zxid >= 1 {
				self.lastZxid = replyHeader_.Zxid
			}

			if value, ok := self.pendingOperations.Load(replyHeader_.Xid); ok {
				self.pendingOperations.Delete(replyHeader_.Xid)
				operation_ := value.(*operation)

				if replyHeader_.Err == 0 {
					response := reflect.New(operation_.ResponseType).Interface()

					if e := deserializeRecord(response, data2, &dataOffset); e != nil {
						return e
					}

					if extraDataSize := len(data2) - dataOffset; extraDataSize >= 1 {
						self.policy.Logger.Warningf("extra data of response: sessionID=%#x, responseType=%v, extraDataSize=%#v", self.id, operation_.ResponseType, extraDataSize)
					}

					operation_.Callback(response, 0)
				} else {
					operation_.Callback(nil, replyHeader_.Err)
				}

				completedOperationCount++
			} else {
				switch replyHeader_.Xid {
				case -1: // -1 means notification
					var watcherEvent_ watcherEvent

					if e := deserializeRecord(&watcherEvent_, data2, &dataOffset); e != nil {
						return e
					}

					if extraDataSize := len(data2) - dataOffset; extraDataSize >= 1 {
						self.policy.Logger.Warningf("extra data of watcher event: sessionID=%#x, extraDataSize=%#v", self.id, extraDataSize)
					}

					self.fireWatcherEvent(watcherEvent_.Type, watcherEvent_.Path)
				case -2: // -2 is the xid for pings
				default:
					self.policy.Logger.Warningf("ignored reply: sessionID=%#x, replyHeader=%#v", self.id, replyHeader_)
				}
			}
		}

		self.dequeOfOperations.CommitNodeRemovals(completedOperationCount)

		if e := self.transport.SkipInBatch(data); e != nil {
			return e
		}
	}
}

func (self *session) getErrorCode() ErrorCode {
	return sessionState2ErrorCode[self.getState()]
}

func (self *session) getState() SessionState {
	return SessionState(atomic.LoadInt32(&self.state))
}

func (self *session) getMinPingInterval() time.Duration {
	return self.timeout / 3
}

func (self *session) getXid() int32 {
	self.lastXid = int32((uint32(self.lastXid) + 1) & 0xFFFFFFF)
	return self.lastXid
}

type operation struct {
	ListNode     list.ListNode
	Xid          int32
	OpCode       OpCode
	Request      interface{}
	ResponseType reflect.Type
	AutoRetry    bool
	Callback     func(interface{}, ErrorCode)
}

type invalidSessionStateError struct {
	context string
}

func (self invalidSessionStateError) Error() string {
	result := "zk: invalid session state"

	if self.context != "" {
		result += ": " + self.context
	}

	return result
}

var defaultTransportPolicy TransportPolicy

var watcherEventType2WatcherTypes = [...][]WatcherType{
	WatcherEventNone:                nil,
	WatcherEventNodeCreated:         []WatcherType{WatcherExist},
	WatcherEventNodeDeleted:         []WatcherType{WatcherData, WatcherChild},
	WatcherEventNodeDataChanged:     []WatcherType{WatcherData},
	WatcherEventNodeChildrenChanged: []WatcherType{WatcherChild},
}

var sessionState2ErrorCode = [...]ErrorCode{
	SessionNo:           ErrorSessionExpired,
	SessionNotConnected: 0,
	SessionConnecting:   0,
	SessionConnected:    0,
	SessionClosed:       ErrorSessionExpired,
	SessionAuthFailed:   ErrorAuthFailed,
}
