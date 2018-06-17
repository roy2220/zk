package zk

import (
	"fmt"
)

const (
	OpNotification    OpCode = 0
	OpCreate          OpCode = 1
	OpDelete          OpCode = 2
	OpExists          OpCode = 3
	OpGetData         OpCode = 4
	OpSetData         OpCode = 5
	OpGetACL          OpCode = 6
	OpSetACL          OpCode = 7
	OpGetChildren     OpCode = 8
	OpSync            OpCode = 9
	OpPing            OpCode = 11
	OpGetChildren2    OpCode = 12
	OpCheck           OpCode = 13
	OpMulti           OpCode = 14
	OpCreate2         OpCode = 15
	OpReconfig        OpCode = 16
	OpCheckWatches    OpCode = 17
	OpRemoveWatches   OpCode = 18
	OpCreateContainer OpCode = 19
	OpDeleteContainer OpCode = 20
	OpCreateTTL       OpCode = 21
	OpAuth            OpCode = 100
	OpSetWatches      OpCode = 101
	OpSASL            OpCode = 102
	OpCreateSession   OpCode = -10
	OpCloseSession    OpCode = -11
	OpError           OpCode = -1
)

const (
	PermsRead   Perms = 1 << 0
	PermsWrite  Perms = 1 << 1
	PermsCreate Perms = 1 << 2
	PermsDelete Perms = 1 << 3
	PermsAdmin  Perms = 1 << 4
	PermsAll    Perms = PermsRead | PermsWrite | PermsCreate | PermsDelete | PermsAdmin
)

const (
	CreatePersistent           CreateMode = 0
	CreateEphemeral            CreateMode = 1
	CreatePersistentSequential CreateMode = 2
	CreateEphemeralSequential  CreateMode = 3
)

const (
	WatcherEventNone                WatcherEventType = 0
	WatcherEventNodeCreated         WatcherEventType = 1
	WatcherEventNodeDeleted         WatcherEventType = 2
	WatcherEventNodeDataChanged     WatcherEventType = 3
	WatcherEventNodeChildrenChanged WatcherEventType = 4
)

type OpCode int32

func (self OpCode) GoString() string {
	switch self {
	case OpNotification:
		return "<OpNotification>"
	case OpCreate:
		return "<OpCreate>"
	case OpDelete:
		return "<OpDelete>"
	case OpExists:
		return "<OpExists>"
	case OpGetData:
		return "<OpGetData>"
	case OpSetData:
		return "<OpSetData>"
	case OpGetACL:
		return "<OpGetACL>"
	case OpSetACL:
		return "<OpSetACL>"
	case OpGetChildren:
		return "<OpGetChildren>"
	case OpSync:
		return "<OpSync>"
	case OpPing:
		return "<OpPing>"
	case OpGetChildren2:
		return "<OpGetChildren2>"
	case OpCheck:
		return "<OpCheck>"
	case OpMulti:
		return "<OpMulti>"
	case OpCreate2:
		return "<OpCreate2>"
	case OpReconfig:
		return "<OpReconfig>"
	case OpCheckWatches:
		return "<OpCheckWatches>"
	case OpRemoveWatches:
		return "<OpRemoveWatches>"
	case OpCreateContainer:
		return "<OpCreateContainer>"
	case OpDeleteContainer:
		return "<OpDeleteContainer>"
	case OpCreateTTL:
		return "<OpCreateTTL>"
	case OpAuth:
		return "<OpAuth>"
	case OpSetWatches:
		return "<OpSetWatches>"
	case OpSASL:
		return "<OpSASL>"
	case OpCreateSession:
		return "<OpCreateSession>"
	case OpCloseSession:
		return "<OpCloseSession>"
	case OpError:
		return "<OpError>"
	default:
		return fmt.Sprintf("<OpCode:%d>", self)
	}
}

type Perms int32

func (self Perms) GoString() string {
	result := "<"

	if self&PermsRead != 0 {
		result += "PermsRead|"
		self &^= PermsRead
	}

	if self&PermsWrite != 0 {
		result += "PermsWrite|"
		self &^= PermsWrite
	}

	if self&PermsCreate != 0 {
		result += "PermsCreate|"
		self &^= PermsCreate
	}

	if self&PermsDelete != 0 {
		result += "PermsDelete|"
		self &^= PermsDelete
	}

	if self&PermsAdmin != 0 {
		result += "PermsAdmin|"
		self &^= PermsAdmin
	}

	if self != 0 {
		result += fmt.Sprintf("Perms:%d|", self)
	}

	result = result[:len(result)-1] + ">"
	return result
}

type CreateMode int32

func (self CreateMode) GoString() string {
	switch self {
	case CreatePersistent:
		return "<CreatePersistent>"
	case CreateEphemeral:
		return "<CreateEphemeral>"
	case CreatePersistentSequential:
		return "<CreatePersistentSequential>"
	case CreateEphemeralSequential:
		return "<CreateEphemeralSequential>"
	default:
		return fmt.Sprintf("<CreateMode:%d>", self)
	}
}

type WatcherEventType int32

func (self WatcherEventType) GoString() string {
	switch self {
	case WatcherEventNone:
		return "<WatcherEventNone>"
	case WatcherEventNodeCreated:
		return "<WatcherEventNodeCreated>"
	case WatcherEventNodeDeleted:
		return "<WatcherEventNodeDeleted>"
	case WatcherEventNodeDataChanged:
		return "<WatcherEventNodeDataChanged>"
	case WatcherEventNodeChildrenChanged:
		return "<WatcherEventNodeChildrenChanged>"
	default:
		return fmt.Sprintf("<WatcherEventType:%d>", self)
	}
}

type Id struct {
	Scheme string
	Id     string
}

type ACL struct {
	Perms Perms
	Id    Id
}

type Stat struct {
	CZxid          int64
	MZxid          int64
	CTime          int64
	MTime          int64
	Version        int32
	CVersion       int32
	AVersion       int32
	EphemeralOwner int64
	DataLength     int32
	NumChildren    int32
	PZxid          int64
}

type AuthPacket struct {
	Type   int32
	Scheme string
	Auth   []byte
}

type CreateRequest struct {
	Path  string
	Data  []byte
	ACL   []ACL
	Flags CreateMode
}

type CreateResponse struct {
	Path string
}

type DeleteRequest struct {
	Path    string
	Version int32
}

type ExistsRequest struct {
	Path  string
	Watch bool
}

type ExistsResponse struct {
	Stat Stat
}

type GetDataRequest struct {
	Path  string
	Watch bool
}

type GetDataResponse struct {
	Data []byte
	Stat Stat
}

var AnyoneIdUnsafe = Id{"world", "anyone"}
var AuthIds = Id{"auth", ""}
var OpenACLUnsafe = ACL{PermsAll, AnyoneIdUnsafe}
var CreatorAllACL = ACL{PermsAll, AuthIds}
var ReadACLUnsafe = ACL{PermsRead, AnyoneIdUnsafe}

type connectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    int64
	TimeOut         int32
	SessionId       int64
	Passwd          []byte
}

type connectResponse struct {
	ProtocolVersion int32
	TimeOut         int32
	SessionId       int64
	Passwd          []byte
}

type requestHeader struct {
	Xid  int32
	Type OpCode
}

type replyHeader struct {
	Xid  int32
	Zxid int64
	Err  ErrorCode
}

type watcherEvent struct {
	Type  WatcherEventType
	State int32
	Path  string
}

type setWatches struct {
	RelativeZxid int64
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}
