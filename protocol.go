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

	if self&PermsRead == PermsRead {
		result += "PermsRead|"
		self &^= PermsRead
	}

	if self&PermsWrite == PermsWrite {
		result += "PermsWrite|"
		self &^= PermsWrite
	}

	if self&PermsCreate == PermsCreate {
		result += "PermsCreate|"
		self &^= PermsCreate
	}

	if self&PermsDelete == PermsDelete {
		result += "PermsDelete|"
		self &^= PermsDelete
	}

	if self&PermsAdmin == PermsAdmin {
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

type GetACLRequest struct {
	Path string
}

type GetACLResponse struct {
	ACL  []ACL
	Stat Stat
}

type GetChildrenRequest struct {
	Path  string
	Watch bool
}

type GetChildrenResponse struct {
	Children []string
}

type GetChildren2Response struct {
	Children []string
	Stat     Stat
}

type GetDataRequest struct {
	Path  string
	Watch bool
}

type GetDataResponse struct {
	Data []byte
	Stat Stat
}

type SetACLRequest struct {
	Path    string
	ACL     []ACL
	Version int32
}

type SetACLResponse struct {
	Stat Stat
}

type SetDataRequest struct {
	Path    string
	Data    []byte
	Version int32
}

type SetDataResponse struct {
	Stat Stat
}

type SyncRequest struct {
	Path string
}

type SyncResponse struct {
	Path string
}

type CheckVersionRequest struct {
	Path    string
	Version int32
}

type ErrorResponse struct {
	Err int32
}

type Op struct {
	Type    OpCode
	Request interface{}
}

type OpResult struct {
	Type     OpCode
	Response interface{}
}

type MultiRequest struct {
	Ops []Op
}

func (self MultiRequest) Serialize(buffer *[]byte) {
	for i := range self.Ops {
		op := &self.Ops[i]

		header := multiHeader{
			Type: op.Type,
			Done: false,
			Err:  -1,
		}

		serializeRecord(&header, buffer)
		serializeRecord(op.Request, buffer)
	}

	header := multiHeader{
		Type: -1,
		Done: true,
		Err:  -1,
	}

	serializeRecord(&header, buffer)
}

type MultiResponse struct {
	OpResults []OpResult
}

func (self *MultiResponse) Deserialize(data []byte, dataOffset *int) error {
	var header multiHeader

	for {
		if e := deserializeRecord(&header, data, dataOffset); e != nil {
			return e
		}

		if header.Done {
			return nil
		}

		var response interface{}

		switch header.Type {
		case OpCreate:
			response = &CreateResponse{}
		case OpDelete:
			response = &struct{}{}
		case OpSetData:
			response = &SetDataResponse{}
		case OpCheck:
			response = &struct{}{}
		case OpError:
			response = &ErrorResponse{}
		default:
			return RecordDeserializationError{fmt.Sprintf("headerType=%#v", header.Type)}
		}

		if e := deserializeRecord(response, data, dataOffset); e != nil {
			return e
		}

		self.OpResults = append(self.OpResults, OpResult{
			Type:     header.Type,
			Response: response,
		})
	}
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

type authPacket struct {
	Type   int32
	Scheme string
	Auth   []byte
}

type setWatches struct {
	RelativeZxid int64
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

type watcherEvent struct {
	Type  WatcherEventType
	State int32
	Path  string
}

type multiHeader struct {
	Type OpCode
	Done bool
	Err  ErrorCode
}
