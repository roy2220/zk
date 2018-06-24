package zk

import (
	"fmt"
)

const (
	ErrorSystem                  ErrorCode = -1
	ErrorRuntimeInconsistency    ErrorCode = -2
	ErrorDataInconsistency       ErrorCode = -3
	ErrorConnectionLoss          ErrorCode = -4
	ErrorMarshalling             ErrorCode = -5
	ErrorUnimplemented           ErrorCode = -6
	ErrorOperationTimeout        ErrorCode = -7
	ErrorBadArguments            ErrorCode = -8
	ErrorUnknownSession          ErrorCode = -12
	ErrorNewConfigNoQuorum       ErrorCode = -13
	ErrorReconfigInProgress      ErrorCode = -14
	ErrorAPI                     ErrorCode = -100
	ErrorNoNode                  ErrorCode = -101
	ErrorNoAuth                  ErrorCode = -102
	ErrorBadVersion              ErrorCode = -103
	ErrorNoChildrenForEphemerals ErrorCode = -108
	ErrorNodeExists              ErrorCode = -110
	ErrorNotEmpty                ErrorCode = -111
	ErrorSessionExpired          ErrorCode = -112
	ErrorInvalidCallback         ErrorCode = -113
	ErrorInvalidACL              ErrorCode = -114
	ErrorAuthFailed              ErrorCode = -115
	ErrorSessionMoved            ErrorCode = -118
	ErrorNotReadOnly             ErrorCode = -119
	ErrorEphemeralOnLocalSession ErrorCode = -120
	ErrorNoWatcher               ErrorCode = -121
	ErrorReconfigDisabled        ErrorCode = -123
)

type Error struct {
	code    ErrorCode
	context string
}

func (self Error) GetCode() ErrorCode {
	return self.code
}

func (self Error) Error() string {
	var result string

	switch self.code {
	case ErrorSystem:
		result = "zk: system"
	case ErrorRuntimeInconsistency:
		result = "zk: runtime inconsistency"
	case ErrorDataInconsistency:
		result = "zk: data inconsistency"
	case ErrorConnectionLoss:
		result = "zk: connection loss"
	case ErrorMarshalling:
		result = "zk: marshalling"
	case ErrorUnimplemented:
		result = "zk: unimplemented"
	case ErrorOperationTimeout:
		result = "zk: operation timeout"
	case ErrorBadArguments:
		result = "zk: bad arguments"
	case ErrorUnknownSession:
		result = "zk: unknown session"
	case ErrorNewConfigNoQuorum:
		result = "zk: new config no quorum"
	case ErrorReconfigInProgress:
		result = "zk: reconfig in progress"
	case ErrorAPI:
		result = "zk: api"
	case ErrorNoNode:
		result = "zk: no node"
	case ErrorNoAuth:
		result = "zk: no auth"
	case ErrorBadVersion:
		result = "zk: bad version"
	case ErrorNoChildrenForEphemerals:
		result = "zk: no children for ephemerals"
	case ErrorNodeExists:
		result = "zk: node exists"
	case ErrorNotEmpty:
		result = "zk: not empty"
	case ErrorSessionExpired:
		result = "zk: session expired"
	case ErrorInvalidCallback:
		result = "zk: invalid callback"
	case ErrorInvalidACL:
		result = "zk: invalid acl"
	case ErrorAuthFailed:
		result = "zk: auth failed"
	case ErrorSessionMoved:
		result = "zk: session moved"
	case ErrorNotReadOnly:
		result = "zk: not read only"
	case ErrorEphemeralOnLocalSession:
		result = "zk: ephemeral on local session"
	case ErrorNoWatcher:
		result = "zk: no watcher"
	case ErrorReconfigDisabled:
		result = "zk: reconfig disabled"
	default:
		result = fmt.Sprintf("zk: error %d", self.code)
	}

	if self.context != "" {
		result += ": " + self.context
	}

	return result
}

func (self Error) GoString() string {
	switch self.code {
	case ErrorSystem:
		return "<ErrorSystem>"
	case ErrorRuntimeInconsistency:
		return "<ErrorRuntimeInconsistency>"
	case ErrorDataInconsistency:
		return "<ErrorDataInconsistency>"
	case ErrorConnectionLoss:
		return "<ErrorConnectionLoss>"
	case ErrorMarshalling:
		return "<ErrorMarshalling>"
	case ErrorUnimplemented:
		return "<ErrorUnimplemented>"
	case ErrorOperationTimeout:
		return "<ErrorOperationTimeout>"
	case ErrorBadArguments:
		return "<ErrorBadArguments>"
	case ErrorUnknownSession:
		return "<ErrorUnknownSession>"
	case ErrorNewConfigNoQuorum:
		return "<ErrorNewConfigNoQuorum>"
	case ErrorReconfigInProgress:
		return "<ErrorReconfigInProgress>"
	case ErrorAPI:
		return "<ErrorAPI>"
	case ErrorNoNode:
		return "<ErrorNoNode>"
	case ErrorNoAuth:
		return "<ErrorNoAuth>"
	case ErrorBadVersion:
		return "<ErrorBadVersion>"
	case ErrorNoChildrenForEphemerals:
		return "<ErrorNoChildrenForEphemerals>"
	case ErrorNodeExists:
		return "<ErrorNodeExists>"
	case ErrorNotEmpty:
		return "<ErrorNotEmpty>"
	case ErrorSessionExpired:
		return "<ErrorSessionExpired>"
	case ErrorInvalidCallback:
		return "<ErrorInvalidCallback>"
	case ErrorInvalidACL:
		return "<ErrorInvalidACL>"
	case ErrorAuthFailed:
		return "<ErrorAuthFailed>"
	case ErrorSessionMoved:
		return "<ErrorSessionMoved>"
	case ErrorNotReadOnly:
		return "<ErrorNotReadOnly>"
	case ErrorEphemeralOnLocalSession:
		return "<ErrorEphemeralOnLocalSession>"
	case ErrorNoWatcher:
		return "<ErrorNoWatcher>"
	case ErrorReconfigDisabled:
		return "<ErrorReconfigDisabled>"
	default:
		return fmt.Sprintf("<Error:%d>", self)
	}
}

type ErrorCode int32
