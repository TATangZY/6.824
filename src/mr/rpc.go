package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

const (
	UNASSIGNED = 0
	RUNNING    = 1
	FINISHED   = 2

	MAP    = 0
	REDUCE = 1
	WAIT   = 2
	OVER   = 3

	REQUEST = 0
	DONE    = 1
)

type MrArgs struct {
	Type int
	Task mrTask
}

type MrReply struct {
	Task      mrTask
	ReduceNum int
	FileNum   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
