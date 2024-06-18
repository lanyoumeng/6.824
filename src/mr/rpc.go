package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	Done       TaskType = 2
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType TaskType
	TaskNum  int // task number
	FileName string

	MapTaskTotal    int
	ReduceTaskTotal int
}

type FinishTaskArgs struct {
	TaskType TaskType
	TaskNum  int
}

type FinishTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
