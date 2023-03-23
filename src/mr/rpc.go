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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	NoneT TaskType = iota
	MapT
	ReduceT
)

// Worker calls Coordinator

// RPC for worker to request work from coordinator
type RequestTaskArgs struct{}

type RequestTaskReply struct {
	Type            TaskType
	WorkerId        int
	MapPartition    int
	MapFile         string
	NReduce         int
	ReducePartition int
	ReduceFiles     []string
}

type NotifyTaskCompletedArgs struct {
	Type            TaskType
	WorkerId        int
	MapPartition    int
	MapFile         string
	ReducePartition int
}

type NotifyTaskCompletedReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
