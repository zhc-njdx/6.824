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
type RPCArgs struct {
	State              int      // the worker state
	LastTaskType       int      // map or reduce
	LastTaskNo         int      // taskNo
	Name               string   // the name of worker which coordinator can recognize
	LastTaskInputFiles []string // input filename of last task
	OutputFileNames    []string // output filename
}

type RPCReply struct {
	Id             string   // the id assigned by coordinator
	TaskType       int      // map or reduce
	TaskNo         int      // taskNo
	NReduce        int      // reduce task number
	InputFileNames []string // filename of input file
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
