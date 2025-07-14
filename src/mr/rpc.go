package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type WorkerArgs struct {
	ID            int
	File          string
	OptionType    int
	NumReduce     int
	status        int // 0: no start, 1: map processing, 2: map done, 3: reduce processing, 4: reduce done
	TempFileNames []string

	ReduceIndex int
}

type WorkerReply struct {
	ID         int
	File       string
	OptionType int
	NumReduce  int
	status     int // 0: no start, 1: map processing, 2: map done, 3: reduce processing, 4: reduce done

	ReduceIndex int
	NumMapper   int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
