package mr

import (
	"os"
	"strconv"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type GetTaskArgs struct {
	WorkerID string
}

type GetTaskReply struct {
	ID      int
	Type    TaskType
	File    string
	NReduce int
}

type ReportTaskArgs struct {
	TaskType TaskType
	WorkerID string
	TaskID   int
}

type ReportTaskReply struct {
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
