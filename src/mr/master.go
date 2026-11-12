package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

type TaskStatus struct {
	state    TaskState
	assignee string
}

type Master struct {
	mu            sync.Mutex
	files         []string
	nReduce       int
	pendingMap    int
	pendingReduce int
	mapTasks      map[int]TaskStatus
	reduceTasks   map[int]TaskStatus
}

const (
	Pending TaskState = iota
	Running
	Completed
)

const TaskTimeout = 10 * time.Second

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	workerID := args.WorkerID
	reply.ID = -1
	for i, task := range m.mapTasks {
		if task.state == Pending {
			task.state = Running
			task.assignee = workerID
			m.mapTasks[i] = task

			reply.Type = Map
			reply.File = m.files[i]
			reply.ID = i
			reply.NReduce = m.nReduce

			m.TimerResetTask(m.mapTasks, i, workerID)

			return nil
		}
	}

	// only move to reduceTasks when pendingMap == 0
	if m.pendingMap == 0 {
		for i, task := range m.reduceTasks {
			if task.state == Pending {
				task.state = Running
				task.assignee = workerID
				m.reduceTasks[i] = task

				reply.Type = Reduce
				reply.ID = i

				m.TimerResetTask(m.reduceTasks, i, workerID)
				return nil
			}
		}
	}

	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskType, workerID, taskID := args.TaskType, args.WorkerID, args.TaskID

	if taskType == Map {
		if m.CompleteTask(taskID, workerID, m.mapTasks) {
			m.pendingMap -= 1
		}
	} else {
		if m.CompleteTask(taskID, workerID, m.reduceTasks) {
			m.pendingReduce -= 1
		}
	}

	return nil
}

func (m *Master) CompleteTask(taskID int, workerID string, tasks map[int]TaskStatus) bool {
	task := tasks[taskID]
	if task.state == Running && task.assignee == workerID {
		task.state = Completed
		tasks[taskID] = task
		return true
	}
	return false
}

func (m *Master) TimerResetTask(tasks map[int]TaskStatus, index int, workerID string) {
	time.AfterFunc(TaskTimeout, func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		task := tasks[index]
		if task.state == Running && task.assignee == workerID {
			task.state = Pending
			task.assignee = ""
			tasks[index] = task
		}
	})
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pendingReduce == 0
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:         files,
		nReduce:       nReduce,
		pendingMap:    len(files),
		pendingReduce: nReduce,
		mapTasks:      make(map[int]TaskStatus),
		reduceTasks:   make(map[int]TaskStatus),
	}

	for i := range files {
		m.mapTasks[i] = TaskStatus{state: Pending}
	}

	for i := range nReduce {
		m.reduceTasks[i] = TaskStatus{state: Pending}
	}

	m.server()
	return &m
}
