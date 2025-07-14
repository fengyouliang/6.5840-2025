package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	tasks   []task
	mu      sync.Mutex

	ReduceMap map[int]bool
}

type task struct {
	id        int
	file      string
	startTime time.Time
	status    int // 0: no start, 1: map processing, 2: map done, 3: reduce processing, 4: reduce done
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerRequest(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.doneByStage(2) {
		for i := range c.tasks {
			if c.tasks[i].status == 0 {
				reply.File = c.tasks[i].file
				reply.OptionType = 0
				reply.NumReduce = c.nReduce
				c.tasks[i].status = 1
				c.tasks[i].startTime = time.Now()
				reply.ID = c.tasks[i].id
				return nil
			}
		}
	} else {
		if !c.doneByStage(4) {
			for reduceIdx, flag := range c.ReduceMap {
				if !flag {
					reply.OptionType = 1
					reply.ReduceIndex = reduceIdx
					reply.NumMapper = len(c.files)
					return nil
				}
			}
		} else {
			reply.OptionType = 3
		}
	}
	return nil
}

func (c *Coordinator) WorkerComplete(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskPtr, ok := c.getTask(args.ID)
	if !ok {
		log.Fatalf("task %d not found", args.ID)
	}
	if args.OptionType == 0 {
		taskPtr.status = 2
	} else {
		c.ReduceMap[args.ReduceIndex] = true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, flag := range c.ReduceMap {
		if !flag {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NumReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.initCoordinator()

	c.server()
	return &c
}

func (c *Coordinator) initCoordinator() {
	for _, file := range c.files {
		c.tasks = append(c.tasks, task{id: len(c.tasks), file: file, status: 0})
	}
	c.ReduceMap = make(map[int]bool)
	for i := range c.nReduce {
		c.ReduceMap[i] = false
	}
}

func (c *Coordinator) getTask(id int) (*task, bool) {
	for i := range c.tasks {
		if c.tasks[i].id == id {
			return &c.tasks[i], true
		}
	}
	return nil, false
}

func (c *Coordinator) doneByStage(stage int) bool {
	res := true
	for i := range c.tasks {
		if c.tasks[i].status < stage {
			res = false
			break
		}
	}
	return res
}
