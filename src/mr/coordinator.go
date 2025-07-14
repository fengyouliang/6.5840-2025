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

type task struct {
	id        int
	file      string
	status    int // 0: not started, 1: in progress, 2: completed
	startTime time.Time
}

type Coordinator struct {
	mu                  sync.Mutex
	files               []string
	nReduce             int
	tasks               []task
	mapTimeout          time.Duration
	reduceTimeout       time.Duration
	mapPhaseComplete    bool
	reducePhaseComplete bool
	ReduceMap           map[int]int // reduce task status: 0=not started, 1=in progress, 2=completed
	reduceStartTime     map[int]time.Time
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:               files,
		nReduce:             nReduce,
		mapTimeout:          10 * time.Second,
		reduceTimeout:       10 * time.Second,
		mapPhaseComplete:    false,
		reducePhaseComplete: false,
		ReduceMap:           make(map[int]int),
		reduceStartTime:     make(map[int]time.Time),
	}

	// Initialize tasks
	for i, file := range files {
		c.tasks = append(c.tasks, task{
			id:     i,
			file:   file,
			status: 0,
		})
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.ReduceMap[i] = 0
	}

	log.Printf("Coordinator: Initialized with %d map tasks and %d reduce tasks", len(c.tasks), nReduce)
	c.server()
	return &c
}

func (c *Coordinator) WorkerRequest(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for timed out tasks
	c.checkTimeouts()

	// Handle map phase
	if !c.mapPhaseComplete {
		if task := c.getAvailableMapTask(); task != nil {
			reply.ID = task.id
			reply.File = task.file
			reply.OptionType = 0 // Map task
			reply.NumReduce = c.nReduce

			task.status = 1
			task.startTime = time.Now()

			log.Printf("Coordinator: Assigned map task %d (file: %s) to worker", task.id, task.file)
			return nil
		}

		if c.allMapTasksCompleted() {
			c.mapPhaseComplete = true
			log.Printf("Coordinator: All map tasks completed, starting reduce phase")
		} else {
			reply.OptionType = 2 // Wait
			return nil
		}
	}

	// Handle reduce phase
	if c.mapPhaseComplete && !c.reducePhaseComplete {
		if reduceIdx := c.getAvailableReduceTask(); reduceIdx != -1 {
			reply.OptionType = 1 // Reduce task
			reply.ReduceIndex = reduceIdx
			reply.NumMapper = len(c.files)

			c.ReduceMap[reduceIdx] = 1
			c.reduceStartTime[reduceIdx] = time.Now()

			log.Printf("Coordinator: Assigned reduce task %d to worker", reduceIdx)
			return nil
		}

		if c.allReduceTasksCompleted() {
			c.reducePhaseComplete = true
			log.Printf("Coordinator: All reduce tasks completed")
		} else {
			reply.OptionType = 2 // Wait
			return nil
		}
	}

	// All tasks completed
	reply.OptionType = 3 // Done
	return nil
}

func (c *Coordinator) WorkerComplete(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.OptionType == 0 { // Map task completed
		if args.ID < len(c.tasks) {
			c.tasks[args.ID].status = 2
			log.Printf("Coordinator: Map task %d completed successfully", args.ID)
		}
	} else if args.OptionType == 1 { // Reduce task completed
		if status, exists := c.ReduceMap[args.ReduceIndex]; exists && status == 1 {
			c.ReduceMap[args.ReduceIndex] = 2
			log.Printf("Coordinator: Reduce task %d completed successfully", args.ReduceIndex)
		}
	}

	return nil
}

func (c *Coordinator) checkTimeouts() {
	now := time.Now()

	// Check map task timeouts
	for i := range c.tasks {
		if c.tasks[i].status == 1 && now.Sub(c.tasks[i].startTime) > c.mapTimeout {
			log.Printf("Coordinator: Map task %d timed out, marking as available", c.tasks[i].id)
			c.tasks[i].status = 0
		}
	}

	// Check reduce task timeouts
	for idx, status := range c.ReduceMap {
		if status == 1 {
			if startTime, exists := c.reduceStartTime[idx]; exists && now.Sub(startTime) > c.reduceTimeout {
				log.Printf("Coordinator: Reduce task %d timed out, marking as available", idx)
				c.ReduceMap[idx] = 0
			}
		}
	}
}

func (c *Coordinator) getAvailableMapTask() *task {
	for i := range c.tasks {
		if c.tasks[i].status == 0 {
			return &c.tasks[i]
		}
	}
	return nil
}

func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.tasks {
		if task.status != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) getAvailableReduceTask() int {
	for i := 0; i < c.nReduce; i++ {
		if c.ReduceMap[i] == 0 {
			return i
		}
	}
	return -1
}

func (c *Coordinator) allReduceTasksCompleted() bool {
	for i := 0; i < c.nReduce; i++ {
		if c.ReduceMap[i] != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapPhaseComplete && c.reducePhaseComplete
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
