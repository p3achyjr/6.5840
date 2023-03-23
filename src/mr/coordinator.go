package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle = iota
	InProgress
	Completed
)

type MapTask struct {
	mapPartition int
	mapFile      string
}

type ReduceTask struct {
	reducePartition int
}

type Coordinator struct {
	// Your definitions here.
	mu                    sync.Mutex
	nMap                  int
	nReduce               int
	mapTasksIdle          []MapTask
	mapTasksInProgress    map[int]MapTask // map from worker ID to tasks
	mapTasksCompleted     map[int]MapTask
	reduceTasksIdle       []ReduceTask
	reduceTasksInProgress map[int]ReduceTask
	reduceTasksCompleted  map[int]ReduceTask
	nextWorkerId          int
	activeWorkers         map[int]bool
}

func GetMapOutFile(mapPartition int, reducePartition int) string {
	return fmt.Sprintf("mr-%v-%v", mapPartition, reducePartition)
}

func GetReduceOutFile(reducePartition int) string {
	return fmt.Sprintf("mr-out-%v", reducePartition)
}

func (c *Coordinator) assignMapTask(reply *RequestTaskReply, workerId int) {
	mapTask := c.mapTasksIdle[0]
	c.mapTasksIdle = c.mapTasksIdle[1:]

	reply.Type = MapT
	reply.MapPartition = mapTask.mapPartition
	reply.MapFile = mapTask.mapFile
	reply.NReduce = c.nReduce

	c.mapTasksInProgress[workerId] = mapTask
	c.activeWorkers[workerId] = true
}

func (c *Coordinator) assignReduceTask(reply *RequestTaskReply, workerId int) {
	reduceTask := c.reduceTasksIdle[0]
	c.reduceTasksIdle = c.reduceTasksIdle[1:]

	reply.Type = ReduceT
	reply.ReducePartition = reduceTask.reducePartition
	reply.ReduceFiles = make([]string, 0)
	for _, mapTask := range c.mapTasksCompleted {
		filename := GetMapOutFile(mapTask.mapPartition, reduceTask.reducePartition)
		reply.ReduceFiles = append(reply.ReduceFiles, filename)
	}

	c.reduceTasksInProgress[workerId] = reduceTask
	c.activeWorkers[workerId] = true
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(
	args *RequestTaskArgs, reply *RequestTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := c.nextWorkerId
	if len(c.mapTasksIdle) > 0 {
		// There remain open map tasks.
		c.assignMapTask(reply, workerId)
	} else if len(c.mapTasksCompleted) == c.nMap && len(c.reduceTasksIdle) > 0 {
		// all map tasks are finished. Can begin reduce tasks.
		c.assignReduceTask(reply, workerId)
	} else {
		// conditions are such that no task can be assigned.
		reply.Type = NoneT
		return nil
	}

	reply.WorkerId = workerId
	c.nextWorkerId++
	go c.checkWorkerUnresponsive(workerId)

	return nil
}

func (c *Coordinator) NotifyTaskCompleted(
	args *NotifyTaskCompletedArgs, reply *NotifyTaskCompletedReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := args.WorkerId
	if args.Type == MapT {
		mapTask, ok := c.mapTasksInProgress[workerId]
		if !ok {
			log.Printf(
				`Received call to complete map task, but task in not in progress.
				 Maybe this worker took too long to finish the task? 
				 Worker ID: %v, Map Partition: %v, MapFile: %v`,
				workerId, args.MapPartition, args.MapFile)
			return nil
		}

		c.mapTasksCompleted[workerId] = mapTask
		delete(c.mapTasksInProgress, workerId)
	} else if args.Type == ReduceT {
		reduceTask, ok := c.reduceTasksInProgress[workerId]
		if !ok {
			log.Printf(
				`Received call to complete map task, but task in not in progress.
				 Maybe this worker took too long to finish the task? 
				 Worker ID: %v, Reduce Partition: %v`,
				workerId, args.ReducePartition)
			return nil
		}

		c.reduceTasksCompleted[workerId] = reduceTask
		delete(c.reduceTasksInProgress, workerId)
	} else {
		log.Fatalf("Error in Coordinator: Unknown Task Type Completed %v", args.Type)
	}

	delete(c.activeWorkers, workerId)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// waits 10 seconds, then checks if worker has not completed task.
//
func (c *Coordinator) checkWorkerUnresponsive(workerId int) {
	time.Sleep(time.Duration(10) * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.activeWorkers[workerId] {
		// task is not active. presumably it has completed successfully.
		return
	}

	// task is still active. reset running tasks to idle.
	if mapTask, ok := c.mapTasksInProgress[workerId]; ok {
		c.mapTasksIdle = append(c.mapTasksIdle, mapTask)
		delete(c.mapTasksInProgress, workerId)
	} else if reduceTask, ok := c.reduceTasksInProgress[workerId]; ok {
		c.reduceTasksIdle = append(c.reduceTasksIdle, reduceTask)
		delete(c.reduceTasksInProgress, workerId)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapTasksIdle) != 0 {
		return false
	} else if len(c.mapTasksInProgress) != 0 {
		return false
	} else if len(c.reduceTasksIdle) != 0 {
		return false
	} else if len(c.reduceTasksInProgress) != 0 {
		return false
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("MakeCoordinator")
	c := Coordinator{}

	// Your code here.
	c.mapTasksIdle = make([]MapTask, len(files))
	for i, file := range files {
		c.mapTasksIdle[i].mapPartition = i
		c.mapTasksIdle[i].mapFile = file
	}

	c.reduceTasksIdle = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasksIdle[i].reducePartition = i
	}

	c.mapTasksInProgress = make(map[int]MapTask)
	c.mapTasksCompleted = make(map[int]MapTask)
	c.reduceTasksInProgress = make(map[int]ReduceTask)
	c.reduceTasksCompleted = make(map[int]ReduceTask)
	c.activeWorkers = make(map[int]bool)
	c.nMap = len(files)
	c.nReduce = nReduce

	c.server()
	return &c
}
