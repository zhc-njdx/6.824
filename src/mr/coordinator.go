package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// file state
const NOT_PROCESSED = 0
const IN_PROCESSED = 1
const COMPLETED = 2

// task type
const MAP_TASK = 0
const REDUCE_TASK = 1
const EXIT_TASK = 2
const WAITING = 3

// worker state
const IDLE = 0
const DEAD = 3

type Task struct {
	taskType int
	taskNo   int
}

type Coordinator struct {
	// Your definitions here.
	files                   []string   // initial input files
	filesState              []int      // the files state: 0 not_processed 1 in_processed 2 completed
	numFilesCompleted       int        // the number of the files has been processed by map task
	intermediateFiles       []string   // the intermediate file produced by map task
	nReduce                 int        // reduce tasks number
	reduceTaskState         []int      // reduce task's state
	numReduceTaskCompleted  int        // the number of the completed reduce task
	jobDone                 bool       // if job done
	workerNo                int        // the worker number
	workerLastCommunication []int64    // the last communication timestamp of the workers
	worker2Task             []Task     // record the task distributed to the workers
	lock                    sync.Mutex // the lock for sharing data
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DistributeTask(args *RPCArgs, reply *RPCReply) error {
	c.lock.Lock()
	log.Println("get the rpc request...")

	currentWorker := -1

	// if a new worker, assign the id to it
	// update the communication time
	if args.Name == "worker" {
		reply.Id = strconv.Itoa(c.workerNo)
		currentWorker = c.workerNo
		c.workerLastCommunication = append(c.workerLastCommunication, time.Now().Unix()) // add a idle worker
		c.worker2Task = append(c.worker2Task, Task{})
		c.workerNo++
	} else {
		currentWorker, _ = strconv.Atoi(args.Name)
		c.workerLastCommunication[currentWorker] = time.Now().Unix()
	}

	// deal the args
	if args.State == COMPLETED { // has completed the task
		if args.LastTaskType == MAP_TASK { // map task
			// mark the completed file
			for _, filename := range args.LastTaskInputFiles {
				for index, filename0 := range c.files {
					if filename == filename0 {
						c.filesState[index] = COMPLETED
						c.numFilesCompleted++
					}
				}
			}
			// c.numFilesCompleted += len(args.LastTaskInputFiles)
			c.intermediateFiles = append(c.intermediateFiles, args.OutputFileNames...)
			// log.Printf("intermediate files: %v\n", c.intermediateFiles)
		} else if args.LastTaskType == REDUCE_TASK { // reduce task
			c.reduceTaskState[args.LastTaskNo] = COMPLETED
			c.numReduceTaskCompleted++
		}
	} else if args.State == IN_PROCESSED {
		c.lock.Unlock()
		return nil
	}

	log.Println("complete the data update and begin to distribute task...")
	// distribute the tasks
	if c.numReduceTaskCompleted == c.nReduce { // completed all reduce tasks so the job is done
		reply.TaskType = EXIT_TASK
		c.worker2Task[currentWorker].taskType = EXIT_TASK
		c.jobDone = true
		c.lock.Unlock()
		return nil
	}
	reply.TaskType = MAP_TASK                // map task
	if c.numFilesCompleted == len(c.files) { // all input files has been completed
		reply.TaskType = REDUCE_TASK // reduce task

		reduceNo := -1
		for index, taskState := range c.reduceTaskState {
			if taskState == NOT_PROCESSED {
				reduceNo = index
				c.reduceTaskState[index] = IN_PROCESSED
				break
			}
		}

		if reduceNo == -1 { // all reduce task has already distributed
			reply.TaskType = WAITING
		} else {
			reply.TaskNo = reduceNo

			re := regexp.MustCompile("mr-[0-9]-" + strconv.Itoa(reply.TaskNo)) // match the corresponding file
			for _, filename := range c.intermediateFiles {
				// intermediate filename format: mr-x-y.json
				if re.MatchString(filename) {
					reply.InputFileNames = append(reply.InputFileNames, filename)
				}
			}
		}
	} else {
		mapNo := -1
		filename := ""
		for index, fileState := range c.filesState {
			if fileState == NOT_PROCESSED {
				filename = c.files[index]
				c.filesState[index] = IN_PROCESSED
				mapNo = index
				break
			}
		}
		if mapNo == -1 { // all file is in_processed or completed
			log.Printf("%v != %v", c.numFilesCompleted, len(c.files))
			reply.TaskType = WAITING // waiting
		} else {
			reply.NReduce = c.nReduce
			reply.InputFileNames = append(reply.InputFileNames, filename)
			reply.TaskNo = mapNo
		}
	}
	// update the worker-task
	c.worker2Task[currentWorker].taskType = reply.TaskType
	c.worker2Task[currentWorker].taskNo = reply.TaskNo

	log.Println("complete task distribution...")
	// log.Printf("%v", reply)
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) CheckWorkerState() {
	now := time.Now().Unix()
	c.lock.Lock()
	for index, last := range c.workerLastCommunication {
		if now-last >= 10 { // dead work
			fmt.Printf("~~~~ worker %v is dead ~~~~\n", index)
			taskType := c.worker2Task[index].taskType
			taskNo := c.worker2Task[index].taskNo
			c.worker2Task[index].taskType = DEAD
			if taskType == MAP_TASK {
				c.filesState[taskNo] = NOT_PROCESSED
			} else if taskType == REDUCE_TASK {
				c.reduceTaskState[taskNo] = NOT_PROCESSED
			}
		}
	}
	c.lock.Unlock()
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	c.lock.Lock()
	if c.jobDone {
		ret = true
	}
	c.lock.Unlock()

	if ret {
		log.Println("Job Finished...")
	} else {
		log.Println("Jon not Finished...")
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	log.Println("Coordinator c begins init...")

	// Your code here.
	// do the initialise of the Coordinator
	len := len(files)
	c.files = make([]string, len)
	copy(c.files, files)
	c.nReduce = nReduce
	c.filesState = make([]int, len)
	for i := 0; i < len; i++ {
		c.filesState[i] = NOT_PROCESSED
	}
	c.numFilesCompleted = 0
	c.reduceTaskState = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTaskState[i] = IDLE
	}
	c.numReduceTaskCompleted = 0
	c.intermediateFiles = []string{}
	c.jobDone = false
	c.lock = sync.Mutex{}
	c.workerNo = 0
	c.workerLastCommunication = []int64{}
	c.worker2Task = []Task{}

	log.Println("Coordinator c ends init...")

	c.server()
	log.Println("Coordinator c begins serve...")
	return &c
}
