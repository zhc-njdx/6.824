package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var state = IDLE

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// do the map function or reduce function
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.Println("begin working...")

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// send a rpc
	args := RPCArgs{}
	reply := RPCReply{}

	args.Name = "worker"
	args.State = state

	for {
		log.Println("send a rpc request...")
		// get the task information
		ok := call("Coordinator.DistributeTask", &args, &reply)
		if !ok {
			log.Fatalf("call failed!\n")
		}

		// clear the args outputfilenames
		args.OutputFileNames = args.OutputFileNames[0:0]
		args.LastTaskInputFiles = args.LastTaskInputFiles[0:0]
		// get the Id
		if args.Name == "worker" {
			args.Name = reply.Id
		}

		// do the task
		if reply.TaskType == MAP_TASK { // map task
			// read the file and invoke the map function
			log.Println("begin doing map task...")
			state = IN_PROCESSED

			// collect all the key-value which belongs the same reduce task NO
			kvaListByNReduce := make([][]KeyValue, reply.NReduce)

			for _, filename := range reply.InputFileNames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v\n", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v\n", filename)
				}
				file.Close()

				kva := mapf(filename, string(content))
				// insert the kv into the right place
				for _, kv := range kva {
					reduceTaskNo := ihash(kv.Key) % reply.NReduce
					kvaListByNReduce[reduceTaskNo] = append(kvaListByNReduce[reduceTaskNo], kv)
				}
			}
			// encode the intermediate kv into json file
			tempFilePath := []string{}
			for reduceNo, kvList := range kvaListByNReduce {
				intermediateFileName := "mr-" + strconv.Itoa(reply.TaskNo) + "-" + strconv.Itoa(reduceNo)
				args.OutputFileNames = append(args.OutputFileNames, intermediateFileName)
				output := writeIntermediateData(reply.TaskNo, reduceNo, kvList)
				tempFilePath = append(tempFilePath, output)
			}
			// rename the file
			for index, tempFile := range tempFilePath {
				err := os.Rename(tempFile, args.OutputFileNames[index])
				if err != nil {
					log.Fatalf("can not rename temp file %v, the err: %v\n", tempFile, err)
				}
			}

			state = COMPLETED
		} else if reply.TaskType == REDUCE_TASK { // reduce task
			log.Println("begin doing reduce task...")
			state = IN_PROCESSED

			kva := []KeyValue{}
			// collect all the keyvalue from mr-x-y file produced by map task
			for _, filename := range reply.InputFileNames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("can not open %v\n", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva)) // sort the key-value

			oname := "temp-mr-out-" + strconv.Itoa(reply.TaskNo)
			ofile, err := ioutil.TempFile("", oname)
			if err != nil {
				log.Fatalf("can not creat temp file %v\n", oname)
			}

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			err = os.Rename(ofile.Name(), oname[5:])
			if err != nil {
				log.Fatalf("can not rename temp file %v, the err: %v\n", ofile.Name(), err)
			}

			ofile.Close()

			state = COMPLETED
		} else if reply.TaskType == EXIT_TASK { // 'please exit' persudo task
			log.Println("please exit...")
			os.Exit(0)
		} else { // waiting for coordinator
			state = IDLE
			time.Sleep(time.Second)
		}
		// update the args and reply
		log.Println("finish task...")
		args.State = state
		if state == COMPLETED {
			args.LastTaskNo = reply.TaskNo
			args.LastTaskType = reply.TaskType
			args.LastTaskInputFiles = reply.InputFileNames
		}
		reply = RPCReply{}
	}
}

func writeIntermediateData(mapNo int, reduceNo int, kvList []KeyValue) string {
	intermediateFileName := "temp-mr-" + strconv.Itoa(mapNo) + "-" + strconv.Itoa(reduceNo)
	// try to open, if file does not exist, try to create it
	intermediateFile, err := ioutil.TempFile("", intermediateFileName)
	if err != nil {
		log.Fatalf("can not create temp file %v\n", intermediateFileName)

	}
	enc := json.NewEncoder(intermediateFile)
	for _, kv := range kvList {
		if err = enc.Encode(&kv); err != nil {
			log.Fatalf("can not encode %v\n", kv)
		}
	}
	log.Printf("finish writing the temp file %v\n", intermediateFileName)
	return intermediateFile.Name()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
