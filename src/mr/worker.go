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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// Reduce functions take in a list of values per key.
//
type KeyValueGroup struct {
	Key    string
	Values []string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func RunMapTask(
	mapPartition int,
	mapFile string,
	nReduce int,
	mapf func(string, string) []KeyValue,
) {
	file, err := os.Open(mapFile)
	if err != nil {
		log.Fatalf("cannot open %v, error: %v", mapFile, err.Error())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v, error: %v", mapFile, err.Error())
	}
	file.Close()
	kva := mapf(mapFile, string(content))

	// write to disk
	outFilenames := make([]string, nReduce)
	outTempFiles := make([]*os.File, nReduce)
	for i := 0; i < len(outFilenames); i++ {
		fname := GetMapOutFile(mapPartition, i)
		tempFile, err := os.CreateTemp(".", fname)
		if err != nil {
			log.Fatalf(
				"cannot create map tmp outfile %v, Error: %v",
				tempFile.Name(), err.Error(),
			)
		}

		outTempFiles[i] = tempFile
		outFilenames[i] = fname
	}

	for _, kv := range kva {
		outFile := outTempFiles[ihash(kv.Key)%nReduce]
		if err != nil {
			log.Fatalf("cannot open file %v, error: %v", outFile.Name(), err.Error())
		}

		enc := json.NewEncoder(outFile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v, error: %v", kv.Key, err.Error())
		}
	}

	// rename each file to the final file.
	// there are potential write races here, but it shouldn't matter, since the
	// filesystem guarantees atomicity and the result is the same.
	for i := 0; i < len(outFilenames); i++ {
		outTempFiles[i].Close()
		os.Rename(outTempFiles[i].Name(), outFilenames[i])
	}
}

func RunReduceTask(
	reducePartition int,
	reduceFiles []string,
	reducef func(string, []string) string,
) {
	// read all key-values from disk
	kva := make([]KeyValue, 0)
	for _, filename := range reduceFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open map outfile %v", filename)
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

	// sort
	sort.SliceStable(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// group
	kvGroups := make([]KeyValueGroup, 0)
	for _, kv := range kva {
		k, v := kv.Key, kv.Value
		if len(kvGroups) == 0 || k != kvGroups[len(kvGroups)-1].Key {
			kvGroups = append(kvGroups, KeyValueGroup{Key: k})
		}

		if len(kvGroups[len(kvGroups)-1].Values) == 0 {
			kvGroups[len(kvGroups)-1].Values = make([]string, 0)
		}

		kvGroups[len(kvGroups)-1].Values = append(
			kvGroups[len(kvGroups)-1].Values,
			v,
		)
	}

	// create output file
	outFilename := GetReduceOutFile(reducePartition)
	outFile, err := os.CreateTemp(".", outFilename)
	if err != nil {
		log.Fatalf("cannot create file %v", outFile)
	}

	// pass to user fn
	for _, kvGroup := range kvGroups {
		output := reducef(kvGroup.Key, kvGroup.Values)
		fmt.Fprintf(outFile, "%v %v\n", kvGroup.Key, output)
	}

	// rename file
	outFile.Close()
	os.Rename(outFile.Name(), outFilename)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	sleepTimeMs := 10
	for {
		requestTaskArgs := RequestTaskArgs{}
		requestTaskReply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &requestTaskArgs, &requestTaskReply)
		if !ok {
			os.Exit(0)
		}

		if requestTaskReply.Type == NoneT {
			time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
			sleepTimeMs *= 2
			continue
		}

		ok = true
		if requestTaskReply.Type == MapT {
			log.Println(
				fmt.Sprintf(
					"Received Map Task. Map Partition: %v, Map File: %v",
					requestTaskReply.MapPartition,
					requestTaskReply.MapFile))
			RunMapTask(
				requestTaskReply.MapPartition,
				requestTaskReply.MapFile,
				requestTaskReply.NReduce,
				mapf,
			)

			notifyTaskArgs := NotifyTaskCompletedArgs{
				Type:         MapT,
				WorkerId:     requestTaskReply.WorkerId,
				MapPartition: requestTaskReply.MapPartition,
				MapFile:      requestTaskReply.MapFile,
			}
			notifyTaskReply := NotifyTaskCompletedReply{}
			ok = call("Coordinator.NotifyTaskCompleted",
				&notifyTaskArgs,
				&notifyTaskReply,
			)
		} else if requestTaskReply.Type == ReduceT {
			log.Println(
				fmt.Sprintf(
					"Received Reduce Task. Reduce Partition: %v",
					requestTaskReply.ReducePartition))
			RunReduceTask(
				requestTaskReply.ReducePartition,
				requestTaskReply.ReduceFiles,
				reducef,
			)

			notifyTaskArgs := NotifyTaskCompletedArgs{
				Type:            ReduceT,
				WorkerId:        requestTaskReply.WorkerId,
				ReducePartition: requestTaskReply.ReducePartition,
			}
			notifyTaskReply := NotifyTaskCompletedReply{}
			ok = call("Coordinator.NotifyTaskCompleted",
				&notifyTaskArgs,
				&notifyTaskReply,
			)
		}

		if !ok {
			log.Println("(worker) Something went wrong.")
			time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
			sleepTimeMs *= 2
		} else {
			// everything is ok
			sleepTimeMs = 10
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	log.Println(err)
	return false
}
