package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		getTaskArgs := GetTaskArgs{}
		getTaskReply := GetTaskReply{}

		if !call("Coordinator.GetTask", &getTaskArgs, &getTaskReply) {
			log.Fatal("GetTask failed")
		}

		switch getTaskReply.TaskType {
		case MapTask:
			doMap(mapf, getTaskReply)
		case ReduceTask:
			doReduce(reducef, getTaskReply)
		case Done:
			os.Exit(0)
		default:
			log.Fatal("invalid TaskType ", getTaskReply.TaskType)
		}

		finishTaskArgs := FinishTaskArgs{
			TaskType: getTaskReply.TaskType,
			TaskNum:  getTaskReply.TaskNum,
		}
		finishTaskReply := FinishTaskReply{}
		if !call("Coordinator.FinishTask", &finishTaskArgs, &finishTaskReply) {
			log.Fatal("FinishTask failed")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(mapf func(string, string) []KeyValue, getTaskReply GetTaskReply) {
	filename := getTaskReply.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("worker:81 cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	tmpFiles := make([]*os.File, getTaskReply.ReduceTaskTotal)
	tmpEncoders := make([]*json.Encoder, getTaskReply.ReduceTaskTotal)
	tmpFilesName := make([]string, getTaskReply.ReduceTaskTotal)

	for i := 0; i < getTaskReply.ReduceTaskTotal; i++ {

		tmpFile, err := os.CreateTemp("", "")
		if err != nil {
			fmt.Println("Error creating temporary file:", err)
			return
		}
		tmpFileName := tmpFile.Name()
		tmpFilesName[i] = tmpFileName
		tmpFiles[i] = tmpFile
		tmpEncoders[i] = json.NewEncoder(tmpFile)

	}
	for _, kv := range kva {

		reduceTaskNum := ihash(kv.Key) % getTaskReply.ReduceTaskTotal
		tmpEncoders[reduceTaskNum].Encode(&kv)
	}
	for i := 0; i < getTaskReply.ReduceTaskTotal; i++ {
		tmpFiles[i].Close()
	}
	for i := 0; i < getTaskReply.ReduceTaskTotal; i++ {
		oname := fmt.Sprintf("mr-%v-%v", getTaskReply.TaskNum, i)
		err := os.Rename(tmpFilesName[i], oname)
		if err != nil {
			fmt.Println("Error renaming temporary file:", err)
			return
		}

	}

}

func doReduce(reducef func(string, []string) string, getTaskReply GetTaskReply) {
	intermediate := []KeyValue{}
	for i := 0; i < getTaskReply.MapTaskTotal; i++ {

		oname := fmt.Sprintf("mr-%v-%v", i, getTaskReply.TaskNum)

		file, err := os.Open(oname)
		if err != nil {
			log.Fatalf("worker:135 cannot open %v", oname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := os.CreateTemp("", "")
	if err != nil {
		fmt.Println("Error creating temporary file:", err)
		return

	}
	tmpFileName := tmpFile.Name()
	//tmpEncoder := json.NewEncoder(tmpFile)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		//tmpEncoder.Encode(KeyValue{intermediate[i].Key, output})
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j

	}
	tmpFile.Close()
	oname := fmt.Sprintf("mr-out-%v", getTaskReply.TaskNum)
	err = os.Rename(tmpFileName, oname)
	if err != nil {
		fmt.Println("Error renaming temporary file:", err)
		return
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Coordinator.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

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
