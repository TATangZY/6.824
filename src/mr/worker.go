package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := MrArgs{}
		args.Type = REQUEST
		reply := MrReply{}

		res := call("Master.WorkerHandler", &args, &reply)
		if !res {
			log.Fatal("cannot call master")
		}

		switch reply.Task.Type {
		case MAP:
			doMap(&reply, mapf)
		case REDUCE:
			doReduce(&reply, reducef)
		case WAIT:
			time.Sleep(1 * time.Second)
		case OVER:
			break
		}
	}
}

func doMap(reply *MrReply, mapf func(string, string) []KeyValue) {
	inFile, err := os.Open(reply.Task.MapFile)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Task.MapFile)
	}

	content, err := ioutil.ReadAll(inFile)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Task.MapFile)
	}

	inFile.Close()

	kva := mapf(reply.Task.MapFile, string(content))
	outFiles := make([]*os.File, reply.ReduceNum)
	fileEncs := make([]*json.Encoder, reply.ReduceNum)
	for i := 0; i < reply.ReduceNum; i++ {
		outFiles[i], err = ioutil.TempFile(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file. Error: %v", err)
		}

		fileEncs[i] = json.NewEncoder(outFiles[i])
	}

	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % reply.ReduceNum
		enc := fileEncs[reduceIndex]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("File %v Key %v Value %v Error %v", reply.Task.MapFile, kv.Key, kv.Value, err)
		}
	}

	for i, file := range outFiles {
		outName := "mr-" + strconv.Itoa(reply.Task.Index) + "-" + strconv.Itoa(i)
		oldPath := filepath.Join(file.Name())
		os.Rename(oldPath, outName)
		file.Close()
	}

	args := MrArgs{}
	args.Task = reply.Task
	args.Type = DONE
	fmt.Printf("a map task %v finish \n", reply.Task.Index)
	call("Master.WorkerHandler", &args, nil)
}

func doReduce(reply *MrReply, reducef func(string, []string) string) {
	namePrefix := "mr-"
	nameSuffix := "-" + strconv.Itoa(reply.Task.Index)

	intermedia := []KeyValue{}
	for i := 0; i < reply.FileNum; i++ {
		inName := namePrefix + strconv.Itoa(i) + nameSuffix
		inFile, err := os.Open(inName)
		if err != nil {
			log.Fatalf("File %v cannot open", inName)
		}

		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermedia = append(intermedia, kv)
		}

		inFile.Close()
	}

	sort.Sort(ByKey(intermedia))

	outFile, err := ioutil.TempFile(".", "mr-*")
	if err != nil {
		log.Fatalf("cannot create output file %v", outFile)
	}

	i := 0
	for i < len(intermedia) {
		j := i + 1
		for j < len(intermedia) && intermedia[j].Key == intermedia[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermedia[k].Value)
		}

		output := reducef(intermedia[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", intermedia[i].Key, output)

		i = j
	}

	outName := "mr-out-" + strconv.Itoa(reply.Task.Index)
	os.Rename(filepath.Join(outFile.Name()), outName)
	outFile.Close()

	args := MrArgs{}
	args.Task = reply.Task
	args.Type = DONE
	fmt.Printf("a reduce task %d finished", args.Task.Index)
	call("Master.WorkerHandler", &args, nil)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
