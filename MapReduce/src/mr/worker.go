package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const OutDir = "."

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := strconv.Itoa(os.Getpid())

	for {
		time.Sleep(time.Second)
		args := GetTaskArgs{WorkerID: workerID}
		reply := GetTaskReply{}

		call("Master.GetTask", &args, &reply)

		filename, taskID, nReduce, taskType := reply.File, reply.ID, reply.NReduce, reply.Type
		if taskID == -1 {
			continue
		}

		if taskType == Map {
			HandleMap(filename, taskID, nReduce, mapf)
		} else {
			HandleReduce(taskID, reducef)
		}

		reportTaskArgs := ReportTaskArgs{WorkerID: workerID, TaskID: taskID, TaskType: taskType}
		reportTaskReply := ReportTaskReply{}

		call("Master.ReportTask", &reportTaskArgs, &reportTaskReply)
	}
}

func HandleMap(filename string, taskID int, nReduce int, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("read %s: %v", filename, err)
	}

	kva := mapf(filename, string(content))
	buckets := make([]string, nReduce)

	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] += fmt.Sprintf("%s %s\n", kv.Key, kv.Value)
	}

	for bucket := range nReduce {
		filename := fmt.Sprintf("mr_%d_%d.out", bucket, taskID)
		err = os.WriteFile(filename, []byte(buckets[bucket]), 0644)
		if err != nil {
			log.Fatalf("write file %s: %v", filename, err)
		}
	}
}

func HandleReduce(taskID int, reducef func(string, []string) string) {
	dict := map[string][]string{}

	entries, err := os.ReadDir(OutDir)
	if err != nil {
		log.Fatalf("read folder %s: %v", OutDir, err)
	}

	pattern := fmt.Sprintf("mr_%d_", taskID)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), pattern) {
			content, err := os.ReadFile(entry.Name())
			if err != nil {
				log.Fatalf("read file %s: %v", entry.Name(), err)
			}

			text := strings.TrimSpace(string(content))
			if text == "" {
				continue
			}

			for line := range strings.SplitSeq(text, "\n") {
				kv := strings.Split(line, " ")
				dict[kv[0]] = append(dict[kv[0]], kv[1])
			}
		}
	}

	keys := make([]string, 0, len(dict))
	for key := range dict {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var output strings.Builder
	for _, key := range keys {
		value := reducef(key, dict[key])
		fmt.Fprintf(&output, "%s %s\n", key, value)
	}

	filename := fmt.Sprintf("mr-out-%d.out", taskID)
	err = os.WriteFile(filename, []byte(output.String()), 0644)
	if err != nil {
		log.Fatalf("write file %s: %v", filename, err)
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
