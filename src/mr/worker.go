package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	// uncomment to send the Example RPC to the coordinator.
	for {
		reply, err := CallRequest()
		if err != nil {
			log.Fatal("CallRequest error:", err)
		}
		optionType := reply.OptionType
		if optionType == 0 {
			tempFileNames, err := doMap(mapf, reply)
			if err != nil {
				log.Fatal("doMap error:", err)
			}
			CallComplete(WorkerArgs{ID: reply.ID, File: reply.File, OptionType: optionType, TempFileNames: tempFileNames})
		} else if optionType == 1 {
			doReduce(reducef, reply)
			CallComplete(WorkerArgs{ID: reply.ID, File: reply.File, OptionType: optionType, ReduceIndex: reply.ReduceIndex})
		} else {
			log.Printf("done")
			break
		}
	}

}

func doMap(mapf func(string, string) []KeyValue, reply WorkerReply) ([]string, error) {
	filename := reply.File
	fmt.Printf("Map: %#v\n", reply)

	// 使用map func得到中间结果
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %#v\n", reply)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %#v\n", reply)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 保存中间结果到文件中
	nReduce := reply.NumReduce
	tempFiles := make([]*os.File, nReduce)
	tempFileNames := make([]string, nReduce)
	writers := make([]*bufio.Writer, nReduce)

	for i := range nReduce {
		tempFileName := fmt.Sprintf("mr-%d-%d.tmp", reply.ID, i)
		file, err := os.Create(tempFileName)
		if err != nil {
			for j := 0; j < i; j++ {
				os.Remove(tempFileNames[j])
			}
			return nil, fmt.Errorf("failed to create temporary File %s: %w", tempFileName, err)
		}
		tempFiles[i] = file
		tempFileNames[i] = tempFileName
		writers[i] = bufio.NewWriter(file)
	}

	for _, kv := range kva {
		reducePartition := ihash(kv.Key) % nReduce
		if _, err := fmt.Fprintf(writers[reducePartition], "%v %v\n", kv.Key, kv.Value); err != nil {
			for _, f := range tempFiles {
				if f != nil {
					f.Close()
				}
			}
			for _, name := range tempFileNames {
				os.Remove(name)
			}
			return nil, fmt.Errorf("failed to write to temporary File: %w", err)
		}
	}

	for _, w := range writers {
		if w != nil {
			if err := w.Flush(); err != nil {
				fmt.Printf("Warning: failed to flush writer: %v\n", err)
			}
		}
	}

	for _, tempFile := range tempFiles {
		if err := tempFile.Close(); err != nil {
			return nil, fmt.Errorf("failed to close temporary File: %w", err)
		}
	}

	finalIntermediateFilePaths := make([]string, nReduce)
	for i := range nReduce {
		finalFileName := fmt.Sprintf("mr-%d-%d", reply.ID, i)
		if err := os.Rename(tempFileNames[i], finalFileName); err != nil {
			// 如果重命名失败，这是一个严重错误。进行清理。
			for j := 0; j < i; j++ { // Remove already renamed files
				os.Remove(finalIntermediateFilePaths[j])
			}
			for j := i; j < nReduce; j++ { // Remove remaining temp files
				os.Remove(tempFileNames[j])
			}
			return nil, fmt.Errorf("failed to rename temporary File %s to %s: %w", tempFileNames[i], finalFileName, err)
		}
		finalIntermediateFilePaths[i] = finalFileName
	}

	return finalIntermediateFilePaths, nil
}

func doReduce(reducef func(string, []string) string, reply WorkerReply) {
	fmt.Printf("Reduce: %#v\n", reply)
	intermediateData := make(map[string][]string)
	for i := 0; i < reply.NumMapper; i++ {
		intermediateFilePath := fmt.Sprintf("mr-%d-%d", i, reply.ReduceIndex)
		if _, err := os.Stat(intermediateFilePath); os.IsNotExist(err) {
			log.Printf("Worker: Intermediate file %s not found (Map task %d may have failed or produced no output for this reduce partition). Skipping.", intermediateFilePath, i)
			continue
		} else if err != nil {
			log.Fatalf("error checking intermediate file %s: %w", intermediateFilePath, err)
		}
		file, err := os.Open(intermediateFilePath)
		if err != nil {
			log.Fatalf("failed to open intermediate file %s: %w", intermediateFilePath, err)
		}
		defer file.Close() // Ensure file is closed after reading

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Fields(line) // Split by whitespace
			if len(parts) == 2 {
				key := parts[0]
				value := parts[1]
				intermediateData[key] = append(intermediateData[key], value)
			} else {
				log.Printf("Worker: Warning: Malformed intermediate line in %s: %s", intermediateFilePath, line)
			}
		}

		keys := make([]string, 0, len(intermediateData))
		for k := range intermediateData {
			keys = append(keys, k)
		}
		sort.Strings(keys) // Sort keys alphabetically

		outputTempFileName := fmt.Sprintf("mr-out-%d.tmp", reply.ReduceIndex)
		outputTempFile, err := os.Create(outputTempFileName)
		if err != nil {
			log.Fatalf("failed to create output file %s: %w", outputTempFileName, err)
		}
		outputTeamWriter := bufio.NewWriter(outputTempFile)
		defer outputTempFile.Close()

		for _, key := range keys {
			count := reducef(key, intermediateData[key])
			if _, err := fmt.Fprintf(outputTeamWriter, "%s %s\n", key, count); err != nil {
				log.Fatalf("failed to write to output file %s: %w", outputTempFileName, err)
			}

		}

		if err := outputTeamWriter.Flush(); err != nil {
			log.Printf("Worker: Warning: failed to flush output writer: %v", err)
		}

		// 5. Atomic rename final output file
		// 5. 原子性重命名最终输出文件
		finalOutputFileName := fmt.Sprintf("mr-out-%d", reply.ReduceIndex)
		if err := os.Rename(outputTempFileName, finalOutputFileName); err != nil {
			os.Remove(outputTempFileName) // Clean up temp file if rename fails
		}

		log.Printf("Reduce: Output file %s written.\n", outputTempFileName)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallRequest() (WorkerReply, error) {

	// declare an argument structure.
	args := WorkerArgs{}
	// declare a reply structure.
	reply := WorkerReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.WorkerRequest", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return WorkerReply{}, fmt.Errorf("call failed!\n")
	}
}

func CallComplete(args WorkerArgs) {
	reply := WorkerReply{}
	ok := call("Coordinator.WorkerComplete", &args, &reply)
	if !ok {
		log.Fatal("call failed!")
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

func main() {
	testMap()
}

func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
func testMap() {
	reply := WorkerReply{ID: 1, File: "pg-grimm.txt", OptionType: 0, NumReduce: 2}
	doMap(Map, reply)
}
