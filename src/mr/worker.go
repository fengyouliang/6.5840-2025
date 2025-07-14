package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
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

	const maxRetries = 3
	const waitTime = 100 * time.Millisecond
	const coordinatorGoneRetries = 10 // Increased retries for crash test

	coordinatorGoneCount := 0

	for {
		reply, err := callRequestWithRetry(maxRetries)
		if err != nil {
			coordinatorGoneCount++
			if coordinatorGoneCount >= coordinatorGoneRetries {
				log.Printf("Worker: Coordinator appears to be gone, exiting")
				return
			}
			log.Printf("Worker: Failed to get task, attempt %d/%d: %v",
				coordinatorGoneCount, coordinatorGoneRetries, err)
			time.Sleep(time.Second)
			continue
		}

		// Reset counter on successful communication
		coordinatorGoneCount = 0

		switch reply.OptionType {
		case 0: // Map task
			if err := handleMapTask(mapf, reply); err != nil {
				log.Printf("Worker: Map task failed: %v", err)
				continue
			}
		case 1: // Reduce task
			if err := handleReduceTask(reducef, reply); err != nil {
				log.Printf("Worker: Reduce task failed: %v", err)
				continue
			}
		case 2: // Wait
			time.Sleep(waitTime)
			continue
		case 3: // Done
			log.Printf("Worker: All tasks completed, exiting")
			return
		default:
			log.Printf("Worker: Unknown option type: %d", reply.OptionType)
			continue
		}
	}
}

func callRequestWithRetry(maxRetries int) (WorkerReply, error) {
	var reply WorkerReply
	var err error

	for i := 0; i < maxRetries; i++ {
		reply, err = CallRequest()
		if err == nil {
			return reply, nil
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
		}
	}
	return WorkerReply{}, err
}

func handleMapTask(mapf func(string, string) []KeyValue, reply WorkerReply) error {
	tempFileNames, err := doMap(mapf, reply)
	if err != nil {
		return fmt.Errorf("map task execution failed: %w", err)
	}

	args := WorkerArgs{
		ID:            reply.ID,
		File:          reply.File,
		OptionType:    reply.OptionType,
		TempFileNames: tempFileNames,
	}

	return callCompleteWithRetry(args, 3)
}

func handleReduceTask(reducef func(string, []string) string, reply WorkerReply) error {
	if err := doReduce(reducef, reply); err != nil {
		return fmt.Errorf("reduce task execution failed: %w", err)
	}

	args := WorkerArgs{
		ID:          reply.ID,
		File:        reply.File,
		OptionType:  reply.OptionType,
		ReduceIndex: reply.ReduceIndex,
	}

	return callCompleteWithRetry(args, 3)
}

func callCompleteWithRetry(args WorkerArgs, maxRetries int) error {
	for i := 0; i < maxRetries; i++ {
		if err := CallComplete(args); err == nil {
			return nil
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
		}
	}
	return fmt.Errorf("failed to complete task after %d retries", maxRetries)
}

func doMap(mapf func(string, string) []KeyValue, reply WorkerReply) ([]string, error) {
	filename := reply.File
	log.Printf("Worker: Starting map task for file: %s (ID: %d)", filename, reply.ID)

	// Read input file
	content, err := readInputFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read input file %s: %w", filename, err)
	}

	// Apply map function
	kva := mapf(filename, content)
	log.Printf("Worker: Map function produced %d key-value pairs", len(kva))

	// Create intermediate files
	nReduce := reply.NumReduce
	finalPaths, err := createIntermediateFiles(reply.ID, nReduce, kva)
	if err != nil {
		return nil, fmt.Errorf("failed to create intermediate files: %w", err)
	}

	log.Printf("Worker: Map task completed successfully, created %d intermediate files", len(finalPaths))
	return finalPaths, nil
}

func readInputFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func createIntermediateFiles(taskID, nReduce int, kva []KeyValue) ([]string, error) {
	// Create temporary files
	tempFiles := make([]*os.File, nReduce)
	tempFileNames := make([]string, nReduce)
	writers := make([]*bufio.Writer, nReduce)

	// Initialize all resources
	for i := 0; i < nReduce; i++ {
		tempFileName := fmt.Sprintf("mr-%d-%d.tmp", taskID, i)
		file, err := os.Create(tempFileName)
		if err != nil {
			cleanupFiles(tempFiles, tempFileNames, i)
			return nil, fmt.Errorf("failed to create temporary file %s: %w", tempFileName, err)
		}
		tempFiles[i] = file
		tempFileNames[i] = tempFileName
		writers[i] = bufio.NewWriter(file)
	}

	// Write key-value pairs to appropriate files
	for _, kv := range kva {
		reducePartition := ihash(kv.Key) % nReduce
		if _, err := fmt.Fprintf(writers[reducePartition], "%s %s\n", kv.Key, kv.Value); err != nil {
			cleanupFiles(tempFiles, tempFileNames, nReduce)
			return nil, fmt.Errorf("failed to write to temporary file: %w", err)
		}
	}

	// Flush all writers
	for _, writer := range writers {
		if err := writer.Flush(); err != nil {
			cleanupFiles(tempFiles, tempFileNames, nReduce)
			return nil, fmt.Errorf("failed to flush writer: %w", err)
		}
	}

	// Close all files
	for _, file := range tempFiles {
		if err := file.Close(); err != nil {
			cleanupFiles(nil, tempFileNames, nReduce)
			return nil, fmt.Errorf("failed to close temporary file: %w", err)
		}
	}

	// Atomically rename files
	finalPaths := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		finalFileName := fmt.Sprintf("mr-%d-%d", taskID, i)
		if err := os.Rename(tempFileNames[i], finalFileName); err != nil {
			cleanupRenamedFiles(finalPaths, tempFileNames, i, nReduce)
			return nil, fmt.Errorf("failed to rename %s to %s: %w", tempFileNames[i], finalFileName, err)
		}
		finalPaths[i] = finalFileName
	}

	return finalPaths, nil
}

func cleanupFiles(files []*os.File, names []string, count int) {
	for i := 0; i < count; i++ {
		if files != nil && files[i] != nil {
			files[i].Close()
		}
		if names != nil && names[i] != "" {
			os.Remove(names[i])
		}
	}
}

func cleanupRenamedFiles(finalPaths, tempNames []string, renameIndex, total int) {
	for i := 0; i < renameIndex; i++ {
		if finalPaths[i] != "" {
			os.Remove(finalPaths[i])
		}
	}
	for i := renameIndex; i < total; i++ {
		if tempNames[i] != "" {
			os.Remove(tempNames[i])
		}
	}
}

func doReduce(reducef func(string, []string) string, reply WorkerReply) error {
	log.Printf("Worker: Starting reduce task for partition %d", reply.ReduceIndex)

	// Read all intermediate files for this reduce partition
	intermediateData, err := readIntermediateFiles(reply.NumMapper, reply.ReduceIndex)
	if err != nil {
		return fmt.Errorf("failed to read intermediate files: %w", err)
	}

	if len(intermediateData) == 0 {
		log.Printf("Worker: No intermediate data found for reduce partition %d", reply.ReduceIndex)
		// Always create output file, even if empty
		return createEmptyOutputFile(reply.ReduceIndex)
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(intermediateData))
	for k := range intermediateData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create output file
	if err := createOutputFile(reply.ReduceIndex, keys, intermediateData, reducef); err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}

	log.Printf("Worker: Reduce task completed successfully for partition %d", reply.ReduceIndex)
	return nil
}

func createEmptyOutputFile(reduceIndex int) error {
	outputFileName := fmt.Sprintf("mr-out-%d", reduceIndex)
	file, err := os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf("failed to create empty output file %s: %w", outputFileName, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close empty output file %s: %w", outputFileName, err)
	}
	return nil
}

func createOutputFile(reduceIndex int, keys []string, intermediateData map[string][]string, reducef func(string, []string) string) error {
	outputTempFileName := fmt.Sprintf("mr-out-%d.tmp", reduceIndex)
	outputTempFile, err := os.Create(outputTempFileName)
	if err != nil {
		return fmt.Errorf("failed to create temporary output file %s: %w", outputTempFileName, err)
	}

	outputWriter := bufio.NewWriter(outputTempFile)

	for _, key := range keys {
		values := intermediateData[key]
		result := reducef(key, values)
		if _, err := fmt.Fprintf(outputWriter, "%s %s\n", key, result); err != nil {
			outputTempFile.Close()
			os.Remove(outputTempFileName)
			return fmt.Errorf("failed to write to output file: %w", err)
		}
	}

	if err := outputWriter.Flush(); err != nil {
		outputTempFile.Close()
		os.Remove(outputTempFileName)
		return fmt.Errorf("failed to flush output file: %w", err)
	}

	if err := outputTempFile.Close(); err != nil {
		os.Remove(outputTempFileName)
		return fmt.Errorf("failed to close temporary output file: %w", err)
	}

	finalOutputFileName := fmt.Sprintf("mr-out-%d", reduceIndex)
	if err := os.Rename(outputTempFileName, finalOutputFileName); err != nil {
		os.Remove(outputTempFileName)
		return fmt.Errorf("failed to rename output file: %w", err)
	}

	return nil
}

func readIntermediateFiles(numMapper, reduceIndex int) (map[string][]string, error) {
	intermediateData := make(map[string][]string)

	for i := 0; i < numMapper; i++ {
		intermediateFilePath := fmt.Sprintf("mr-%d-%d", i, reduceIndex)

		if _, err := os.Stat(intermediateFilePath); os.IsNotExist(err) {
			continue
		}

		file, err := os.Open(intermediateFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open intermediate file %s: %w", intermediateFilePath, err)
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			parts := strings.SplitN(line, " ", 2)
			if len(parts) != 2 {
				continue
			}

			key, value := parts[0], parts[1]
			intermediateData[key] = append(intermediateData[key], value)
		}

		file.Close()

		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("error reading intermediate file %s: %w", intermediateFilePath, err)
		}
	}

	return intermediateData, nil
}

// RPC functions
func CallRequest() (WorkerReply, error) {
	args := WorkerArgs{}
	reply := WorkerReply{}

	if ok := call("Coordinator.WorkerRequest", &args, &reply); !ok {
		return WorkerReply{}, fmt.Errorf("RPC call failed")
	}
	return reply, nil
}

func CallComplete(args WorkerArgs) error {
	reply := WorkerReply{}
	if ok := call("Coordinator.WorkerComplete", &args, &reply); !ok {
		return fmt.Errorf("RPC call failed")
	}
	return nil
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
