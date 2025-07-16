package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	database map[K]V
}

type K string

type V struct {
	Value   string
	Version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.database = make(map[K]V)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	v, ok := kv.database[K(key)]
	if ok {
		reply.Value = v.Value
		reply.Version = v.Version
		reply.Err = rpc.OK
		return
	} else {
		reply.Err = rpc.ErrNoKey
		return
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	v, ok := kv.database[K(key)]
	if ok {
		if v.Version != args.Version {
			reply.Err = rpc.ErrVersion
			return
		} else {
			v.Value = args.Value
			v.Version++
			kv.database[K(key)] = v
			reply.Err = rpc.OK
			return
		}
	} else {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		} else {
			kv.database[K(key)] = V{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
			return
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
