package mapreduce

import (
	"hash/fnv"
	"net/rpc"
)

const (
	MasterAddress string = ":4444"
	ReducerNum    int    = 4
	MapperNum     int    = 4
)

type JobType int64

const (
	MapType JobType = iota
	ReducerType
)

type ByKey []KeyValue

func (a ByKey) Len() int { return len(a) }

func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type Job struct {
	File    string
	JobType JobType
}

type WorkerResponse struct {
	File string
}

type MasterNotify struct {
	JobType JobType
	Address string
	File    string
}

type KeyValue struct {
	Key   string
	Value int
}

func Hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % ReducerNum
}

func Call(fn string, args interface{}, reply interface{}, address string) error {
	client, err := rpc.Dial("tcp", address)

	if err != nil {
		return err
	}

	err = client.Call(fn, args, reply)

	if err != nil {
		return err
	}

	return nil
}
