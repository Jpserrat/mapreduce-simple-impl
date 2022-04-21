package mapreduce

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"unicode"
)

type Worker struct {
	Stat    Status
	Address string
}

func Register(worker *Worker) {
	var reply string
	Call("Master.Register", worker, &reply, MasterAddress)
}

func (w *Worker) HandleTask(task Job, reply *WorkerResponse) error {
	switch task.JobType {
	case MapType:
		Map(task, w)
	case ReducerType:
		Reducer(task, w)
	}

	return nil
}

func Reducer(task Job, w *Worker) {

	f, err := os.Open(task.File)

	if err != nil {
		log.Printf("Error open file: ", err.Error())
	}

	defer f.Close()
	dec := json.NewDecoder(f)
	kvs := map[string][]KeyValue{}
	for {
		var kv KeyValue
		err = dec.Decode(&kv)
		if err != nil {
			break
		}
		_, ok := kvs[kv.Key]
		if !ok {
			kvs[kv.Key] = []KeyValue{}
		}
		kvs[kv.Key] = append(kvs[kv.Key], kv)
	}

	keys := []string{}
	for k := range kvs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	tempfile := fmt.Sprintf("%v-tempr", task.File)
	file, err := os.Create(tempfile)

	if err != nil {
		log.Fatal("Error creating file ", err.Error())
	}

	enc := json.NewEncoder(file)
	for _, k := range keys {
		err := enc.Encode(&KeyValue{Key: k, Value: len(kvs[k])})
		if err != nil {
			log.Fatal("Error encoding ", err.Error())
		}
	}

	var reply string

	go Call("Master.Notify", MasterNotify{JobType: ReducerType, Address: w.Address, File: tempfile}, &reply, MasterAddress)

	file.Close()
}

func Map(task Job, w *Worker) {

	f, err := os.Open(task.File)

	if err != nil {
		log.Printf("Error open file: ", err.Error())
	}

	defer f.Close()

	content, err := ioutil.ReadAll(f)

	if err != nil {
		log.Printf("Error reading file: ", err.Error())
	}

	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(string(content), ff)

	for _, w := range words {
		reduceKey := Hash(w)
		fileName := fmt.Sprintf("mr-out-%d", reduceKey)

		log.Printf("Word %v with file name %v", w, fileName)

		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

		if err != nil {
			log.Fatalln("Error reading file ", err.Error())
		}

		enc := json.NewEncoder(file)

		kv := KeyValue{Key: w, Value: 1}

		err = enc.Encode(&kv)

		// _, err = file.WriteString(fmt.Sprintf("%v %v\n", w, 1))

		if err != nil {
			log.Fatal("Error writing to file ", err.Error())
		}

		file.Close()
	}
	var reply int
	go Call("Master.Notify", MasterNotify{JobType: MapType, Address: w.Address}, &reply, MasterAddress)
}

func StartWorker(address string) {
	w := &Worker{
		Stat:    Idle,
		Address: address,
	}

	err := rpc.Register(w)

	if err != nil {
		log.Fatal("Error registering rpc ", err.Error())
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", address)

	if err != nil {
		log.Fatal("Error registering rpc ", err.Error())
	}

	l, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		log.Fatal("Error list to a port ", err.Error())
	}

	Register(w)

	rpc.Accept(l)
}
