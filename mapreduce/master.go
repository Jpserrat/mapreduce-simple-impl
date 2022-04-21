package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
)

type Status int64

const (
	Idle Status = iota
	InProgress
	Completed
)

type Master struct {
	Workers      []Worker
	Address      string
	mapTurn      int
	mapTaskCount int
	reducerCount int
	reducerDone  chan MasterNotify
	kvs          []KeyValue
}

func (m *Master) Register(w Worker, reply *string) error {
	log.Print("Worker register with address: ", w.Address)
	m.Workers = append(m.Workers, w)
	reply = nil
	return nil
}

func (m *Master) Notify(notify MasterNotify, reply *string) error {
	log.Println("Worker done with address ", notify.Address)
	switch notify.JobType {
	case MapType:
		{
			m.mapTaskCount--
			fmt.Print("map task count ", m.mapTaskCount)
			if m.mapTaskCount == 0 {
				fmt.Println("done")
				StartReducer(m)
			}
		}
	case ReducerType:
		{
			fmt.Println("Reducer finished")
			m.reducerDone <- notify
		}
	default:
		fmt.Print("Default case ", notify.JobType)
	}
	reply = nil
	return nil
}

func Merge(m *Master) {
	for i := 0; i < ReducerNum; i++ {
		fmt.Println("Waiting for reducer")
		notify := <-m.reducerDone
		fmt.Print("Starting reducer ", i)
		f, err := os.Open(notify.File)
		if err != nil {
			log.Fatal("[Merge]: Error opening file ", err.Error())
		}

		dec := json.NewDecoder(f)

		for {
			kv := KeyValue{}
			err = dec.Decode(&kv)

			if err != nil {
				break
			}

			m.kvs = append(m.kvs, kv)
			fmt.Print("Finished reducer ", i)
		}

		sort.Sort(ByKey(m.kvs))
	}

	sort.Sort(ByKey(m.kvs))
	f := createFile("mr-out-result")
	w := bufio.NewWriter(f)
	for _, k := range m.kvs {
		fmt.Fprintf(w, "%v %v\n", k.Key, k.Value)
	}
	w.Flush()
}

func StartReducer(m *Master) {
	for i := 0; i < ReducerNum; i++ {
		job := &Job{File: fmt.Sprintf("mr-out-%d", i), JobType: ReducerType}
		var res WorkerResponse
		go Call("Worker.HandleTask", job, &res, m.Workers[i].Address)
		m.reducerCount++
	}
}

func SubmitTask(path string, m *Master) {
	job := &Job{File: path, JobType: MapType}
	var res WorkerResponse
	go Call("Worker.HandleTask", job, &res, m.Workers[m.mapTurn%MapperNum].Address)
	m.mapTurn++
	m.mapTaskCount++
}

func WaitTask(m *Master) {

	buf := bufio.NewReader(os.Stdin)

	s, _, err := buf.ReadLine()

	if err != nil {
		log.Fatal("Error reading user input: ", err.Error())
	}

	log.Print(len(m.Workers))

	if len(m.Workers) == 0 {
		log.Fatal("No Workers register")
	}

	files, err := ioutil.ReadDir(string(s))

	if err != nil {
		log.Fatal("Error reading dir: ", err.Error())
	}
	go Merge(m)

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".txt") {
			SubmitTask(file.Name(), m)
		}
	}
}

func createIntermediateFiles(m Master) {
	for i := 0; i < ReducerNum; i++ {
		createFile(fmt.Sprintf("mr-out-%d", i))
	}
}

func createFile(filename string) *os.File {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("Error creating intermediate files: ", err.Error())
	}

	log.Println("Intermediate file created with name ", f.Name())
	return f
}

func StartMaster() {
	m := Master{
		Workers:      make([]Worker, 0, 100),
		Address:      MasterAddress,
		mapTurn:      0,
		mapTaskCount: 0,
		reducerDone:  make(chan MasterNotify),
	}
	err := rpc.Register(&m)

	if err != nil {
		log.Fatal("Error registering rpc ", err.Error())
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", MasterAddress)

	if err != nil {
		log.Fatal("Error registering rpc ", err.Error())
	}

	l, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		log.Fatal("Error list to a port ", err.Error())
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		s := <-sigChan
		for i := 0; i < ReducerNum; i++ {
			os.Remove(fmt.Sprintf("mr-out-%d", i))
		}

		log.Fatalln("shuting down master with signal", s.String())
	}()

	createIntermediateFiles(m)

	go WaitTask(&m)

	rpc.Accept(l)
}
