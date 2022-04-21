package main

import (
	"os"
	"serrats/mapreduce/mapreduce"
)

func main() {

	switch os.Args[1] {
	case "master":
		mapreduce.StartMaster()
	case "worker":
		mapreduce.StartWorker(os.Args[2])
	default:
		mapreduce.StartMaster()
	}
}
