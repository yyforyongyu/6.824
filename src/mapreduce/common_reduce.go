package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	kvFinalSlice := []KeyValue{}

	for m := 0; m < nMap; m++ {

		kvSlice := []KeyValue{}

		filename := reduceName(jobName, m, reduceTask)
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		defer file.Close()

		if err != nil {
			log.Fatalf("error opening file in reduce: %s", filename)
		}

		dec := json.NewDecoder(file)
		for {
			if err := dec.Decode(&kvSlice); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("error decoding in reduce: %s", err)
			}
		}

		kvFinalSlice = append(kvFinalSlice, kvSlice...)
	}

	file, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	defer file.Close()
	if err != nil {
		log.Fatalf("error opening outFile: %s", err)
	}

	kvMap := map[string][]string{}
	for _, kv := range kvFinalSlice {
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}

	for key, values := range kvMap {
		reducedValue := reduceF(key, values)
		enc := json.NewEncoder(file)
		if err := enc.Encode(KeyValue{key, reducedValue}); err != nil {
			log.Fatalf("error writing json in reduce: %s", err)
		}
	}
}
