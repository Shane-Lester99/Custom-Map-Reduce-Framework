package mapreduce

import (
	"bufio"
	"container/list"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

const (
	nNumber = 100000
	nMap    = 100
	nReduce = 50
)

// Create input file with N numbers
// Check if we have N numbers in output file

// // Split in words
// func MapFunc(value string) *list.List {
// 	Dlog.Println("Map val: ", value[0:2])
// 	DPrintf("Map %v\n", value)
// 	res := list.New()
// 	words := strings.Fields(value)
// 	for _, w := range words {
// 		kv := KeyValue{w, ""}
// 		res.PushBack(kv)
// 	}
// 	Dlog.Println("Items in list: ", res.Len())
// 	return res
// }
//
// // Just return key
// func ReduceFunc(key string, values *list.List) string {
// 	for e := values.Front(); e != nil; e = e.Next() {
// 		DPrintf("Reduce %s %v\n", key, e.Value)
// 	}
// 	return ""
// }
//
// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, file string) {
	input, err := os.Open(file)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer input.Close()
	output, err := os.Open("mrtmp." + file)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	inputScanner := bufio.NewScanner(input)
	for inputScanner.Scan() {
		lines = append(lines, inputScanner.Text())
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i += 1
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInput() string {
	name := "824-mrinput.txt"
	file, err := os.Create(name)
	if err != nil {
		log.Fatal("mkInput: ", err)
	}
	w := bufio.NewWriter(file)
	for i := 0; i < nNumber; i++ {
		fmt.Fprintf(w, "%d\n", i)
	}
	w.Flush()
	file.Close()
	return name
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *MapReduce {
	file := makeInput()
	master := port("master")
	mr := MakeMapReduce(nMap, nReduce, file, master)
	return mr
}

func cleanup(mr *MapReduce) {
	mr.CleanupFiles()
	RemoveFile(mr.file)
}

func createLogger() *os.File {
	debug := "./debug_mr.txt"
	if _, err := os.Stat(debug); err == nil {
		os.Remove(debug)
	}
	f, err := os.Create(debug)
	if err != nil {
		panic(err)
	}
	Dlog = log.New(f, "", 3)
	return f
}

func TestQueue(t *testing.T) {
	var q = new(JobQueue)
	q.InitQueue()
	if q.Len() != 0 {
		err := "Should be length 0 after initialization"
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if q.Front() != -1 && q.Len() == 0 && q.Dequeue() != -1 {
			err := "Should be -1 at front of queue after initialization and length be 0."
			t.Fatal(err)
		}

	}
	initLength := 20
	q.Fill(initLength)
	if q.Len() != initLength {
		err := fmt.Sprintf("After filling with length %d Len fails with length %d",
			initLength, q.Len())
		t.Fatal(err)
	}
	var jobId int
	for i := 0; i < initLength; i++ {
		if q.Len() != initLength-i {
			err := fmt.Sprintf("Queue is of length %d but should be of length %d",
				q.Len(), initLength-i)
			t.Fatal(err)
		}
		jobId = q.Dequeue()
		if jobId != i {
			err := fmt.Sprintf("Job in deque %d doesn't equal job in iter %d",
				jobId, i)
			t.Fatal(err)
		}
	}
	if q.Len() != 0 {
		err := fmt.Sprintf("Queue is of length %d but should be of length %d",
			q.Len(), 0)
		t.Fatal(err)
	}
	q.Enqueue(1)
	q.Enqueue(2)
	q.Dequeue()
	q.Enqueue(2)
	q.Enqueue(3)
	q.Dequeue()
	q.Dequeue()
	if q.Len() != 1 && q.Front() != 1 {
		err := fmt.Sprintf("Queue should of length 1 with 1 in front.")
		t.Fatal(err)
	}
}

func TestBasic(t *testing.T) {
	ChooseFunctions("test")
	f := createLogger()
	defer f.Close()
	fmt.Printf("Test: Basic mapreduce ...\n")
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(i)),
			MyMapper, MyReducer, -1)
	}
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... Basic Passed\n")
}

func TestOneFailure(t *testing.T) {
	ChooseFunctions("test")
	f := createLogger()
	defer f.Close()
	fmt.Printf("Test: One Failure mapreduce ...\n")
	mr := setup()
	// Start 2 workers that fail after 10 jobs
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(0)),
		MyMapper, MyReducer, 10)
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(1)),
		MyMapper, MyReducer, -1)
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... One Failure Passed\n")
}

func TestManyFailures(t *testing.T) {
	ChooseFunctions("test")
	createLogger()
	fmt.Printf("Test: One ManyFailures mapreduce ...\n")
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.DoneChannel:
			check(t, mr.file)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 jobs
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, MyMapper, MyReducer, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, MyMapper, MyReducer, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Printf("  ... Many Failures Passed\n")
}
