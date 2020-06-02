package mapreduce

import (
	"container/list"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Inforamtion held about the worker to help master
type WorkerInfo struct {
	file    string
	jobNum  int
	jobType JobType
	alive   bool
}

// Global id is given to nodes when they fail, we start with 2, {0,1} and as
// nodes die we will create unique addresses with this number. It must be surronded
// by a mutex lock so that multiple nodes don't have the same address
var mux = sync.Mutex{}
var jq JobQueue
var res chan string

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	Dlog.Printf("Step 15/16: All data processed. Killing all workers.")
	l := list.New()
	for addr := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", addr)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(addr, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", addr)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) isNodeDead(addr string) bool {
	if mr.Workers[addr].alive == false {
		return true
	}
	mesArg := &PingArgs{}
	mesRep := &PingReply{OK: false}
	ok := call(addr, "Worker.IsAlive", mesArg, mesRep)
	time.Sleep(time.Millisecond * 1)
	alive := mesRep.OK && ok
	return !alive
}

// Will call a worker node given it address to do its assigned job. It is meant
// to be called in a goroutine. It will return  an address upon completion.
// if the job failed (the worker didnt respond) a new worker will be allocated to
// do the job. The old one will be deleted. If the job fails, the workerinfo
// will need a reboot (WorkerInfo.needsReboot). This signifies the job wasnt done
func (mr *MapReduce) CallJobs(addr string) {
	if mr.isNodeDead(addr) == true {
		mux.Lock()
		Dlog.Printf("Step 11/12/13/14: Uh-oh! Worker failure %s for job %d. No longer alive.\n",
			addr, mr.Workers[addr].jobNum)
		jq.Enqueue(mr.Workers[addr].jobNum)
		mr.Workers[addr].alive = false
		mux.Unlock()
		runtime.Goexit()
	}
	var numOtherPhase int
	if mr.Workers[addr].jobType == Map {
		numOtherPhase = mr.nReduce
	} else if mr.Workers[addr].jobType == Reduce {
		numOtherPhase = mr.nMap
	} else {
		err := fmt.Sprintf("Called job on worker assigned no role: addr: %s jobNum: %d role: %s. Reassign to %s",
			addr, mr.Workers[addr].jobNum, mr.Workers[addr].jobType, mr.State)
		panic(err)

	}
	jobArg := &DoJobArgs{File: mr.Workers[addr].file,
		Operation: mr.Workers[addr].jobType, JobNumber: mr.Workers[addr].jobNum,
		NumOtherPhase: numOtherPhase}
	jobReply := &DoJobReply{}
	call(addr, "Worker.DoJob", jobArg, jobReply)
	res <- addr
	mr.checkIfJobDoneCorrect(mr.Workers[addr].jobType, mr.Workers[addr].jobNum, addr)
}

func (mr *MapReduce) checkIfJobDoneCorrect(job JobType, jobNum int, addr string) {
	var tempFileBase string
	var tempFileArr []string
	mux.Lock()
	if job == Map {
		tempFileBase = fmt.Sprintf("mrtmp.824-mrinput.txt-%d-*", jobNum)
		tempFileArr, _ = filepath.Glob(tempFileBase)
		if len(tempFileArr) != mr.nReduce {
			Dlog.Printf("Job %d not completed correctly by %s. Placing back in queue.", jobNum, addr)
			jq.Enqueue(jobNum)
		}
	}
	if job == Reduce {
		tempFileBase = fmt.Sprintf("mrtmp.824-mrinput.txt-res-%d", jobNum)
		tempFileArr, _ = filepath.Glob(tempFileBase)
		if len(tempFileArr) != 1 {
			Dlog.Printf("Job %d not completed correctly by %s. Placing back in queue.", jobNum, addr)
			jq.Enqueue(jobNum)
		}
	}
	mux.Unlock()
}

// This will set all the workers to jobs (either Map or Reduce or Idle)
func (mr *MapReduce) scheduleJobs(job JobType) {
	mux.Lock()
	switch {
	case job == Map:
		Dlog.Println("Step 10: Reassinging nodes to work on ", job)
	case job == Reduce:
		Dlog.Println("Step 12/13: Reassinging nodes to work on ", job)
	}
	for addr := range mr.Workers {
		mr.Workers[addr].jobType = job
	}
	mr.State = job
	mux.Unlock()
}

func (mr *MapReduce) handleRegistrations() {
	Dlog.Println("Step 9: Creating routine to handle registrations")
	for {
		addr := <-mr.registerChannel
		mux.Lock()
		Dlog.Println("Step 9/10/11/12/13/14 Registering new worker with addr ", addr)
		newWorker := new(WorkerInfo)
		newWorker.file = mr.file
		newWorker.jobType = mr.State
		newWorker.alive = true
		mr.Workers[addr] = newWorker
		mux.Unlock()
		if len(mr.Workers) >= 2 {
			res <- addr
		}
	}
}

func (mr *MapReduce) waitForWorkers() {
	for {
		if len(mr.Workers) >= 2 {
			break
		}
	}
	Dlog.Println("Step 9: Registered workers. Ready to Begin setting them to work.")
}

// This runs the map or reduce task.
func (mr *MapReduce) runMapOrReduce(job JobType) {
	var nJobs int
	var debugStep [2]string
	switch job {
	case Map:
		nJobs = mr.nMap
		debugStep[0] = "11"
		debugStep[1] = "11/12"
	case Reduce:
		nJobs = mr.nReduce
		debugStep[0] = "13"
		debugStep[1] = "13/14"
	default:
		panic("Must be map or reduce job. Exiting.")
	}
	mux.Lock()
	jq.Fill(nJobs)
	mux.Unlock()
	for addr := range mr.Workers {
		mux.Lock()
		front := jq.Front()
		mux.Unlock()
		mux.Lock()
		if front != -1 {
			mr.Workers[addr].jobNum = jq.Dequeue()
			Dlog.Printf("Step %s: Calling job on worker: address -> %s job -> %s job number: %d",
				debugStep[0], addr, mr.Workers[addr].jobType,
				mr.Workers[addr].jobNum)
			mux.Unlock()
			go mr.CallJobs(addr)
		} else {
			panic("Not enough jobs to early.")
		}
	}
	time.Sleep(3 * time.Millisecond)
	count := 0
	for {
		if count >= nJobs && jq.Len() == 0 {
			break
		}
		count++
		addr := <-res
		time.Sleep(2 * time.Millisecond)
		Dlog.Printf("Step %s: Finished job with address -> %s job -> %s job number -> %d",
			debugStep[1], addr, mr.Workers[addr].jobType, mr.Workers[addr].jobNum)
		mux.Lock()
		front := jq.Front()
		mux.Unlock()
		if front != -1 {
			mux.Lock()
			mr.Workers[addr].jobNum = jq.Dequeue()
			Dlog.Printf("Step %s: Calling job on worker: address -> %s job -> %s job number: %d",
				debugStep[0], addr, mr.Workers[addr].jobType, mr.Workers[addr].jobNum)
			mux.Unlock()
			go mr.CallJobs(addr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Main method, this will do all the work of master for the framework
func (mr *MapReduce) RunMaster() *list.List {
	Dlog.Println("Step 9: Building shared respose channel and job queue")
	res = make(chan string)
	jq.InitQueue()
	go mr.handleRegistrations()
	mr.waitForWorkers()
	mr.scheduleJobs(Map)
	mr.runMapOrReduce(Map)
	mr.scheduleJobs(Reduce)
	mr.runMapOrReduce(Reduce)
	return mr.KillWorkers()
}
