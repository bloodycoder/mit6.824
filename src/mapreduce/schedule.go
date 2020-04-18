package mapreduce

import (
	"fmt"
	"sync"
	"sync/atomic"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	jobList := make([]int,ntasks)
	for i:=0;i<ntasks;i++{
		jobList[i] = i
	}
	var currentTask int32= 0  //currentTask is used to index
	var finishTaskCnt int32 = 0
	var mutexForTask = &sync.Mutex{}
	var wg sync.WaitGroup
	for atomic.LoadInt32(&finishTaskCnt)<int32(ntasks){
		select{
		case workerName := <-registerChan:
			wg.Add(1)
			go func(routeWorker string) {
				for{
					mutexForTask.Lock()
					currentId := int(atomic.LoadInt32(&currentTask))
					if currentId<len(jobList) {
						currentLocalTaskId:= jobList[currentId]
						debug("currentId is  %d \n",currentId)
						debug("joblist[0]  is  %d \n",jobList[currentId])
						debug("p1 currentLocalTaskId is %d \n",currentLocalTaskId)
						atomic.AddInt32(&currentTask,1)
						mutexForTask.Unlock()
						//start do work
						args:= new(DoTaskArgs)
						args.File = mapFiles[currentLocalTaskId]
						args.JobName = jobName
						args.NumOtherPhase = n_other
						args.Phase = phase
						args.TaskNumber = int(currentLocalTaskId)
						ok := call(routeWorker, "Worker.DoTask", args, new(struct{}))
						if ok == false {
							debug("worker failed currentId %d \n",currentLocalTaskId)
							mutexForTask.Lock()
							jobList = append(jobList, currentLocalTaskId)
							mutexForTask.Unlock()
							wg.Add(-1)
							debug("worker failed close %d\n",currentLocalTaskId)
							return
						} else {
							atomic.AddInt32(&finishTaskCnt,1)
							debug("success %d\n",currentLocalTaskId)
						}
					}else{
						mutexForTask.Unlock()
					}
					if atomic.LoadInt32(&finishTaskCnt) >= int32(ntasks){
						wg.Add(-1)
						debug("work done %d\n",currentId)
						return
					}
				}
			}(workerName)
		default:
			continue
		}
	}
	//wait for job complete
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
