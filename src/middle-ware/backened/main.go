package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"middle-ware/util"
	"middle-ware/util/concurrent_map"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
)

var BatchCount int32
var FinishProcessCount int32
var CurrentBatch int32
var TIBList []*TraceIdBatch
var TraceCheckSumMap concurrent_map.ConcurrentMap
var logger *log.Logger
func main() {
	file,_ := os.Create("/error.log")
	logger = log.New(file, "[Error] ", log.Ldate|log.Ltime)
	//runtime.GOMAXPROCS(2)
	FinishProcessCount = 0
	CurrentBatch = 0
	BatchCount = 1024
	TIBList = make([]*TraceIdBatch, 0, BatchCount)
	for i := int32(0); i < BatchCount; i++ {
		TIBList = append(TIBList, NewTIB())
	}
	TraceCheckSumMap = concurrent_map.New()
	//启动端口
	var wg sync.WaitGroup

	startPort := os.Args[1]
	fmt.Println("Starting the server ...")
	logger.Println("Starting the server ...")
	launchService := NewLaunchService()
	//监听客户端tcp连接
	wg.Add(1)
	go func() {
		defer wg.Done()
		launchService.Listen()
	}()

	//监听http连接
	wg.Add(1)
	go func() {
		defer wg.Done()
		launchService.LaunchService(startPort)
	}()
	//wg.Add(1)
	wg.Wait()
	//mapBatch
	sum := 0
	for {
		traceIdBatch := GetFinishedBatch()
		if traceIdBatch == nil {
			if isFinished() {
				//if sendCheckSum(){ //todo
				//	break
				//}
				fmt.Printf("处理结束")
				break
			}
			continue
		}
		launchService.getWrongTrace(traceIdBatch)
		//go trace
	}

	bytesData, _ := json.Marshal(TraceCheckSumMap)
	fmt.Printf(string(bytesData))
	req,err:=http.PostForm("http://127.0.0.1:9000/api/finished", url.Values{
		"result": []string{string(bytesData)},
	})
	if err!=nil{
		fmt.Printf(err.Error())
		return
	}
	defer req.Body.Close()
	body,err := ioutil.ReadAll(req.Body)
	if err != nil {
		logger.Println(err.Error())
	}
	fmt.Printf(string(body))
	wg.Add(1)
	fmt.Printf("总长度:%d", sum)
	wg.Wait()

}

func GetFinishedBatch() *TraceIdBatch {
	next := atomic.LoadInt32(&CurrentBatch) + 1

	if next >= BatchCount {
		next = 0
	}
	currentBatch := TIBList[atomic.LoadInt32(&CurrentBatch)]
	nextBatch := TIBList[next]

	if (nextBatch.getProcessCount() >= util.ProcessCount && currentBatch.getProcessCount() >= util.ProcessCount) ||
		(atomic.LoadInt32(&FinishProcessCount) >= util.ProcessCount && (!currentBatch.getFinish() && currentBatch.getProcessCount() >= util.ProcessCount)) {
		//fmt.Printf("准备处理的batch:%d\n", atomic.LoadInt32(&CurrentBatch))
		nTIB := NewTIB() // 清空操作可以优化？
		TIBList[atomic.LoadInt32(&CurrentBatch)] = nTIB
		atomic.SwapInt32(&CurrentBatch, next)
		//fmt.Printf("当前处理的CurrentBatch:%d", atomic.LoadInt32(&CurrentBatch))

		return currentBatch
	}
	//fmt.Printf("nil当前处理的CurrentBatch:%d   %+v\n", atomic.LoadInt32(&CurrentBatch),currentBatch)
	//fmt.Printf("nil当前处理的nextBatch:%d    %+v\n", atomic.LoadInt32(&CurrentBatch)+1,nextBatch)
	return nil
}

func isFinished() bool {
	if atomic.LoadInt32(&FinishProcessCount) >= util.ProcessCount {
		for i := int32(0); i < BatchCount; i++ {
			currentBatch := TIBList[i]
			if !currentBatch.getFinish() && currentBatch.getProcessCount() > 0 {
				//fmt.Printf("currentBatch:%+v\n", currentBatch)
				//fmt.Printf("nextBatch:%+v\n", TIBList[i+1])
				//fmt.Printf("未处理的batch:%d\n", i)
				return false
			}
		}
		return true
	}
	return false
}
