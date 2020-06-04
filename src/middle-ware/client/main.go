package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"middle-ware/util"
	"net/http"
	"os"
	"sync"
	"time"
)

var BatchTraceList []map[string]*[]string
var BatchCount int
var ErrBufferFull = errors.New("bufio: buffer full")
var logger *log.Logger
func main() {
	file,_ := os.Create("/error.log")
	logger = log.New(file, "[Error] ", log.Ldate|log.Ltime)
	fmt.Println("Starting the client ...")
	logger.Println("Starting the client ...")
	//runtime.GOMAXPROCS(2)
	BatchCount = 1024
	for i := 0; i < BatchCount; i++ {
		BatchTraceList = append(BatchTraceList, make(map[string]*[]string, 8192))
	}

	var wg sync.WaitGroup

	args := os.Args
	var path string
	if args[1] == util.ClientProcessPort1 {
		path = "/trace1.data"
	} else if args[1] == util.ClientProcessPort2 {
		path = "/trace2.data"
	}
	//启动ready和setParameter服务
	launchClient := NewLaunchClient()
	wg.Add(1)
	go func() {
		defer wg.Done()
		launchClient.LaunchClient(args[1])
	}()
	wg.Wait()

	var wg2 sync.WaitGroup
	wg2.Add(1)
	//开始处理数据
	start := time.Now()
	dataPath := "http://127.0.0.1:" +  launchClient.TraceDataPort + path
	//dataPath := "http://127.0.0.1/testData"  + path
	resp, err := http.Get(dataPath)
	if err != nil {
		panic(err)
	}

	//done := make(chan bool)
	logger.Println("start handle")
	buf := bufio.NewReaderSize(resp.Body, 1024*1024*256) //todo 调整数值可能有优化
	count := 0
	pos := 0
	//set<string>
	BadTraceIdList := make([]byte, 0, 512)
	traceMap := BatchTraceList[pos]
	badcount := 0
	for {
		//line, status, err := buf.ReadBytes( '\n')
		line, err := buf.ReadBytes('\n')
		if len(line) > 1 {
			count++
			place := 0
			for index, sign := range line {
				if sign == '|' {
					place = index
					break
				}
			}
			traceId := line[0:place]

			if tmp, ok := traceMap[util.Bytes2str(traceId)]; ok {
				*tmp = append(*tmp, util.Bytes2str(line))
			} else {
				spanList := new([]string)
				*spanList = make([]string, 0, 32)
				traceMap[util.Bytes2str(traceId)] = spanList
				*spanList = append(*spanList, util.Bytes2str(line))
			}

			if isContain(line) {
				badcount++
				BadTraceIdList = append(BadTraceIdList, traceId...)
				BadTraceIdList = append(BadTraceIdList, '|')
			}
		}
		if count%util.BatchSize == 0 {
			pos++
			if pos >= BatchCount {
				pos = 0
			}
			traceMap = BatchTraceList[pos]
			//fmt.Printf("%d    %d\n  ",count,len(BadTraceIdList))

			go func(traceList []byte, batchPos int) {
				launchClient.UpdateWrongTraceIdThread(traceList, batchPos/util.BatchSize-1)
			}(BadTraceIdList, count)

			//等待下一个pos清空
			for len(traceMap) > 0 {

			}
			BadTraceIdList = make([]byte, 0, 512)
		}
		if err != nil {
			if err == io.EOF { //读取结束，会报EOF
				fmt.Printf("count:%d\n", count)
				fmt.Printf("badcount:%d\n", badcount)
				launchClient.UpdateWrongTraceIdThread(BadTraceIdList, count/util.BatchSize)
				launchClient.SetWTIConn.Write([]byte("!")) //结束标志
				break
			}
			break
		}
	}

	end := time.Now()
	fmt.Println(end.Sub(start))
	wg2.Wait()
}

func isContain(line []byte) bool {
	for i := len(line) - 1; i > 0; i-- {
		if line[i] == '1' && line[i-1] == '=' && line[i-2] == 'r' && line[i-3] == 'o' &&
			line[i-4] == 'r' && line[i-5] == 'r' && line[i-6] == 'e' {
			return true
		}
		if line[i] == 'e' && line[i-1] == 'd' && line[i-2] == 'o' && line[i-3] == 'c' &&
			line[i-4] == '_' && line[i-5] == 's' && line[i-6] == 'u' && line[i-7] == 't' &&
			line[i-8] == 'a' && line[i-9] == 't' && line[i-10] == 's' && line[i-11] == '.' &&
			line[i-12] == 'p' && line[i-13] == 't' && line[i-14] == 't' && line[i-15] == 'h' {
			if line[i+1] == '=' && (line[i+2] != '2' || line[i+3] != '0' || line[i+4] != '0') {
				return true
			}
		}
		if line[i] == '|' {
			return false
		}
	}
	return false
}
