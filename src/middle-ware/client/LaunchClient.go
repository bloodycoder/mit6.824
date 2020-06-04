package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"middle-ware/util"
	"net"
	"net/http"
	"strconv"
)

type launchClient struct {
	TraceDataPort string
	SetWTIConn    *util.TcpClient
	GetWTConn     *util.TcpClient
}

func NewLaunchClient() *launchClient {
	return &launchClient{}
}
func (lc *launchClient) LaunchClient(startPort string) {
	m := http.NewServeMux()
	s := http.Server{Addr: ":" + startPort, Handler: m}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		//打开连接:
		conn1, _ := net.Dial("tcp", "localhost:8003")
		lc.SetWTIConn = util.NewTcpClient(conn1)
		conn2, _ := net.Dial("tcp", "localhost:8004")
		lc.GetWTConn = util.NewTcpClientSize(conn2, 131072)
		go lc.GetWrongTrace()
		log.Printf("ready success\n")
		w.Write([]byte("suc"))
		// Cancel the context on request
	})

	m.HandleFunc("/setParameter", func(w http.ResponseWriter, r *http.Request) {
		lc.TraceDataPort = r.FormValue("port")
		w.Write([]byte("suc"))
		// Cancel the context on request
		log.Printf("setParameter success\n")
		cancel()
	})

	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	select {
	case <-ctx.Done():
		// Shutdown the server when the context is canceled
		s.Shutdown(ctx)
	}
	log.Printf("Ready Finished ")
}

func (lc *launchClient) GetWrongTrace() {
	// 123456|345678|123451|,batchPos
	for {

		buf, _ := lc.GetWTConn.Read()

		length := len(buf)

		if length > 0 {
			//log.Printf("GetWrongTrace:%s\n", string(buf))
			//go func() {
			wrongTraceMap := make(map[string]*[]string, 64) //256kB
			dataBytes := make([]byte, 0, 16)
			traceIdList := make([]string, 0, 64)
			var batchPos int
			for i := 0; i < length; i++ {
				if buf[i] == '|' {
					traceIdList = append(traceIdList, util.Bytes2str(dataBytes))
					dataBytes = make([]byte, 0, 16)
				} else if buf[i] == ',' {
					batchPos, _ = strconv.Atoi(util.Bytes2str(buf[i+1 : length]))
					break
				} else {
					dataBytes = append(dataBytes, buf[i])
				}
			}
			//log.Printf("BatPos:%d,GetWrongTrace:%s\n", batchPos, string(buf))
			pos := batchPos % BatchCount
			previous := pos - 1
			if previous == -1 {
				previous = BatchCount - 1
			}
			next := pos + 1
			if next == BatchCount {
				next = 0
			}
			//spanList := new([]string)
			//			if tmp, ok := traceMap[util.Bytes2str(traceId)]; ok {
			//				spanList = tmp
			//			} else {
			//				*spanList = make([]string, 0, 32)
			//				traceMap[util.Bytes2str(traceId)] = spanList
			//			}
			//			*spanList = append(*spanList, util.Bytes2str(line))

			for _, traceId := range traceIdList {
				//log.Printf("traceId:%s", string(traceId))
				if _, ok := wrongTraceMap[traceId]; ok {
					continue
				}
				spanListPtr := new([]string)
				*spanListPtr = make([]string, 0, 64)
				wrongTraceMap[traceId] = spanListPtr
				traceMap := BatchTraceList[previous]
				if spanList, ok := traceMap[traceId]; ok {
					*spanListPtr = append(*spanListPtr, *spanList...)
				}

				traceMap = BatchTraceList[pos]
				if spanList, ok := traceMap[traceId]; ok {
					*spanListPtr = append(*spanListPtr, *spanList...)
				}
				traceMap = BatchTraceList[next]
				if spanList, ok := traceMap[traceId]; ok {
					*spanListPtr = append(*spanListPtr, *spanList...)
				}
				if len(*spanListPtr) <= 0 {
					delete(wrongTraceMap, traceId)
				}
			}
			//log.Printf("%+v\n", wrongTraceMap)
			//fmt.Printf("batchPos:%d  发送map数量:%d\n", batchPos, len(wrongTraceMap))
			msg, _ := json.Marshal(wrongTraceMap)
			//log.Printf("%+v\n", string(msg))
			lc.GetWTConn.Write(msg)
			BatchTraceList[previous] = make(map[string]*[]string, 8192)
			//}()
		}
	}
}

func (lc *launchClient) UpdateWrongTraceIdThread(badTraceIdList []byte, batchPos int) {
	// 123456|345678|123451|,batchPos\n
	//lc.Maxlength = int(math.Max(float64(lc.Maxlength), float64(len(badTraceIdList))))

	if len(badTraceIdList) > 0 {
		badTraceIdList = append(badTraceIdList, util.Str2bytes(","+strconv.Itoa(batchPos))...)
		fmt.Printf("batchPos:%d 发送的长度:%d  发送的badTraceId:%s\n", batchPos,len(badTraceIdList) ,string(badTraceIdList))
		//log.Printf("setTraceId:" + string(badTraceIdList) + "\n")
		lc.SetWTIConn.Write(badTraceIdList)
	}

}
