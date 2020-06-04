package test

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"middle-ware/util"
	"sort"
	"testing"
)

func TestRead(T *testing.T) {
	//buf := []byte("39ca43cd27c3ecce|7ce3917796482fa1|6b99f637f3aca9aa|112f137ea728ee37|c8f349f54a8332c|15e54c4f8fba4b76|15e54c4f8fba4b76|62e31de0271c6ada,756\n")
	//len := len(buf)
	//dataBytes := make([]byte, 0, 16)
	//traceIdList := make([]string, 0, len>>4)
	//var batchPos string
	//for i := 0; i < len; i++ {
	//	if buf[i] == '|' {
	//		traceIdList = append(traceIdList, util.Bytes2str(dataBytes))
	//		dataBytes = make([]byte, 0, 16)
	//	} else if buf[i] == ',' {
	//		traceIdList = append(traceIdList, util.Bytes2str(dataBytes))
	//		batchPos = util.Bytes2str(buf[i+1 : len-1])
	//		break
	//	} else {
	//		dataBytes = append(dataBytes, buf[i])
	//	}
	//}
	//fmt.Println(batchPos)
	//fmt.Println(traceIdList)

	//
	//data := []int{1, 2, 3, 34, 5, 5, 6, 67, 77, 87}
	//fmt.Println(data)
	//s1 := data[:0:0]
	//s1 = append(s1, 123)
	//s2 := data[:0]
	//s2 = append(s2, 10)
	//fmt.Printf("%p\n", &data[0])
	//fmt.Printf("%p\n", &s2[0])
	//fmt.Printf("%p\n", &s1[0])
	//m := make(map[int]*[]byte, 10)
	//m[1] = new([]byte)
	//
	//if value, ok := m[1]; ok {
	//	fmt.Printf("%+v\n", &value)
	//	fmt.Printf("%T\n", value)
	//	fmt.Printf("%+v\n", value)
	//	*value = []byte("ba")
	//}
	//if value, ok := m[1]; ok {
	//	fmt.Printf("%+v\n", &value)
	//	fmt.Printf("%T\n", value)
	//	fmt.Printf("%+v\n", value)
	//	*value = []byte("b")
	//}
	//s := []int{1, 1, 1,1,1,1}
	//for i := range s[2:4] {
	//	fmt.Println(i)
	//}
	//wrongTrace := make([]byte, 0, 1>>18) //256kB
	//traceId := "wuyuhao"
	//wrongTrace = append(wrongTrace,util.Str2bytes("\""+traceId+"\":")... )
	//fmt.Println(string(wrongTrace))
	//256kB
	//traceMap := make(map[string]*[]string, 32)
	//spanList := new([]string)
	//*spanList = make([]string, 0, 32)
	//traceMap["1"] = spanList //256kB
	//*spanList = append(*spanList, "12312")
	//fmt.Printf("%+v\n", traceMap)
	//
	//wrongTraceMap := make(map[string]*[]string, 32)
	//spanListPtr := new([]string)
	//*spanListPtr = make([]string, 0, 32)
	//wrongTraceMap["1"] = spanListPtr
	//if v, ok := traceMap["1"]; ok {
	//	*spanListPtr = append(*spanListPtr, *v...)
	//}
	//fmt.Printf("%+v\n", wrongTraceMap)
	//marshal, _ := json.Marshal(wrongTraceMap)
	//fmt.Printf(string(marshal))
	////update(s)
	//buf := util.Str2bytes("46ecf2ac75a7a49|2df2426f286eac99|2df2426f286eac99|6c89810303360d0c|6cac34fdeab718b7|6cac34fdeab718b7|38aebbe9628e1d57|38aebbe9628e1d57|3a87c5f3e626095c|333507ca5e2ba266|30af7b4f76a7a92d|30af7b4f76a7a92d|2f5196e2018c227f|2f5196e2018c227f|15879397e89d99b9|15879397e89d99b9|6c89810303360d0c|3a87c5f3e626095c|333507ca5e2ba266|,57")
	//length := len(buf)
	////wrongTraceMap := make(map[string]*[]string, 32) //256kB
	//dataBytes := make([]byte, 0, 16)
	//traceIdList := make([]string, 0, 32)
	//var batchPos int
	//for i := 0; i < length; i++ {
	//	if buf[i] == '|' {
	//		traceIdList = append(traceIdList, util.Bytes2str(dataBytes))
	//		dataBytes = make([]byte, 0, 16)
	//	} else if buf[i] == ',' {
	//		batchPos, _ = strconv.Atoi(util.Bytes2str(buf[i+1 : length]))
	//		break
	//	} else {
	//		dataBytes = append(dataBytes, buf[i])
	//	}
	//}
	//fmt.Println(batchPos)
	//fmt.Println(traceIdList)
	//fmt.Println(int(15140000)/util.BatchSize)
	//m := make(map[string][]string, 64)
	//m1 := make(map[string][]string, 64)
	//m2 := make(map[string][]string, 64)
	//m1["a"] = []string{"1","2"}
	//m1["b"] = []string{"1","3"}
	//m2["b"] = []string{"1","4"}
	//m2["c"] = []string{"1","5"}
	//for k, v := range m1 {
	//	m[k] = v
	//}
	//for s, strings := range m2 {
	//	if _, ok := m[s]; ok {
	//		m[s] = append(m[s], strings...)
	//	} else {
	//		m[s] = strings
	//	}
	//}
	//fmt.Printf("%+v",len(m))
	//pack := make([]byte,0, int(10))
	//fmt.Println(len(pack),cap(pack))
	//for len(pack) < cap(pack) {
	//	fmt.Println(len(pack),cap(pack))
	//	pack = append(pack, 1)
	//}
	//fmt.Println(len(pack),cap(pack))
	//fmt.Println(pack)
	//fmt.Println(s)
	//
	//newS := myAppend(s)
	//
	//fmt.Println(s)
	//fmt.Println(newS)
	//
	//s = newS
	//
	//myAppendPtr(&s)
	//fmt.Println(s)
	map1 := make(map[string][]string, 32)
	map2 := make(map[string][]string, 32)
	map1["a"] = []string{"123456|1590216547740489|", "123451|1590216547740475|", "123451|1590216547740473|","123451|1590216547740487|","123451|1590216547740485|"}
	map1["b"] = []string{"123456|100000000000000000009|", "1234|100000000000000000007|", "123451|100000000000000000005|"}
	map2["b"] = []string{"123456|100000000000000000004|", "1234|100000000000000000002|"}
	map2["c"] = []string{"123456|100000000000000000006|", "1234|100000000000000000003|"}
	m := make(map[string][]sortItem, 64)
	for traceId, spanList := range map1 {
		for _, span := range spanList {
			var createTime string
			spanByte := util.Str2bytes(span)
			length := len(spanByte)
			for i := 0; i < length; i++ {
				if spanByte[i] == '|' {
					j := i + 1
					for ; j < length; j++ {
						if spanByte[j] == '|' {
							createTime = util.Bytes2str(spanByte[i+1 : j])
							break
						}
					}
					break
				}
			}
			m[traceId] = append(m[traceId], sortItem{
				createTime: createTime,
				data:       span,
			})
		}
	}

	for traceId, spanList := range map2 {
		for _, span := range spanList {
			var createTime string
			spanByte := util.Str2bytes(span)
			for i := 0; i < len(spanByte); i++ {
				if spanByte[i] == '|' {
					for j := i + 1; j < len(spanByte); j++ {
						if spanByte[j] == '|' {
							createTime = util.Bytes2str(spanByte[i+1 : j])
						}
					}
				}
			}
			m[traceId] = append(m[traceId], sortItem{
				createTime: createTime,
				data:       span,
			})
		}
	}

	for _, v := range m {
		data := make([]byte, 0, 8192)
		sort.Slice(v, func(i, j int) bool {
			return v[i].createTime < v[j].createTime
		})
		for _, span := range v {
			data = append(data, util.Str2bytes(span.data)...)
		}
		data = append(data, '\n')
		sum := md5.Sum(data)
		hex.EncodeToString(sum[:])
	}

	fmt.Printf("%+v", m)
}

type sortItem struct {
	createTime string
	data       string
}

func update(s []int) {
	s[1] = 2
}
func myAppend(s []int) []int {
	// 这里 s 虽然改变了，但并不会影响外层函数的 s
	fmt.Printf("%+v\n", &s)
	s = append(s, 100)
	fmt.Printf("%+v\n", &s)
	return s
}

func myAppendPtr(s *[]int) {
	// 会改变外层 s 本身
	*s = append(*s, 100)
	return
}
