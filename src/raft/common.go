package raft

import "fmt"

//calculate min
func minInt(a int,b int)int{
	if a<b{
		return a
	}
	return b
}
func (rf *Raft)prtlog(){
	fmt.Print("i am ",rf.me," log is ")
	for _,log:= range rf.log{
		fmt.Print("id",rf.me," ",log," ")
	}
	fmt.Print("\n")
}