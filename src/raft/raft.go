package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"



//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//On all servers:
	currentTerm int32
	votedFor int //candidate Id that received vote in current term. -1 means none
	log []CommandAndTerm
    //volatile state on all servers:
    commitIndex int
	lastApplied int
	leaderId int
	//volatile on leaders
	nextIndex []int  //initialize to len(log)
	matchIndex[]int //
	//self defined
	stateMu sync.Mutex
	currentState States
	expireTimer *time.Timer
	heartbeatTicker *time.Ticker
	notifyApplych chan struct{}  //notify to apply
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	term = int(atomic.LoadInt32(&rf.currentTerm))
	if rf.currentState == States_LEADER{
		isleader = true
	}
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	/*
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)*/
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	/*
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int //candidate Id that received vote in current term
	var log []CommandAndTerm
	if d.Decode(&currentTerm)!=nil||
		d.Decode(&votedFor)!=nil||
		d.Decode(&log)!=nil{
		fmt.Println("Error Decode")
		return
	}else{
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}*/
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term := int(rf.currentTerm)
	if rf.currentState != States_LEADER{
		return -1,-1,false
	}
	// Your code here (2B).
	rf.mu.Lock()
	newcmd := CommandAndTerm{command,int(rf.currentTerm)}
	rf.log = append(rf.log,newcmd)
	rf.mu.Unlock()
	go rf.BroadcastAppendEntries()
	index := rf.commitIndex
	fmt.Println(rf.me," Start is called add ",command)
	rf.prtlog()
	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}
//
// self-defined function
func (rf *Raft) getLastLogIndex() int{
	return len(rf.log)-1
}
func (rf *Raft) leaderInitialize(){
	//init structure
	fmt.Println(rf.me ,"leader initialize")
	rf.nextIndex = make([]int,len(rf.peers))
	for i:=0;i<len(rf.nextIndex);i++{
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int,len(rf.peers))
	for i:=0;i<len(rf.matchIndex);i++{
		rf.matchIndex[i] = 0
	}
	//init timer
	if rf.heartbeatTicker!=nil{
		rf.heartbeatTicker.Stop()
	}
	rf.heartbeatTicker = time.NewTicker(time.Duration(HEARTBEAT_TIME)* time.Millisecond)
}
func (rf *Raft) resetTimer(duration time.Duration){
	rf.expireTimer.Stop()
	rf.expireTimer.Reset(duration)
}
//send vote
func (rf *Raft) sendVote(server int,args RequestVoteArgs,replyCh chan<- RequestVoteReply){
	reply:= RequestVoteReply{}
	if !rf.peers[server].Call("Raft.RequestVote",&args,&reply){
		reply.Server,reply.Errcode = server,SENDVOTEERROR
	}
	replyCh <- reply
}
func (rf *Raft) startElection(){
	rf.mu.Lock()
	if rf.currentState != States_FOLLOWER{
		fmt.Println(rf.me," time expire but not in follower State")
		rf.mu.Unlock()
		return
	}
	fmt.Println("server id ",rf.me," expire timer")
	rf.currentState = States_CANDIDATE
	atomic.AddInt32(&rf.currentTerm,1)
	fmt.Println("server id ",rf.me," is now candidate. currentTerm ",atomic.LoadInt32(&rf.currentTerm))
	rf.votedFor = rf.me
	newExpireTime := time.Millisecond*time.Duration(150+rand.Intn(EXPIRE_TIME-150))
	rf.resetTimer(newExpireTime)
	quitTimer:= time.NewTimer(newExpireTime) // timer to quit this go routhine
	rvargs := RequestVoteArgs{int(atomic.LoadInt32(&rf.currentTerm)),rf.me,rf.getLastLogIndex(),rf.log[rf.getLastLogIndex()].CommandTerm}
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply,len(rf.peers)-1)
	//rf.expireTimer.Reset(time.Millisecond*time.Duration(150+rand.Intn(EXPIRE_TIME-150)))
	//send RequestVoteRPC
	for i:=0;i<len(rf.peers);i++{
		if i != rf.me{
			fmt.Println("sent args term ",rvargs.Term," me is ",rvargs.CandidateId)
			go rf.sendVote(i,rvargs,replyCh)
		}
	}
	fmt.Println("wait for vote finish")
	voteCount, threshold := 1,len(rf.peers)/2
	for voteCount<=threshold{
		select{
		case reply:= <-replyCh:
			if reply.Errcode == SENDVOTEERROR{
				go rf.sendVote(reply.Server,rvargs,replyCh)
			}else if reply.Term > int(atomic.LoadInt32(&rf.currentTerm)){
				//become follower
				fmt.Println(rf.me," become follower again ")
				rf.BecomeFollower(reply.Term)
			}else if reply.VoteGranted{
				voteCount += 1
			}
		case <-quitTimer.C:
			fmt.Println(rf.me, " need to quit right now current term is ",rf.currentTerm)
			rf.currentState = States_FOLLOWER
			rf.resetTimer(time.Millisecond*time.Duration(EXPIRE_TIME+100*rand.Intn(5)*100))
			return
		}
	}
	//become leader
	rf.mu.Lock()
	if rf.currentState == States_CANDIDATE{
		rf.currentState = States_LEADER
		fmt.Printf("candidate: %d recv enough votes and becomes leader",rf.me)
		rf.leaderInitialize()
	}
	rf.mu.Unlock()
	rf.BroadcastAppendEntries()
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	fmt.Println(me," initialize term is ",0)
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1  //every heart beat reset
	rf.log = make([]CommandAndTerm,1)
	rf.log[0] = CommandAndTerm{0,0}
	rf.commitIndex = 0
	rf.lastApplied = 0
	//initialize self defined
	rf.currentState = States_FOLLOWER
	rf.expireTimer = time.NewTimer(time.Millisecond*time.Duration(rf.me*250+rand.Intn(100)))
	rf.heartbeatTicker = time.NewTicker(time.Millisecond*time.Duration(HEARTBEAT_TIME))
	rf.applyCh = applyCh
	rf.notifyApplych = make(chan struct{},100)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//go func
	go rf.applyEvent()
	go func(){
		for{
			select{
			case <-rf.expireTimer.C:
				//electionc
				rf.startElection()
			case <-rf.heartbeatTicker.C:
				if rf.currentState == States_LEADER{
					go rf.BroadcastAppendEntries()
				}
			}
		}}()
		/*
    	   rf.currentState == States_LEADER{
				rf.leaderInitialize()
				for range rf.heartbeatTicker.C{
					if rf.currentState != States_LEADER{
						break
					}
					rf.BroadcastAppendEntries()
				}
			}*/
	return rf
}
func (rf *Raft) applyEvent(){
	for{
		select{
		case <-rf.notifyApplych:
			fmt.Println(rf.me, " start to apply get lock")
			rf.mu.Lock()
			var entries []CommandAndTerm
			var commandValid bool
			fmt.Println(rf.me, " start to apply")
			initIndex:=rf.lastApplied+1
			if rf.lastApplied<rf.commitIndex{
				commandValid=true
				entries = rf.log[rf.lastApplied+1:rf.commitIndex+1]
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()
			for offset,entry:=range entries{
				posi:=initIndex+offset
				rf.applyCh <- ApplyMsg{commandValid,entry.Command,posi}
			}
		}
	}
}
func (rf *Raft) BecomeFollower(term int){
	rf.stateMu.Lock()
	rf.currentTerm = int32(term)
	rf.currentState = States_FOLLOWER
	rf.resetTimer(time.Millisecond*time.Duration(EXPIRE_TIME))
	rf.stateMu.Unlock()
}
func sendRequestVoteWorker(rf *Raft,server int,args *RequestVoteArgs, reply *RequestVoteReply,wg *sync.WaitGroup,voteCnt *int32){

	wg.Done()
}
func (rf *Raft) BroadcastAppendEntries(){
	if rf.currentState != States_LEADER{
		return
	}
	fmt.Println(rf.me," broad cast append entri")
	replyCh := make(chan AppendEntriesReply,len(rf.peers)-1)
	lastIndex := rf.getLastLogIndex()
	for i:=0;i<len(rf.peers);i++{
		tmpi:=i
		if tmpi!=rf.me{
			rf.mu.Lock()
			args := AppendEntriesArgs{}
			args.Term = int(atomic.LoadInt32(&rf.currentTerm))
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[tmpi]-1
			args.PrevLogTerm = rf.log[rf.nextIndex[tmpi]-1].CommandTerm
			args.LeaderCommit = rf.commitIndex
			fmt.Println(rf.me," lastlogIndex is ",rf.getLastLogIndex()," nextIndex ",rf.nextIndex[tmpi])
			if rf.getLastLogIndex()>=rf.nextIndex[tmpi]{
				//send with log entri
				args.Entries = rf.log[rf.nextIndex[tmpi]:]
			}else{
				//heart beat
				fmt.Println(rf.me," only send heart beat")
				args.Entries =[]CommandAndTerm{}
			}
			rf.mu.Unlock()
			go rf.AppendEntryToServer(tmpi,args,replyCh)
		}
	}
	cnt:=0
	agreeCnt:=0
	for cnt<len(rf.peers)-1{
		select{
		case reply:= <- replyCh:
			if reply.Errcode == ERROR_APPEND_LOGINCONSISTENCY{
				fmt.Println(rf.me," log dont match retry")
				rf.mu.Lock()
				rf.nextIndex[reply.Server] -= 1
				args := AppendEntriesArgs{}
				args.Term = int(atomic.LoadInt32(&rf.currentTerm))
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[reply.Server]-1
				args.PrevLogTerm = rf.log[rf.nextIndex[reply.Server]-1].CommandTerm
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				go rf.AppendEntryToServer(reply.Server,args,replyCh)
			}
			if reply.Errcode == APPENDERROR{
				//cant connnect cnt++
				cnt+=1
				fmt.Println(rf.me," appendentrie ",reply.Server," error")
			}else if reply.Term>int(atomic.LoadInt32(&rf.currentTerm)){
				rf.BecomeFollower(reply.Term)
				rf.heartbeatTicker.Stop()
				fmt.Println("Leader ",rf.me," become follower ")
				return
			}else if reply.Success == true{
				//success
				rf.mu.Lock()
				agreeCnt+=1
				cnt+=1
				rf.nextIndex[reply.Server] = lastIndex+1
				rf.matchIndex[reply.Server] = lastIndex
				rf.mu.Unlock()
				fmt.Println(rf.me," server is ",reply.Server," nextIndex update to ",rf.nextIndex[reply.Server])
			}
		}
	}
	if agreeCnt+1>cnt/2 && lastIndex>rf.commitIndex{
		//all commiit
		fmt.Println("all need commit")
		rf.commitIndex = lastIndex
		rf.notifyApplych <- struct{}{}
	}

	fmt.Println("recv ",cnt," in total")
}
func (rf *Raft)AppendEntryToServer(serverId int,args AppendEntriesArgs,replyCh chan<- AppendEntriesReply){
	reply:= AppendEntriesReply{}
	ok:=rf.peers[serverId].Call("Raft.AppendEntries",&args,&reply)
	if !ok{
		reply.Errcode = APPENDERROR
		reply.Server = serverId
	}
	replyCh<-reply
}