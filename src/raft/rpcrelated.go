package raft

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type CommandAndTerm struct{
	Command interface{}
	CommandTerm int
}
type States int32
const(
	States_FOLLOWER States = 1
	States_CANDIDATE States = 2
	States_LEADER States = 3
)
// TIME CONST
const(
	EXPIRE_TIME  int = 700
	HEARTBEAT_TIME int = 150
	SENDVOTEERROR int = 1
	APPENDERROR int = 2
	ERROR_APPEND_LOGINCONSISTENCY int = 3
)
//AppendEntries RPC
type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int //i think it is nextIndex[]
	PrevLogTerm int
	Entries []CommandAndTerm
	LeaderCommit int
}
//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int //mycurrent Term
	Success bool
	Server int
	Errcode int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int //candidate's term
	CandidateId int //
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //mycurrent Term
	VoteGranted bool//true means recv vote
	Server int
	Errcode int
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// example AppendEntries RPC handler.
////
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	fmt.Println("append entries ",rf.me," args.Term is ",args.Term," myterm",int(atomic.LoadInt32(&rf.currentTerm)))
	rf.resetTimer(time.Millisecond*time.Duration(EXPIRE_TIME+rand.Intn(50)))
	fmt.Println("id ",rf.me," timer reset")
	curTerm := int(atomic.LoadInt32(&rf.currentTerm))
	reply.Term = curTerm
	reply.Success = false
	reply.Server = rf.me
	if args.Term<curTerm{
		return
	}
	if args.Term>curTerm{
		atomic.StoreInt32(&rf.currentTerm,int32(args.Term))
		rf.votedFor = -1
		fmt.Println("vote for clear")
	}
	///rf.log[args.PrevLogIndex].CommandTerm != args.PrevLogTerm
	fmt.Println(rf.me," rf.getLastLogIndex ",rf.getLastLogIndex(),"prevLogindex ",args.PrevLogIndex)
	if rf.getLastLogIndex()<args.PrevLogIndex{
		fmt.Println(rf.me," dont match")
		reply.Success = false
		reply.Errcode = ERROR_APPEND_LOGINCONSISTENCY
	}else if rf.log[args.PrevLogIndex].CommandTerm != args.PrevLogTerm{
		reply.Success = false
		reply.Errcode = ERROR_APPEND_LOGINCONSISTENCY
		fmt.Println(rf.me," delete some log")
		rf.log = rf.log[:args.PrevLogIndex]
	}else{
		if len(args.Entries)>0{
			rf.log = rf.log[:args.PrevLogIndex+1]
			for _,entry:= range args.Entries{
				rf.log = append(rf.log,entry)
			}
			fmt.Println(rf.me," log updated")
		}else{
			fmt.Println(rf.me," only recv heartbeat")
		}

		reply.Success = true
		rf.prtlog()
	}
	rf.mu.Unlock()
	if args.LeaderCommit>rf.commitIndex && rf.getLastLogIndex()>=args.LeaderCommit{
		fmt.Println(rf.me," commitIndex updated to ",args.LeaderCommit)
		rf.commitIndex = minInt(args.LeaderCommit,rf.getLastLogIndex())
		rf.notifyApplych <- struct{}{}
	}
}


//
// example RequestVote RPC handler.
////
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	curTerm := int(atomic.LoadInt32(&rf.currentTerm))
	reply.Term = curTerm
	reply.VoteGranted = false
	fmt.Println(args.CandidateId," asks me",rf.me," to vote")
	fmt.Println("args term is ",args.Term," my term is ",curTerm)
	if args.Term<curTerm || curTerm == args.Term && rf.votedFor!=-1{
		return
	}
	if args.Term>curTerm{
		atomic.StoreInt32(&rf.currentTerm,int32(args.Term))
		rf.votedFor = -1
		fmt.Println("vote for clear")
		if rf.currentState != States_FOLLOWER{
			rf.BecomeFollower(args.Term)
		}
	}
	fmt.Println("rf.votedFor ",rf.votedFor," args.LastLogindex",args.LastLogIndex," rf.getLastLogIndex",rf.getLastLogIndex())
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		if args.LastLogIndex >= rf.getLastLogIndex(){
			fmt.Println("i voted")
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.resetTimer(time.Millisecond*time.Duration(EXPIRE_TIME))
			return
		}
	}
}