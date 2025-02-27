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
	"bytes"
	"labgob"
	"labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

const (
	Follower          = "Follower"
	Candidate         = "Candidate"
	Leader            = "Leader"
	heartbeatInterval = 100 // millisecond
	checkInterval     = 10  // millisecond
)

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

	// Persistent state on all servers
	CurrentTerm int         // latest term server has seen, increases monotonically
	VotedFor    int         // CandidateID that received vote in current term
	Log         []*LogEntry // log entries. each entry contains command for state matchine, and term when entry was received by leader

	// Volatile state on all servers
	CommitIndex int // index of highest log entry known to be committed
	LastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	NextIndex  []int // for each server, index of the next log entry to send to that server, init to leader last log index+1
	MatchIndex []int // for each server, index of highest log entry kown to be replicated on server

	State     string
	ExpiredAt time.Time
	VoteCount int
	applyChan chan ApplyMsg
}

// LogEntry stores a state machine command along with the term.
type LogEntry struct {
	Term    int         // The term number when the entry was received by the leader
	Command interface{} // The command received from client
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = rf.State == Leader
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

	RPrintf(rf, "saving Raft state: <term:%v, votedFor:%v, #log:%v>", rf.CurrentTerm, rf.VotedFor, len(rf.Log))

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	RPrintf(rf, "saved: <term:%v, votedFor:%v, #log:%v>", rf.CurrentTerm, rf.VotedFor, len(rf.Log))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	RPrintf(rf, "reading saved states...")

	if data == nil || len(data) < 1 { // bootstrap without any state?
		RPrintf(rf, "no states saved, skipped")
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var CurrentTerm int
	var VotedFor int
	var Log []*LogEntry
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil {
		RPrintf(rf, "error in decoding")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
	}
	RPrintf(rf, "loaded with: <term:%v, votedFor:%v, #log:%v>", rf.CurrentTerm, rf.VotedFor, len(rf.Log))
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
	// Your code here (2B).
	RPrintf(rf, "try append entries:<command:%v>", command)
	index, term, isLeader := rf.leaderAddEntry(command)
	return index, term, isLeader
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

	rf := NewRaft(peers, persister, me, applyCh)

	// Your initialization code here (2A, 2B, 2C).
	go stateLooper(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start applyer
	go Applyer(rf)

	return rf
}

// NewRaft creates a new raft.
func NewRaft(peers []*labrpc.ClientEnd, persister *Persister, me int, applyCh chan ApplyMsg) (rf *Raft) {
	rf = &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		VotedFor:    -1,       // -1 means voted for none
		State:       Follower, // started as a follower,
		CommitIndex: 0,
		LastApplied: 0,
		applyChan:   applyCh,
	}
	rf.resetElectionTimeout()
	return rf
}

// stateLooper checks the Raft's current state and puts it into the right loop
func stateLooper(rf *Raft) {
	// no need to sleep
	for {
		// lock read
		rf.mu.Lock()
		currentState := rf.State
		RPrintf(rf, "got state change, looping...")
		rf.mu.Unlock()

		switch currentState {

		case Follower:
			rf.followerState()
		case Leader:
			rf.leaderState()
		case Candidate:
			rf.candidateState()
		}
	}
}

// Applyer applied changes to state machines
func Applyer(rf *Raft) {
	RPrintf(rf, "Applyer started...")
	for {
		time.Sleep(time.Duration(checkInterval) * time.Millisecond)
		globalRule5_3(rf)
	}
}

// globalRule5_3 enforces Rule 5.3.
// if commitIndex > lastApplied, increment lastApplied, apply log[lastApplied]
// to state machine.
func globalRule5_3(rf *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.CommitIndex > rf.LastApplied {
		RPrintf(rf, "found CommitIndex, applying it, <CommitIndex:%v, LastApplied:%v>", rf.CommitIndex, rf.LastApplied)
		entry := rf.Log[rf.LastApplied]
		rf.LastApplied++
		rf.sendApplyMsg(rf.LastApplied, entry)
	}
}

func (rf *Raft) sendApplyMsg(index int, entry *LogEntry) {
	rf.applyChan <- ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: index,
	}
	RPrintf(rf, "applyChan -> sending ApplyMsg: <Command:%v, CommandIndex:%v>", entry.Command, index)
}
