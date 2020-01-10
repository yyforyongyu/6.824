package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int  // CurrentTerm, for candidate to update itself
	VoteGrandted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	VPrintf(rf, args.CandidateID, "got request with args:%s", jsonVoteArgs(args))

	// currentTerm > args.Term
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		VPrintf(rf, args.CandidateID, "got request, arg term is smaller: <term:%v, current:%v>, abort", args.Term, rf.CurrentTerm)
		return
	}

	//  args.Term > currentTerm
	if rf.CurrentTerm < args.Term {
		VPrintf(rf, args.CandidateID, "got request, update term")
		rf.becomeFollower(args.Term)
	}

	// currentTerm <= args.Term
	if !passRequestVoteRule2(args, rf) {
		VPrintf(rf, args.CandidateID, "got request, fail rule 2, abort")
		// implicitly set reply.VoteGrandted to false
		return
	}

	reply.VoteGrandted = true
	rf.VotedFor = args.CandidateID
	rf.resetElectionTimeout()
	VPrintf(rf, args.CandidateID, "got request, vote granted")
}

func checkVoteIDMatch(voteFor, candidateID int) bool {
	if !(voteFor == -1 || voteFor == candidateID) {
		return false
	}
	return true
}

// if votedFor is null or candidateID, and candidate's log is at least as
// up-to-date as receiver's log, grant vote
func passRequestVoteRule2(args *RequestVoteArgs, rf *Raft) bool {

	if !checkVoteIDMatch(rf.VotedFor, args.CandidateID) {
		VPrintf(rf, args.CandidateID, "vote ID not match, <has:%v, want:%v>", rf.VotedFor, args.CandidateID)
		return false
	}

	// check log is more update-to-date
	if checkMoreUpToDate(args, rf) {
		VPrintf(rf, args.CandidateID, "rf log is longer, no votes given")
		return false
	}

	rf.State = Follower
	return true
}

// checkUpToDate checks whether log is update-to-date.
// Raft determins which of two logs is more up-to-date by comparing the index
// and term of the last entries in the logs.
// If the logs have last entries with different terms, then the log with the
// latter term is more up-to-date.
func checkMoreUpToDate(args *RequestVoteArgs, rf *Raft) bool {
	VPrintf(rf, args.CandidateID, "checking which log is more up-to-date, rf log:%s, args:%s", jsonLog(rf.Log), jsonVoteArgs(args))
	if len(rf.Log) == 0 {
		return false
	}

	entryIndex := len(rf.Log)
	entryTerm := rf.Log[entryIndex-1].Term

	// if log term in raft is greater, it's more up-to-date.
	if entryTerm > args.LastLogTerm {
		VPrintf(rf, args.CandidateID, "rf log term is greater")
		return true
	}

	// if log term in args is the same, and the last index is no smaller, i.e.,
	// its log is as least as long, the it's more up-to-date.
	if entryTerm == args.LastLogTerm {
		if entryIndex > args.LastLogIndex {
			VPrintf(rf, args.CandidateID, "rf log index is greater")
			return true
		}
	}

	return false
}
