package raft

type AppendEntriesArgs struct {
	Term         int         // leader's term
	LeaderID     int         // for follower to redirect client requests
	PrevLogIndex int         // index of log entry, immediately preceding new ones
	PrevLogTerm  int         // term of PrevLogIndex
	Entries      []*LogEntry // log entries to store, empty for hearbeat
	LeaderCommit int         // leader's CommitIndex
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex&PrevLogTerm

	ConflictIndex int // used for fast backtracking
	ConflictTerm  int // used for fast backtracking
}

// AppendEntries implements the RPC specified in Raft's paper.
// It takes an args, and modifies rf or replay.
// When a reply is set to be false,
//  - during a hearbeat response, this means the current peer doesn't recognize
// 	  the sender as the leader. The leader should step down by 	globalRule5_1.
//  - during normal ops, this means the current follower doesn't have the required
//    logs, which requires the leader sending it over.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	APrintf(rf, args.LeaderID, "got request with args:%s", jsonEntryArgs(args))

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		APrintf(rf, args.LeaderID, "got request, term is smaller: <term:%v, current:%v>, abort", args.Term, rf.CurrentTerm)
		return
	}

	APrintf(rf, args.LeaderID, "got request, agree the leadership and reset timeout")
	rf.becomeFollower(args.Term)

	rf.checkConflictTermAndIndex(args, reply)
	if !reply.Success {
		APrintf(rf, args.LeaderID, "got request, previous entry not match at <prevIndex:%v, prevTerm:%v>", args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	appendNewEntries(args, rf)

	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = Min(args.LeaderCommit, len(rf.Log))
	}

	APrintf(rf, args.LeaderID, "got request, reply success:%s", jsonEntryReply(reply))
}

// 2. Reply false if log doesn't contain an entry at preLogIndex whose term
// matches preLogTerm.
func (rf *Raft) checkConflictTermAndIndex(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.PrevLogIndex == 0 {
		reply.Success = true
		APrintf(rf, args.LeaderID, "no previous log required, skip checking, reply:%s", jsonEntryReply(reply))
		return
	}

	// Found shorter entry
	if len(rf.Log) < args.PrevLogIndex {
		reply.ConflictIndex = Max(len(rf.Log), 1) // if log is empty, should return 1
		APrintf(rf, args.LeaderID, "log mismatch, no previous entry at <%v>, reply:%s", args.PrevLogIndex, jsonEntryReply(reply))
		return
	}

	prevEntry := rf.Log[args.PrevLogIndex-1]
	// no conflicts
	if prevEntry.Term == args.PrevLogTerm {
		reply.Success = true
		APrintf(rf, args.LeaderID, "no conflicts found, return reply:%s", jsonEntryReply(reply))
		return
	}

	// Found a previous entry, but term mismatched
	reply.ConflictTerm = prevEntry.Term
	// find the index
	for i, entry := range rf.Log {
		if entry.Term == reply.ConflictTerm {
			reply.ConflictIndex = i + 1
			break
		}
	}

	APrintf(rf, args.LeaderID, "log mismatch, terms mismatch <log:%v, prevLogTerm:%v>, reply:%s", prevEntry.Term, args.PrevLogIndex, jsonEntryReply(reply))
	return
}

// appendNewEntries modifies the logs and append them
// - if an existing entry conflicts with a new one, i.e., same index different terms,
// delete the existing entry and all that follow it.
// - append any new entries not already in the log
func appendNewEntries(args *AppendEntriesArgs, rf *Raft) {
	// no new entries, abort
	if len(args.Entries) == 0 {
		APrintf(rf, args.LeaderID, "got request, hearbeat message")
		return
	}

	defer rf.persist()

	APrintf(rf, args.LeaderID, "got request, working on logs, rf.Log:%v, args.Entries:%v", len(rf.Log), len(args.Entries))

	// Find out which log is shorter
	// the leader: len(args.Entries)+args.PrevLogIndex
	// the follower: len(rf.Log)
	length := Min(len(rf.Log), len(args.Entries)+args.PrevLogIndex)
	if length == 0 {
		// no need to check the logs, just append all new entries
		rf.Log = append(rf.Log, args.Entries...)
		APrintf(rf, args.LeaderID, "got request, finish logs, new rf.Log:%v, args.Entries:%v", len(rf.Log), len(args.Entries))
		return
	}
	if args.PrevLogIndex == 0 {
		// rm all logs and append all new entries
		rf.Log = []*LogEntry{}
		rf.Log = append(rf.Log, args.Entries...)
		APrintf(rf, args.LeaderID, "got request, finish logs, new rf.Log:%v, args.Entries:%v", len(rf.Log), len(args.Entries))
		return
	}

	// find out conflict and duplicate entries
	dupEntryIndex := 0
	logCutIndex := len(rf.Log)
	for i := args.PrevLogIndex; i < length; i++ {
		currentEntry := rf.Log[i]
		argsEntry := args.Entries[i-args.PrevLogIndex]

		// the entry is already in rf, remove it from entries
		dupEntryIndex = i - args.PrevLogIndex

		if currentEntry.Term != argsEntry.Term {
			// cut off the difference from rf, and append the entries
			logCutIndex = i
			break
		} else {
			dupEntryIndex++
		}

	}

	rf.Log = append([]*LogEntry{}, rf.Log[:logCutIndex]...)
	args.Entries = append([]*LogEntry{}, args.Entries[dupEntryIndex:]...)
	rf.Log = append(rf.Log, args.Entries...)
	APrintf(rf, args.LeaderID, "got request, finish logs, new rf.Log:%v, args.Entries:%v", len(rf.Log), len(args.Entries))
}
