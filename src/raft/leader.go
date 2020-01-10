package raft

import "time"

// becomeLeader finishes the necessary steps of being a leader.
// 1. set its state to be leader.
// 2. set the vote count to be zero.
// 3. initialize all nextIndex values.
// 3. start leader state, which sends out hearbeat messages.
func (rf *Raft) becomeLeader() {
	RPrintf(rf, "elected as a leader")
	rf.State = Leader
	rf.VoteCount = 0

	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	for peerIndex := range rf.peers {
		rf.NextIndex[peerIndex] = len(rf.Log) + 1
		rf.MatchIndex[peerIndex] = 0
	}
}

// leaderState loops the candidate state
func (rf *Raft) leaderState() {
	for {
		rf.mu.Lock()
		currentState := rf.State
		rf.mu.Unlock()

		if currentState != Leader {
			RPrintf(rf, "abort leader loop")
			return
		}

		go rf.requestAppendEntries()

		time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) requestAppendEntries() {

	for peer := range rf.peers {
		// Bypass self
		if peer == rf.me {
			continue
		}

		args := rf.newAppendEntriesArgs(peer)
		go rf.processAppendEntries(peer, args)
	}
}

// newAppendEntriesArgs creates an AppendEntries args.
func (rf *Raft) newAppendEntriesArgs(peerIndex int) (args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogTerm := 0
	prevLogIndex := rf.NextIndex[peerIndex] - 1

	if prevLogIndex > 0 {
		prevLogTerm = rf.Log[prevLogIndex-1].Term
	}

	args = &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.CommitIndex,
		Entries:      rf.Log[prevLogIndex:],
	}
	return args
}

func (rf *Raft) processAppendEntries(peerIndex int, args *AppendEntriesArgs) {
	APrintf(rf, peerIndex, "prepare sending request append entries:%s", jsonEntryArgs(args))

	reply := &AppendEntriesReply{Term: args.Term}
	ok := rf.sendAppendEntries(peerIndex, args, reply)
	if !ok {
		// try it next time
		APrintf(rf, peerIndex, "got not OK response: <args:%v, reply:%v>", args.Term, reply.Term)
		return
	}

	rf.processAppendEntriesReply(peerIndex, args, reply)
}

func (rf *Raft) processAppendEntriesReply(peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore old reply
	if reply.Term != args.Term {
		APrintf(rf, peerIndex, "found old reply: <reply:%v, args.Term:%v>, abort", reply.Term, args.Term)
		return
	}

	// Rule 5.1
	if reply.Term > rf.CurrentTerm {
		APrintf(rf, peerIndex, "term is smaller: <term:%v, current:%v>, step down", reply.Term, rf.CurrentTerm)
		rf.becomeFollower(reply.Term)
		return
	}

	// ignore unneeded reply
	if rf.State != Leader {
		VPrintf(rf, peerIndex, "not a leader, won't process")
		return
	}

	if reply.Success {

		// a heartbeat reply, return
		if len(args.Entries) == 0 {
			APrintf(rf, peerIndex, "heartbeat message success")
			return
		}

		// update the matchIndex and nextIndex for the follower
		rf.MatchIndex[peerIndex] = args.PrevLogIndex + len(args.Entries)
		rf.NextIndex[peerIndex] += len(args.Entries)

		// filter out duplicate requests??
		if rf.NextIndex[peerIndex] > len(rf.Log)+1 {
			rf.NextIndex[peerIndex] -= len(args.Entries)
		}

		APrintf(rf, peerIndex, "append entry success, <matchIndex:%v, nextIndex:%v>, checking indexes...", rf.MatchIndex[peerIndex], rf.NextIndex[peerIndex])
		rf.checkMajorityCommit()
		return
	}

	if reply.ConflictTerm == 0 {
		rf.NextIndex[peerIndex] = reply.ConflictIndex
		APrintf(rf, peerIndex, "append entry failed, try with <matchIndex:%v, nextIndex:%v>", rf.MatchIndex[peerIndex], rf.NextIndex[peerIndex])
		return
	}

	for i, entry := range rf.Log {
		if entry.Term == reply.ConflictTerm {
			rf.NextIndex[peerIndex] = i + 1
			APrintf(rf, peerIndex, "append entry failed: <reply term:%v, args term:%v>, try with <matchIndex:%v, nextIndex:%v>", reply.Term, args.Term, rf.MatchIndex[peerIndex], rf.NextIndex[peerIndex])
			return
		}
	}

	rf.NextIndex[peerIndex] = reply.ConflictIndex
	APrintf(rf, peerIndex, "append entry failed, try with <matchIndex:%v, nextIndex:%v>", rf.MatchIndex[peerIndex], rf.NextIndex[peerIndex])
}

// TODO: optimize to give faster commitment index increment.
func (rf *Raft) checkMajorityCommit() {
	APrintf(rf, rf.me, "leader has: <commitIndex:%v, matchIndex:%v, nextIndex:%v>", rf.CommitIndex, rf.MatchIndex, rf.NextIndex)
	if rf.CurrentTerm != rf.Log[len(rf.Log)-1].Term {
		return
	}

	for i := 0; i < len(rf.MatchIndex); i++ {
		// abort the check if half not passed
		if i > len(rf.MatchIndex) {
			return
		}

		// bypass the initial value
		if rf.MatchIndex[i] == 0 {
			continue
		}

		// bypass old value
		if rf.MatchIndex[i] < rf.CommitIndex+1 {
			continue
		}

		// propose a new commitIndex
		newCommitIndex := rf.MatchIndex[i]
		count := 0

		for j := i; j < len(rf.MatchIndex); j++ {
			// check the rest to make sure we share the same index
			if rf.MatchIndex[j] == newCommitIndex && rf.MatchIndex[j] == rf.NextIndex[j]-1 {
				count++
			}
		}

		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
		// and log[N].term == currentTerm, set commitIndex = N
		if count > len(rf.peers)/2 {
			if rf.Log[newCommitIndex-1].Term == rf.CurrentTerm {
				rf.CommitIndex = newCommitIndex
			} else {
				rf.CommitIndex++
			}
			APrintf(rf, rf.me, "leader found majority replied, commitIndex is now: %v", rf.CommitIndex)
			return
		}
	}
}

// leaderAddEntry appends the command to its log as a new entry.
func (rf *Raft) leaderAddEntry(command interface{}) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != Leader {
		return -1, rf.CurrentTerm, false
	}

	entry := &LogEntry{
		Term:    rf.CurrentTerm,
		Command: command,
	}
	RPrintf(rf, "made entry:%s", jsonEntry(entry))
	rf.Log = append(rf.Log, entry)
	rf.MatchIndex[rf.me] = len(rf.Log)
	rf.NextIndex[rf.me]++
	RPrintf(rf, "leader has log:%s, matchIndex:%v, nextIndex:%v", jsonLog(rf.Log), rf.MatchIndex, rf.NextIndex)

	rf.persist()

	return rf.MatchIndex[rf.me], rf.CurrentTerm, true
}
