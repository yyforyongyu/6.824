package raft

import "time"

// becomeCandidate finishes the necessary steps of being a candidate.
// should be used inside a lock.
func (rf *Raft) becomeCandidate() {
	RPrintf(rf, "promoted as a candidate")
	rf.State = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.VoteCount = 1
	rf.resetElectionTimeout()
}

// candidateState loops the candidate state
func (rf *Raft) candidateState() {
	// start election
	rf.mu.Lock()
	args := rf.newRequestVoteArgs()
	rf.mu.Unlock()
	go rf.startElection(args)

	// wait for timeout to restart an election, or,
	// becomes a leader/follower, abort
	for {
		rf.mu.Lock()
		currentState := rf.State
		expiredAt := rf.ExpiredAt
		rf.mu.Unlock()

		if currentState != Candidate {
			RPrintf(rf, "abort candidate loop")
			return
		}

		if hasTimedOut(expiredAt) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// if rf.State != Candidate {
			// 	RPrintf(rf, "timeout but not a candidate, abort candidate loop")
			// 	return
			// }
			RPrintf(rf, "has timed out, entering a new election...")
			rf.becomeCandidate()

			// end current candidate state, entering a new one
			return
		}

		time.Sleep(time.Duration(checkInterval) * time.Millisecond)
	}
}

// newRequestVoteArgs creates new args for request voting.
// should be used in a lock
func (rf *Raft) newRequestVoteArgs() *RequestVoteArgs {
	lastLogTerm := 0
	lastLogIndex := 0
	if len(rf.Log) > 0 {
		lastLogIndex = len(rf.Log)
		lastLogTerm = rf.Log[lastLogIndex-1].Term
	}
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
}

// startElection sends RequestVote RPCs to all other servers
// Notice there is no lock here, so the order of args.Term cannot
// be guaranteed, see ignoreOldReply for details.
func (rf *Raft) startElection(args *RequestVoteArgs) {

	for peer := range rf.peers {
		// no need to send to self
		if peer == rf.me {
			continue
		}
		go rf.processVoteRequest(peer, args)
	}
}

func (rf *Raft) processVoteRequest(peerIndex int, args *RequestVoteArgs) {
	VPrintf(rf, peerIndex, "started election with args:%s", jsonVoteArgs(args))

	reply := &RequestVoteReply{Term: args.Term}
	ok := rf.sendRequestVote(peerIndex, args, reply)
	if !ok {
		VPrintf(rf, peerIndex, "got response NOT ok, abort")
		return
	}

	rf.processVoteRequestReply(peerIndex, args, reply)
}

func (rf *Raft) processVoteRequestReply(peerIndex int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore old reply
	if reply.Term != args.Term {
		VPrintf(rf, peerIndex, "found old reply: <reply:%v, args.Term:%v>, abort", reply.Term, args.Term)
		return
	}

	// Rule 5.1
	if reply.Term > rf.CurrentTerm {
		VPrintf(rf, peerIndex, "term is smaller: <term:%v, current:%v>, step down", reply.Term, rf.CurrentTerm)
		rf.becomeFollower(reply.Term)
		return
	}

	// ignore unneeded reply
	if rf.State != Candidate {
		VPrintf(rf, peerIndex, "not a candidate, won't process")
		return
	}

	if reply.VoteGrandted {
		rf.VoteCount++
		VPrintf(rf, peerIndex, "got a vote, now <voteCount:%v>", rf.VoteCount)

		// get majority vote, becomes a leader!
		if rf.VoteCount > len(rf.peers)/2 {
			VPrintf(rf, peerIndex, "got majority votes, <voteCount:%v>", rf.VoteCount)
			rf.becomeLeader()
		}
	}
}
