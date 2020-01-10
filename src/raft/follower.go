package raft

import "time"

// becomeFollower finishes the necessary steps of being a follower.
// should be used inside a lock.
func (rf *Raft) becomeFollower(term int) {
	if rf.State != Follower {
		RPrintf(rf, "step down as a follower")
		rf.State = Follower
	}
	rf.CurrentTerm = term
	rf.VoteCount = 0
	rf.VotedFor = -1
	rf.resetElectionTimeout()
}

// followerState loops the follower state
func (rf *Raft) followerState() {
	for {
		rf.mu.Lock()
		currentState := rf.State
		expiredAt := rf.ExpiredAt
		rf.mu.Unlock()

		if currentState != Follower {
			RPrintf(rf, "Expected to have a follower, abort")
			return
		}

		if hasTimedOut(expiredAt) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// if rf.State != Follower {
			// 	RPrintf(rf, "timeout but not a follower, abort follower loop")
			// 	return
			// }
			RPrintf(rf, "has timed out, entering election...")
			rf.becomeCandidate()

			// end follower state
			return
		}

		time.Sleep(time.Duration(checkInterval) * time.Millisecond)
	}
}
