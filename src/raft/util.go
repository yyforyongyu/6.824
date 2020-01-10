package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

// DPrintf is the default log
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// RPrintf prints Raft logs
func RPrintf(rf *Raft, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		prefix := fmt.Sprintf("Raft:<%s:%v, Term:%v>:", rf.State, rf.me, rf.CurrentTerm)
		DPrintf(prefix+format, a...)
	}
	return
}

// VPrintf prints RequestVote RPC logs
func VPrintf(rf *Raft, peerIndex int, format string, a ...interface{}) (n int, err error) {
	if Debug == 2 || Debug == 5 || Debug == 6 || Debug == 9 {
		prefix := fmt.Sprintf("RequestVote:<%s:%v, Term:%v> -> <peer:%v>:", rf.State, rf.me, rf.CurrentTerm, peerIndex)
		DPrintf(prefix+format, a...)
	}
	return
}

// APrintf prints AppendEntries RPC logs
func APrintf(rf *Raft, peerIndex int, format string, a ...interface{}) (n int, err error) {
	if Debug == 3 || Debug == 5 || Debug == 7 || Debug == 9 {
		prefix := fmt.Sprintf("AppendEntry:<%s:%v, Term:%v> -> <peer:%v>:", rf.State, rf.me, rf.CurrentTerm, peerIndex)
		DPrintf(prefix+format, a...)
	}
	return
}

// CPrintf prints requests in config.go
func CPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 4 || Debug == 7 || Debug == 6 || Debug == 9 {
		log.Printf(format, a...)
	}
	return
}

func jsonVoteArgs(args *RequestVoteArgs) []byte {
	bytes, _ := json.Marshal(args)
	return bytes
}

func jsonEntryArgs(args *AppendEntriesArgs) []byte {
	bytes, _ := json.Marshal(&struct {
		Term         int
		LeaderID     int
		LeaderCommit int
		PrevLogIndex int
		PrevLogTerm  int
		Entries      int
	}{
		Term:         args.Term,
		LeaderID:     args.LeaderID,
		LeaderCommit: args.LeaderCommit,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      len(args.Entries),
	})
	return bytes
}

func jsonEntryReply(reply *AppendEntriesReply) []byte {
	bytes, _ := json.Marshal(reply)
	return bytes
}

func jsonLog(log []*LogEntry) []byte {
	bytes, _ := json.Marshal(&struct {
		Log int
	}{len(log)})
	return bytes
}

func jsonEntry(entry *LogEntry) []byte {
	bytes, _ := json.Marshal(entry)
	return bytes
}

// resetElectionTimeout resets the election timeout
// min value: greater than heartbeat intervals, which is 100ms(10 in 1 second), choose 300ms
// max value: smaller than 5 seconds / N, where N is the max Rounds needed for leader election.
// As a start point, choose 5, so the max is 500ms.
func (rf *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	max := 500
	min := 300
	ms := rand.Intn(max-min) + min
	rf.ExpiredAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

// hasTimedOut checks whether the election timeout has reached.
func hasTimedOut(expiredAt time.Time) bool {
	if time.Now().Sub(expiredAt) > 0 {
		return true
	}
	return false
}

// Min gives the smaller value of the two ints
func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

// Min gives the greater value of the two ints
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
