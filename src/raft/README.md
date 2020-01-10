# Notes
Some tips on how I finished the raft protocol.



### Lab 2A

Tricky parts,
1. Enfore Rule 5.1, which says, if RPC request or response contains term T > current Term: set currentTerm = T, convert to follower. This means,
+ in `RequestVote` RPC and `AppendEntries` RPC, if `args.Term` > `rf.CurrentTerm`, you should change `rf` based on the rule.
+ when processing `reply` from the RPC request, if `reply.Term` > `rf.CurrentTerm`, you should change `rf` based on the rule.
+ **Attention on the comparison operation!** It may seem obvious that there are `>`, `<`, and `=` operations. **Carefully with the `=` operation**, which is implicit applied in the following code block.

 ```
 if a > b {
     ...
 } else {
     this means a < b or a = b
     ...
 }
 ```
 I've stuck on this bug for one hour!

 2. Find the right range for election timeout, from the raft paper, *5.6 Timing and availability*,
 + `broadcastTime` should be an order of magnitude less than the election timeout. However, since the test allow at most 10 heartbeat messages per second, it limits the heartbeat timeout to be at least 100ms, and there's no way to make it an order of magnitude larger than the `broadcastTime`.
 + `MTBF`, medium time between failures. In real life this value should be way larger than seconds. In this test though, this value is 1 to 2 seconds. And it's required to select a new leader under 5 seconds. 

 3. Ignore old responses. This is the most tricky one, [check the thoughts from the TA here](https://thesquareplanet.com/blog/students-guide-to-raft/#term-confusion). Both RPC calls can give old responses.



### Lab 2B

Tips:
1. use `matchIndex` and `nextIndex` to decide whether majority servers have replicated the log. Note that these two indexes should have the relationship of `matchIndex = nextIndex - 1`. Also, make sure the leader's `matchIndex` and `nextIndex` are updated while sending RPCs to peers.
2. leader will send the same `AppendEntries` request multiple times, which means the follower will receive the same logs, and will give the same responses. We need to de-duplicate them. Meanwhile, since it's a RPC call, you should assume the only way the leader and follower can pass information around is go through the `reply` message.
3. All the indexes should be initialized as specified. I treated the first item in a slice to have an index of 0, as opposed to 1, which caused A LOT pain!
4. The best way to debug is print a lot logs! And I did! Also, try to write some unit tests, it really helped(I didn't have the time to refactor my test files, they were messy, so I didn't include them here.)



### Lab 2C

Because we are starting the slice index at 1, sometimes you may find the `ConflictIndex` starting at 0 when `rf.Log` is empty, when it happens, return the `ConflictIndex = 1` or do something at the `NextIndex`.