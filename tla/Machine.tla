------------------------------ MODULE Machine ------------------------------
EXTENDS Integers, FiniteSets, TLC, Sequences
CONSTANTS N, \* set of all nodes 
          K  \* set of all keys
VARIABLES \* part of mnevis
          machines,
          leader,
          \* meta data used for debugging, state constraints or as a model for invariants to be checked against
          node_change_ctr,
          machine_action,
          ooo_writes,
          write_log

M_vars == << machines, leader, node_change_ctr, machine_action, ooo_writes, write_log >>

          
\*--------------------------------------------------          
\* Type definitions
\*--------------------------------------------------
NodeHost == { 0 } \union N
Command == { "nop", "locker_up", "locker_down", "commit", "blacklist" }
Role == {"leader", "follower", "candidate", "offline" }

NoOpEntry == [index: Nat, command: Command]
LockerUpDownEntry == [index: Nat, command: Command, pid: Nat]
TransactionValue == [tid: Nat,
                     writes: [K -> Nat],
                     locker_pid: Nat]
CommitEntry == [index: Nat, command: Command, transaction: TransactionValue]

BlacklistValue == [locker_pid: Nat, tid: Nat]
BlacklistEntry == [index: Nat, command: Command, transaction: BlacklistValue]

Entry == LockerUpDownEntry \union CommitEntry \union BlacklistEntry

WriteLogValue == [tid: Nat, value: Nat, log_index: Nat]
StoreValue == [value: Nat, version: Nat, log: SUBSET WriteLogValue]
ProcTran == [pid: Nat, tid: Nat]
Machine == [ role: Role, 
             log: SUBSET Entry, 
             last_index: Nat, 
             committed_index: Nat,
             applied_index: Nat,
             locker_pid: Nat,
             monitored_locker_pid: Nat,
             term: Nat,
             kvs: [K -> StoreValue],
             blacklist: SUBSET Nat,
             applied_txns: SUBSET TransactionValue]
         
\*--------------------------------------------------          
\* Null/empty representations
\*--------------------------------------------------

NoStoreValue == [value |-> 0, version |-> 0, log |-> {}]
            
NoEntry == [index |-> 0, command |-> "nop"]


\*--------------------------------------------------
\* Helper state formulae
\*--------------------------------------------------

M_LiveNodes ==
    { n \in N : machines[n].role # "offline" }
    
QuorumOnline ==
    Cardinality(M_LiveNodes) >= Cardinality(N) \div 2 + 1
  
ParticipatingNodes ==
    { n \in N : machines[n].role \in { "follower", "leader" } }

QuorumParticipating ==
    Cardinality(ParticipatingNodes) >= Cardinality(N) \div 2 + 1

M_TermLessThanEq(term) ==
    \A n \in N : machines[n].term <= term
        
M_LastIndexLessThanEq(log_index) ==
    \A n \in N : machines[n].last_index <= log_index
    
\* locker pid also the term in this model
M_HasHighestLockerTerm(n, term) ==
    term > machines[n].locker_pid
    
\* Is this locker pid known by any follower
M_FollowerHasLocker(pid, ldr) ==
    \E n \in N : /\ n # ldr
                 /\ machines[n].role # "offline"
                 /\ machines[n].locker_pid = pid
                 
M_IsLeader(n) ==
    leader = n
    
M_IsFollower(n) ==
    machines[n].role = "follower"

M_LeaderExists ==
    leader # 0
    
M_LeaderMatches(n, term) ==
    /\ leader # 0
    /\ leader = n
    /\ machines[leader].term = term    

M_LeaderNode ==
    leader

M_LeaderTerm ==
    machines[leader].term
    
M_LeaderLastIndex ==
    machines[leader].last_index    

\* Only a node with highest term and index can become leader
CanBeLeader(n) ==
    \A node \in N : 
        /\ machines[n].last_index >= machines[node].last_index
        /\ machines[n].term       >= machines[node].term
        
\* The node has a locker_down command in its log that it is yet to apply
M_ExistsPendingLockerDownEntry ==
    /\ \E e \in machines[leader].log :
        /\ e.command = "locker_down"
        /\ e.index > machines[leader].applied_index
        
M_IsCurrentLocker(n, pid) ==
    machines[n].locker_pid = pid
    
M_IsLockerMonitored(pid) ==
    /\ leader # 0
    /\ machines[leader].monitored_locker_pid = pid            

M_AppliedIndexBehindLastIndex(n) ==
    machines[n].committed_index > machines[n].applied_index

M_GetNextUnappliedEntry(n) ==
    CHOOSE e \in machines[n].log : e.index = machines[n].applied_index + 1

\* Returns true if the given entry was applied by the leader
M_TransactionWasApplied(log_index, locker_pid, process_pid, tid) ==
    /\ leader # 0
    /\ machines[leader].applied_index >= log_index
    /\ \E t \in machines[leader].applied_txns : 
        /\ t.locker_pid = locker_pid
        /\ t.tid        = tid

\* Returns true if the given entry was discarded by the leader                                          
M_TransactionWasDiscarded(log_index, locker_pid, process_pid, tid) ==
    /\ leader # 0
    /\ machines[leader].applied_index >= log_index
    /\ ~\E t \in machines[leader].applied_txns : 
        /\ t.locker_pid = locker_pid
        /\ t.tid        = tid       

\*--------------------------------------------------          
\* Invariants
\*--------------------------------------------------

\* Ensure that the variables hold expected types
M_TypeOK == 
    /\ machines \in [N -> Machine]
    /\ leader \in NodeHost
    /\ write_log \in [K -> SUBSET WriteLogValue]

\* Check for out-of-order writes
M_NoOutOfOrderWritesInvariant ==
    ooo_writes = 0
    
\* Check for data loss
\* Data loss is:
\* There exists a key, which has a write in the write_log
\* and for that write there exists a node whose applied_index >= the log_index of that write
\* and whose kvs does not have that write in its log
DataLoss ==
    \E k \in K :
        \E write \in write_log[k] :
            /\ \E n \in N :
                /\ machines[n].applied_index >= write.log_index
                /\ ~\E sv \in machines[n].kvs[k].log : 
                    /\ write.tid        = sv.tid
                    /\ write.value      = sv.value
                    /\ write.log_index  = sv.log_index

M_NoDataLossInvariant == 
    ~DataLoss

\*--------------------------------------------------          
\* Next state formulae
\*--------------------------------------------------

M_Init == 
    /\ machines = [n \in N |-> 
                       [log |-> {}, 
                        term |-> 0,
                        last_index |-> 0, 
                        committed_index |-> 0,
                        applied_index |-> 0, 
                        locker_pid |-> 0,
                        monitored_locker_pid |-> 0,
                        role |-> "candidate",
                        kvs |-> [k \in K |-> NoStoreValue],
                        blacklist |-> {},
                        applied_txns |-> {}]]
    /\ leader = 0
    /\ node_change_ctr = 0
    /\ machine_action = "init"
    /\ ooo_writes = 0
    /\ write_log = [k \in K |-> {}]


\* -------------------------------------------------------
\* Append entry to log
\* -------------------------------------------------------        
    
\* Create an entry according to the command
CreateEntry(command, data, index) ==
    CASE command = "locker_up" ->   [index |-> index, command |-> command, pid |-> data]
      [] command = "locker_down" -> [index |-> index, command |-> command, pid |-> data]
      [] command = "commit" ->      [index |-> index, command |-> command, transaction |-> data]
      [] command = "blacklist" ->   [index |-> index, command |-> command, transaction |-> data]
      [] OTHER -> NoOpEntry    
    
\* append a command to the leader's log
M_AppendCommandToLeader(command, data) ==
    /\ leader # 0
    /\ machines' = [machines EXCEPT ![leader] = 
                        [machines[leader] EXCEPT !.last_index = @ + 1,
                                                 !.log        = @ \union { 
                                                                    CreateEntry(command, 
                                                                       data, 
                                                                       machines[leader].last_index + 1)
                                                                    }]]   
    /\ machine_action' = "Append " \o command \o " Command To Leader"
    /\ UNCHANGED << leader, node_change_ctr, ooo_writes, write_log >>                                                   

\* -------------------------------------------------------
\* Leader elections, nodes starting, stopping
\* -------------------------------------------------------    

\* Leader election is a single atomic action in this simplified model
\* Runs if no leader currently exists    
M_WinsLeaderElection(n) ==
    /\ QuorumOnline
    /\ CanBeLeader(n) 
    /\ machines' = [nd \in N |->
                IF nd = n THEN
                    [machines[nd] EXCEPT !.role = "leader",
                                         !.term = @ + 1]
                ELSE
                    [machines[nd] EXCEPT !.role            = "follower",
                                         !.term            = machines[nd].term + 1,
                                         !.log             = IF machines[nd].last_index > machines[n].last_index 
                                                                THEN machines[n].log 
                                                                ELSE @,
                                         !.last_index      = IF @ > machines[n].last_index 
                                                                THEN machines[n].last_index 
                                                                ELSE @,
                                         !.committed_index = IF @ > machines[n].committed_index 
                                                                THEN machines[n].committed_index 
                                                                ELSE @]]
    /\ leader' = n
    /\ machine_action' = "LeaderElection"
    /\ UNCHANGED << node_change_ctr, ooo_writes, write_log >>    
    

\* Nodes goes offline taking the mnevis machine with it
\* If its the leader then set the leader id to 0
M_NodeGoesOffline(n) ==
    /\ machines[n].role # "offline"
    /\ machines' = [nd \in N |-> IF nd = n 
                                    THEN [machines[n] EXCEPT !.role = "offline"]
                                    ELSE machines[nd]]
    /\ \/ /\ leader  = n
          /\ leader' = 0
       \/ /\ leader  # n
          /\ leader' = leader
    /\ node_change_ctr' = node_change_ctr + 1
    /\ machine_action'  = "NodeGoesOffline n=" \o ToString(n)
    /\ UNCHANGED << ooo_writes, write_log >>    
    
\* Node comes back online as a candidate (using a simplified model of Raft)
M_NodeGoesOnline(n) ==
    /\ machines[n].role = "offline"
    /\ machines'        = [nd \in DOMAIN machines |-> 
                                IF nd = n 
                                   THEN [machines[n] EXCEPT !.role                 = "candidate",
                                                            !.monitored_locker_pid = 0]
                                   ELSE machines[nd]]
    /\ node_change_ctr' = node_change_ctr + 1
    /\ machine_action'  = "NodeGoesOnline n=" \o ToString(n)
    /\ UNCHANGED << leader, ooo_writes, write_log >>

\* -------------------------------------------------------
\* Replicate commands
\* -------------------------------------------------------
    
\* map the log of entries to a map of indexes
LogIndexes(n) ==
    { l.index : l \in machines[n].log }

\* map of nodes that have this index
NodesWithIndex(index) ==
    { n \in N : /\ machines[n].role \in { "leader", "follower" }
                /\ index \in LogIndexes(n) }

\* if with the replication of the index to another follower, will it be committed?    
IsCommandCommittedOnReplication(index) ==
    /\ Cardinality(NodesWithIndex(index)) >= (Cardinality(N) \div 2)

Followers == 
    { n \in N : machines[n].role = "follower" }
    
\* Replicates a single command to a follower
\* sets committed index on follower and potentially on leader
M_ReplicateCommand ==
    /\ leader # 0
    /\ QuorumParticipating
    /\ \E follower \in Followers: 
        /\ machines[follower].last_index < machines[leader].last_index
        /\ LET entry == CHOOSE e \in machines[leader].log : e.index = machines[follower].last_index + 1
           IN /\ machines' = [nd \in N |->
                    IF nd = leader THEN \* if its the leader then update its committed_index if needed
                        IF machines[nd].committed_index < entry.index THEN
                            IF IsCommandCommittedOnReplication(entry.index) THEN
                                [machines[nd] EXCEPT !.committed_index = entry.index]
                            ELSE
                                machines[nd]
                        ELSE
                            machines[nd]
                    ELSE \* if its the dest node then append to its log and update its committed_index if needed
                        IF nd = follower THEN
                            [machines[nd] EXCEPT !.log             = @ \union { entry },
                                                 !.last_index      = entry.index,
                                                 !.committed_index = machines[leader].committed_index]
    
                        ELSE machines[nd]]
              /\ machine_action' = "Replicate " \o entry.command \o " command from n=1 " \o ToString(leader) \o " to n=" \o ToString(follower)
    /\ UNCHANGED << leader, node_change_ctr, ooo_writes, write_log >>    
    
    
\* -------------------------------------------------------
\* APPLY COMMANDS
\* -------------------------------------------------------

SetAppliedIndexOnly(n) ==
    machines' = [machines EXCEPT ![n] = [machines[n] EXCEPT !.applied_index = @ + 1]]
    
M_SetAppliedIndexOnly(n) ==
    /\ SetAppliedIndexOnly(n)    
    /\ UNCHANGED << leader, machine_action, node_change_ctr, ooo_writes, write_log >>


\* APPLY LOCKER_UP
\* -------------------------------------------------------

M_SetLockerPid(n, pid) ==
    /\ machines' = [machines EXCEPT ![n] = 
                         [machines[n] EXCEPT !.applied_index = @ + 1,
                                             !.locker_pid    = pid]]
    /\ UNCHANGED << leader, machine_action, node_change_ctr, ooo_writes, write_log >>
                                             
M_SetAndMonitorLockerPid(n, pid) ==
    /\ machines' = [machines EXCEPT ![n] = 
                         [machines[n] EXCEPT !.applied_index        = @ + 1,
                                             !.locker_pid           = pid,
                                             !.monitored_locker_pid = pid]]
    /\ UNCHANGED << leader, machine_action, node_change_ctr, ooo_writes, write_log >>
                                                

\* -------------------------------------------------------
\* APPLY COMMIT
\* -------------------------------------------------------

\* For every key in the kvs, if there is a corresponding write in the transaction
\* then set its value with the write and add it to the log of writes to that key
\* increment the applied index
ExecuteWrites(n, transaction, log_index) ==
    /\ machines' = [machines EXCEPT ![n] =
                    [machines[n] EXCEPT !.applied_index = log_index,
                                        !.applied_txns  = @ \union { transaction },
                                        !.kvs           = [k \in K |->
                                                            IF transaction.writes[k] > 0 
                                                            THEN [@[k] EXCEPT !.value   = transaction.writes[k],
                                                                              !.version = @ + 1,
                                                                              !.log     = @ \union 
                                                                                            {[tid      |-> transaction.tid, 
                                                                                             value     |-> transaction.writes[k],
                                                                                             log_index |-> log_index]}]                                                    
                                                            ELSE
                                                                @[k]]]]
    /\ \/ \E k \in K :
            /\ transaction.writes[k] > 0
            /\ transaction.writes[k] <= machines[n].kvs[k].value
            /\ ooo_writes' = ooo_writes + 1
       \/ ooo_writes' = ooo_writes
    
\* To be able to commit a transaction it must have the current locker_pid 
\* and must not be blacklisted
CanApplyCommit(n, entry) ==
    /\ machines[n].locker_pid = entry.transaction.locker_pid
    /\ entry.transaction.tid \notin machines[n].blacklist 

\* If the commit is valid (right locker and not on blacklist) then apply it
M_ApplyCommit(n, entry) ==
    /\ entry.command    = "commit"
    /\ machines[n].role \in {"leader", "follower"}
    /\ machine_action'  = "ApplyCommitCommand m=" \o ToString(n)
    /\ CanApplyCommit(n, entry)
    /\ ExecuteWrites(n, entry.transaction, entry.index)
    /\ UNCHANGED << leader, node_change_ctr, write_log >>

\* If the commit is not valid (wrong locker or on blacklist) then discard it
M_DiscardCommit(n, entry) ==
    /\ entry.command    = "commit"
    /\ machines[n].role \in {"leader", "follower"}
    /\ machine_action'  = "ApplyCommitCommand apply_node=" \o ToString(n)
    /\ ~CanApplyCommit(n, entry) 
    /\ SetAppliedIndexOnly(n)
    /\ UNCHANGED << leader, node_change_ctr, ooo_writes, write_log >>          
          
\* -------------------------------------------------------
\* APPLY BLACKLIST
\* -------------------------------------------------------

\* Add the transaction to the blacklist on this node
M_AddTransactionToBlacklist(n, tid) ==
    /\ machines' = [machines EXCEPT ![n].blacklist     = @ \union {tid},
                                    ![n].applied_index = @ + 1]
    /\ machine_action' = "AddTransactionToBlacklist apply_node=" \o ToString(n)
    /\ UNCHANGED << leader, node_change_ctr, ooo_writes, write_log >>

\* -------------------------------------------------------
\* MODEL
\* -------------------------------------------------------
    
\* The write_log is a model of what we should see in the node key value store logs
\* It is used solely by the data loss invariant
MODEL_AppendToWriteLogModel(transaction) ==
    /\ write_log' = [k \in K |->
                    IF transaction.writes[k] > 0 THEN
                        write_log[k] \union {[tid       |-> transaction.tid,
                                              value     |-> transaction.writes[k],
                                              log_index |-> transaction.log_index]}
                    ELSE
                        write_log[k]]
    /\ UNCHANGED << leader, machine_action, machines, node_change_ctr, ooo_writes, machine_action >>                        




=============================================================================