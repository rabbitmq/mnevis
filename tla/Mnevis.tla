------------------------------ MODULE Mnevis ------------------------------
EXTENDS Integers, FiniteSets, TLC, Sequences, Machine, Locker, Process

\* TODO:
\* Add network partitions
\* Add blatransactiond process down
\* What is lock/unlock transaction about?
\* Investigate lock timeouts


\*--------------------------------------------------          
\* Section 1 - Invariants
\*--------------------------------------------------

\* Ensure that the variables hold expected types
TypeOK == 
    /\ M_TypeOK
    /\ L_TypeOK
    /\ P_TypeOK
    
Invariants ==
    /\ M_NoOutOfOrderWritesInvariant
    /\ M_NoDataLossInvariant
    /\ L_OnlyOneLeaderLockerInvariant
    \* /\ NoLeaderLockersNoPendingLockerDown

\*--------------------------------------------------          
\* Section 2 - Next state formulae
\*--------------------------------------------------

Init == 
    /\ M_Init
    /\ L_Init
    /\ P_Init

\*--------------------------------------------------
\* Leader elections and nodes starting, stopping
\*--------------------------------------------------

\* A leader election is a single atomic action in this simplied model
\* A new locker may be created as a result
PerformLeaderElection ==
    \E n \in M_LiveNodes :
        /\ M_WinsLeaderElection(n)
        /\ L_SpawnLocker(n)
        /\ UNCHANGED << P_vars >>

\* When a node goes offline, we update the state of:
\* - the machine to be offline but without losing its persistent data
\* - Any locker processes with monitors on processes hosted on that node are notified
\* - Any processes on that node are killed
NodeGoesOffline(n) ==
    LET process_pids_on_node == P_ProcessPidsOnNode(n)
    IN 
        /\ M_NodeGoesOffline(n)
        /\ L_NodeGoesOffline(n, process_pids_on_node)
        /\ P_NodeGoesOffline(n)

NodeGoesOnline(n) ==
    /\ M_NodeGoesOnline(n)
    /\ UNCHANGED << P_vars, L_vars >> 
    
\*--------------------------------------------------
\* Locker operations
\*--------------------------------------------------

\* A locker sends a locker_up command to the leader machine and becomes a candidate locker
LockerUpSent ==
    /\ M_LeaderExists
    /\ \E l \in L :
        LET locker_pid == L_LockerPid(l)
        IN /\ L_BecomeCandidate(l)
           /\ M_AppendCommandToLeader("locker_up", locker_pid)
           /\ UNCHANGED << P_vars >>

\* A non leader locker dies which only directly affects the locker itself    
NonLeaderLockerDies ==
    /\ L_NonLeaderLockerDies
    /\ UNCHANGED << P_vars, M_vars >>
    
\* A leader locker dies and if it is being monitored by the leader machine, 
\* a locker_down command is added to leader machine's log    
LeaderLockerDies ==
    \E l \in L_LeaderLockers :
        LET locker_pid == L_LockerPid(l)
        IN
            /\ L_LeaderLockerDies(l)
            /\ \/ /\ M_IsLockerMonitored(locker_pid)
                  /\ M_AppendCommandToLeader("locker_down", locker_pid)
                  /\ UNCHANGED << P_vars >>
               \/ /\ ~M_IsLockerMonitored(locker_pid)
                  /\ UNCHANGED << P_vars, M_vars >>
    
\* A process that has an open transaction with a locker has previously died
\* The locker blacklists the tid locally and if there is a leader machine, sends it a blacklist command
ProcessWithTransactionIsDown ==
    \E l \in L_LockersWithDeadTransactions :
        \E tran \in L_DeadTransactionsOf(l) :
            LET locker_pid == L_LockerPid(l)
            IN
                /\ L_BlacklistsLocally(locker_pid, tran.tid)
                /\ \/ /\ M_LeaderExists
                      /\ M_AppendCommandToLeader("blacklist", [tid |-> tran.tid, locker_pid |-> locker_pid])
                      /\ UNCHANGED << P_vars >>
                   \/ /\ ~M_LeaderExists
                      /\ UNCHANGED << P_vars, M_vars >>
    
\* A locker is falsely notified that a process with an open transaction is down
\* As with any down event, the locker blacklists the tid locally and 
\* if there is a leader machine, sends it a blacklist command
FalseProcessDown ==
    \E l \in L :
        \E tran \in L_TransactionsOf(l) :
            LET locker_pid == L_LockerPid(l)
            IN
                /\ L_BlacklistsLocally(locker_pid, tran.tid)
                /\ \/ /\ M_LeaderExists
                      /\ M_AppendCommandToLeader("blacklist", [tid |-> tran.tid, locker_pid |-> locker_pid])
                      /\ UNCHANGED << P_vars >>
                   \/ /\ ~M_LeaderExists
                      /\ UNCHANGED << P_vars, M_vars >>

\* A locker previously blacklisted a tid but there was not machine leader
\* Now there is a leader the locker sends the blacklist command
LeaderNotifiedOfPendingBlacklist ==
    /\ M_LeaderExists
    /\ \E l \in L_LockersWithPendingBlacklistTrans :
            \E tran \in L_PendingBlacklistTransOf(l) :
                LET locker_pid == L_LockerPid(l)
                IN /\ L_LeaderNotifiedOfBlacklisting(l, tran)
                   /\ M_AppendCommandToLeader("blacklist", [tid |-> tran.tid, locker_pid |-> locker_pid])
                   /\ UNCHANGED << P_vars >>


\*--------------------------------------------------
\*  Command Replication
\*--------------------------------------------------

\* Replicate an entry to a follower
ReplicateCommand ==
    /\ M_ReplicateCommand
    /\ UNCHANGED << P_vars, L_vars >>

\*--------------------------------------------------
\*  APPLY locker_up
\*--------------------------------------------------

LeaderAppliesLockerUpAndConfirms(n, entry) ==
    /\ M_IsLeader(n)
    /\ M_HasHighestLockerTerm(n, entry.pid)
    /\ M_SetAndMonitorLockerPid(n, entry.pid)
    /\ L_ConfirmNewLocker(entry.pid, machines[n].locker_pid)
    /\ UNCHANGED << P_vars >>
    
LeaderAppliesLockerUpAndRejects(n, entry) ==
    /\ M_IsLeader(n)
    /\ ~M_HasHighestLockerTerm(n, entry.pid)
    /\ L_RejectNewLocker(entry.pid)
    /\ M_SetAppliedIndexOnly(n)
    /\ UNCHANGED << P_vars >>
   
FollowerAppliesLockerUp(n, entry) == 
    /\ M_IsFollower(n)
    /\ M_HasHighestLockerTerm(n, entry.pid)
    /\ M_SetLockerPid(n, entry.pid)
    /\ UNCHANGED << P_vars, L_vars >>
          
FollowerDiscardsLockerUp(n, entry) == 
    /\ M_IsFollower(n)
    /\ ~M_HasHighestLockerTerm(n, entry.pid)
    /\ M_SetAppliedIndexOnly(n)
    /\ UNCHANGED << P_vars, L_vars >>

ApplyLockerUpCommand(n, entry) ==
    /\ entry.command = "locker_up"
    /\ \/ LeaderAppliesLockerUpAndConfirms(n, entry)
       \/ LeaderAppliesLockerUpAndRejects(n, entry)
       \/ FollowerAppliesLockerUp(n, entry)
       \/ FollowerDiscardsLockerUp(n, entry)

\*--------------------------------------------------
\*  APPLY locker_down
\*--------------------------------------------------

LeaderAppliesLockerDownAndSpawnsNewLocker(n, entry) ==
    /\ M_IsLeader(n)
    /\ M_IsCurrentLocker(n, entry.pid)
    /\ M_SetLockerPid(n, 0)
    /\ L_SpawnLocker(n)
    /\ UNCHANGED << P_vars >>
    
LeaderDiscardsLockerDown(n, entry) ==
    /\ M_IsLeader(n)
    /\ ~M_IsCurrentLocker(n, entry.pid)
    /\ M_SetAppliedIndexOnly(n)
    /\ UNCHANGED << P_vars, L_vars >>
    
FollowerAppliesLockerDown(n, entry) ==
    /\ M_IsFollower(n)
    /\ M_IsCurrentLocker(n, entry.pid)
    /\ M_SetLockerPid(n, 0)
    /\ UNCHANGED << P_vars, L_vars >>
    
FollowerDiscardsLockerDown(n, entry) ==
    /\ M_IsFollower(n)
    /\ ~M_IsCurrentLocker(n, entry.pid)
    /\ M_SetAppliedIndexOnly(n)
    /\ UNCHANGED << P_vars, L_vars >>
    
ApplyLockerDownCommand(n, entry) ==
    /\ entry.command = "locker_down"
    /\ \/ LeaderAppliesLockerDownAndSpawnsNewLocker(n, entry)
       \/ LeaderDiscardsLockerDown(n, entry)
       \/ FollowerAppliesLockerDown(n, entry)
       \/ FollowerDiscardsLockerDown(n, entry)
       

\*--------------------------------------------------
\*  APPLY commit
\*--------------------------------------------------

ApplyCommitCommand(n, entry) ==
    /\ \/ M_ApplyCommit(n, entry)
       \/ M_DiscardCommit(n, entry)
    /\ UNCHANGED << P_vars, L_vars >>
    
\*--------------------------------------------------
\*  APPLY blacklist
\*--------------------------------------------------    
                                                    

LeaderAppliesBlacklistTid(n, entry) ==
    /\ M_IsLeader(n)
    /\ M_IsCurrentLocker(n, entry.transaction.locker_pid)
    /\ M_AddTransactionToBlacklist(n, entry.transaction.tid)    
    /\ L_ConfirmBlacklisting(entry.transaction.locker_pid, entry.transaction.tid)
    /\ UNCHANGED << P_vars >>    

LeaderDiscardsBlacklistTid(n, entry) ==
    /\ M_IsLeader(n)
    /\ ~M_IsCurrentLocker(n, entry.transaction.locker_pid)
    /\ M_SetAppliedIndexOnly(n)
    /\ UNCHANGED << P_vars, L_vars >>
    
FollowerAppliesBlacklistTid(n, entry) ==
    /\ M_IsFollower(n)
    /\ M_IsCurrentLocker(n, entry.transaction.locker_pid)
    /\ M_AddTransactionToBlacklist(n, entry.transaction.tid)
    /\ UNCHANGED << P_vars, L_vars >>    

FollowerDiscardsBlacklistTid(n, entry) ==
    /\ M_IsFollower(n)
    /\ ~M_IsCurrentLocker(n, entry.transaction.locker_pid)
    /\ M_SetAppliedIndexOnly(n)
    /\ UNCHANGED << P_vars, L_vars >>

\* Apply the blacklist command
ApplyBlacklistCommand(n, entry) ==
    /\ entry.command = "blacklist"
    /\ \/ LeaderAppliesBlacklistTid(n, entry)
       \/ LeaderDiscardsBlacklistTid(n, entry)
       \/ FollowerAppliesBlacklistTid(n, entry)
       \/ FollowerDiscardsBlacklistTid(n, entry)
    

\*--------------------------------------------------
\*  APPLY command
\*-------------------------------------------------- 

ApplyCommand ==
    \E n \in M_LiveNodes : 
        /\ M_AppliedIndexBehindLastIndex(n)
        /\ LET entry == M_GetNextUnappliedEntry(n)
           IN \/ ApplyLockerUpCommand(n, entry)
              \/ ApplyLockerDownCommand(n, entry)
              \/ ApplyCommitCommand(n, entry)
              \/ ApplyBlacklistCommand(n, entry)

\*------------------------------------------------------------
\* TRANSACTIONS, WRITES and LOCKS
\* There are also events like:
\* - process down events caused by eother processes dying or false positives
\*------------------------------------------------------------

\* A process starts
ProcessStarts(p) ==
    /\ P_ProcessStarts(p)
    /\ UNCHANGED << M_vars, L_vars >>

\* A running process dies
\* Its pid is added to a set of dead pids in the Locker module, which
\* allows the ProcessWithTransactionIsDown action formula to be enabled
ProcessDies(p) ==
    /\ p \in P_LiveProcesses
    /\ LET process_pid == P_ProcessPid(p)
       IN   
            /\ P_ProcessDies(p)
            /\ L_ProcessDies(process_pid)
    /\ UNCHANGED << M_vars >>

\* A process that is running and doesn't currently have an open transaction
\* calls the locker to open a transaction and get a tid
\* The transaction is recorded in the locker and the tid is stored in the process
StartTransaction(p) ==
    \E l \in L_LeaderLockers :
        LET process_pid == P_ProcessPid(p)
            locker_pid == L_LockerPid(l)
            tid == L_LastTid(l) + 1
        IN 
            /\ L_StartTransaction(l, process_pid)
            /\ P_StartTransaction(locker_pid, tid, p)
            /\ UNCHANGED << M_vars >>
             
\* Given that the process is running and has an open transaction
\* and that the locker exists and the the lock is free, then acquire the lock
AcquireLockSucceeds(p, key) ==
    LET locker_pid == P_ProcessLockerPid(p)
    IN 
        /\ L_LockerExists(locker_pid)
        /\ p \in P_ProcessesWithOpenTransactions
        /\ LET process_tid == P_ProcessTid(p)
               new_lock_version == L_CurrentLockVersion(locker_pid, key) + 1
           IN
                /\ P_AcquireLockSucceeds(p, key, new_lock_version)
                /\ L_AcquireLockSucceeds(locker_pid, process_tid, key)
                /\ UNCHANGED << M_vars >>
    
\* A transaction is aborted by cleaning it up in the process and the locker
AbortTransaction(p, locker_pid, tid) ==
    /\ P_CleanUpTransaction(p)
    /\ L_CleanUpTransaction(locker_pid, tid)
    /\ UNCHANGED << M_vars >>

\* The process is running and has an open transaction
\* Depending on the type of failure, we either start a new transaction, retry or abort
AcquireLockFails(p, key) == 
    /\ p \in P_ProcessesWithOpenTransactions
    /\ LET process_tid == P_ProcessTid(p)
           process_locker_pid == P_ProcessLockerPid(p)
       IN
            \* If the locker no longer exists start a new transaction with the new locker
            \/ /\ ~L_LockerExists(process_locker_pid)
               /\ StartTransaction(p)
            \* If the transaction is not valid i.e. blacklisted, then start a new transaction
            \/ /\ ~L_HasValidTransaction(process_locker_pid, process_tid)
               /\ StartTransaction(p)
            \* If the transaction is valid but the key is already locked, 
            \* and if there are still retries left, get ready to retry the same transaction
            \/ /\ L_HasValidTransaction(process_locker_pid, process_tid)
               /\ ~L_LockIsFree(process_locker_pid, key)
               /\ P_HaveMoreRetries(p)
               /\ P_PrepareToRetrySameTransaction(p)
               /\ UNCHANGED << L_vars, M_vars >>
            \* If the transaction is valid but the key is already locked, 
            \* and if there are no more retries left, then abort the transaction
            \/ /\ L_HasValidTransaction(process_locker_pid, process_tid)
               /\ ~L_LockIsFree(process_locker_pid, key)
               /\ ~P_HaveMoreRetries(p)
               /\ AbortTransaction(p, process_locker_pid, process_tid)
               
               

AddWriteToTransaction(p, key) ==
    /\ P_AddWriteToTransaction(p, key)
    /\ UNCHANGED << M_vars, L_vars >>
    
\* The transaction is sent as a commit command and all transaction data is cleaned both
\* in the locker and the process
CommitTransaction(p) ==
    /\ M_LeaderExists
    /\ p \in P_ProcessesWithOpenTransactionsWithWrites
    /\ LET leader_node == M_LeaderNode
           term        == M_LeaderTerm
           next_index  == M_LeaderLastIndex + 1
           tran        == P_TransactionCommitValue(p)
       IN 
          /\ M_AppendCommandToLeader("commit", tran)
          /\ P_SetTransactionAsPendingConfirm(p, next_index, leader_node, term)
    /\ UNCHANGED << L_vars >>    

\* A commit is confirmed back to the process if:
\* - the process transaction status is pending_commit_result
\* - the transaction has been applied by the leader
\* - there has been no leader election since the commit was initiated
\* We add the writes in this transaction to the model so we can test the data loss invariant
CommitSuccessConfirmedToProcess(p) ==
    /\ M_LeaderExists
    /\ p \in P_ProcessesWithPendingCommitTransactions
    /\ LET transaction == P_ProcessTransaction(p)
           process_locker_pid == P_ProcessLockerPid(p)
       IN
            /\ M_LeaderMatches(transaction.leader_node, transaction.leader_term)
            /\ M_TransactionWasApplied(transaction.log_index, 
                                       process_locker_pid, 
                                       transaction.pid, 
                                       transaction.tid)
            /\ MODEL_AppendToWriteLogModel(transaction)
            /\ P_CleanUpTransaction(p)
            /\ L_CleanUpTransaction(process_locker_pid, transaction.tid)

\* A commit is aborted if the process transaction status is pending_commit_result
\* and either:
\* - there has been a leader election since the commit was initiated
\* - the leader discarded the commit
CommitFailureConfirmedToProcess(p) ==
    /\ p \in P_ProcessesWithPendingCommitTransactions
    /\ LET transaction == P_ProcessTransaction(p)
           process_locker_pid == P_ProcessLockerPid(p)
       IN
            /\ \/ ~M_LeaderMatches(transaction.leader_node, transaction.leader_term)
               \/ /\ M_LeaderMatches(transaction.leader_node, transaction.leader_term)
                  /\ M_TransactionWasDiscarded(transaction.log_index, 
                                               process_locker_pid, 
                                               transaction.pid, 
                                               transaction.tid)
            /\ P_CleanUpTransaction(p)
            /\ L_CleanUpTransaction(process_locker_pid, transaction.tid)
            /\ UNCHANGED << M_vars >>
    
    
\* A process performs an action        
\*ProcessAction ==
\*    \E p \in P : 
\*        \/ ProcessStarts(p)
\*        \/ ProcessDies(p)
\*        \/ FalseProcessDown(p)
\*        \/ StartTransaction(p)
\*        \/ CommitTransaction(p)
\*        \/ CommitConfirmedToProcess(p)
\*        \/ CommitFailed(p)
\*        \/ \E key \in K :
\*           \/ AcquireLockSucceeds(p, key)
\*           \/ AcquireLockFails(p, key)
\*           \/ AddWriteToTransaction(p, key)


Next ==
    \/ \E n \in N : \/ NodeGoesOnline(n)
                    \/ NodeGoesOffline(n) 
    \/ PerformLeaderElection
    \/ LockerUpSent
    \/ LeaderLockerDies
    \/ NonLeaderLockerDies
    \/ ProcessWithTransactionIsDown
    \/ FalseProcessDown
    \/ LeaderNotifiedOfPendingBlacklist
    \/ ReplicateCommand
    \/ ApplyCommand
    \/ \E p \in P : 
        \/ ProcessStarts(p)
        \/ ProcessDies(p)
        \/ StartTransaction(p)
        \/ CommitTransaction(p)
        \/ CommitSuccessConfirmedToProcess(p)
        \/ CommitFailureConfirmedToProcess(p)
        \/ \E key \in K :
           \/ AcquireLockSucceeds(p, key)
           \/ AcquireLockFails(p, key)
           \/ AddWriteToTransaction(p, key)
    
    
\*IsLocallyConsistent(n, key) ==
\*    /\ leader # 0
\*    /\ nodes[n].role # "offline"
\*    /\ nodes[n].kvs[key].version = nodes[leader].kvs[key].version


=============================================================================
\* Modification History
\* Last modified Tue Dec 17 16:08:07 CET 2019 by GUNMETAL
\* Last modified Sun Dec 08 09:38:46 CET 2019 by GUNMETAL
\* Last modified Thu Dec 05 10:01:12 PST 2019 by jack
\* Created Tue Nov 19 05:11:10 PST 2019 by jack
