------------------------------ MODULE Locker ------------------------------
EXTENDS Integers, FiniteSets, TLC, Sequences, Machine
CONSTANTS L \* set of all locker processes
VARIABLES \* part of mnevis
          lockers,
          dead_proc_pids, \* used for simulating Erlang monitors
          \* meta
          next_locker_pid,
          locker_action

L_vars == << lockers, dead_proc_pids, next_locker_pid, locker_action >>
LVarsNoLockers == << dead_proc_pids, next_locker_pid >>

\*--------------------------------------------------          
\* Type definitions
\*--------------------------------------------------

\* Note that the locker pid also represents the term, as we are using monotonically increasing pids
BlacklistStatus == { "none", "pending_notify", "pending_result", "confirmed" }
LockerTran == [pid: Nat, tid: Nat, blacklist_status: BlacklistStatus]
Lock == [tid: Nat, version: Nat]
LockerStatus == { "stopped", "pending_notify", "candidate", "leader" }
Locker == [pid: Nat,
            node: NodeHost, 
            status: LockerStatus,
            last_tid: Nat,
            transactions: SUBSET LockerTran,
            locks: [K -> Lock]]
            
\*--------------------------------------------------          
\* Null/empty representations
\*--------------------------------------------------
NoLock == [tid |-> 0, version |-> 0]
NoLockerTran == [pid |-> 0, tid |-> 0, blacklist_status |-> "none"]
NoLocker == [pid |-> 0, 
            node |-> 0, 
            status |-> "stopped",
            last_tid |-> 0, 
            transactions |-> {},
            locks |-> [k \in K |-> NoLock]]

\*--------------------------------------------------
\* Helper state formulae
\*--------------------------------------------------

L_HasValidTransaction(locker_pid, tid) ==
    \E l \in L : 
        /\ lockers[l].pid = locker_pid
        /\ \E t \in lockers[l].transactions : 
            /\ t.tid = tid
            /\ t.blacklist_status = "none"

L_LockerExists(pid) ==
    \E l \in L : lockers[l].pid = pid
    
L_LeaderLockers ==
    { l \in L : lockers[l].status = "leader" }
    
L_LockerPid(l) ==
    lockers[l].pid

L_LockersWithDeadTransactions ==    
    { l \in L :
        \E t \in lockers[l].transactions : t.pid \in dead_proc_pids }

L_TransactionsOf(l) ==    
    lockers[l].transactions

L_DeadTransactionsOf(l) ==    
    { t \in lockers[l].transactions : t.pid \in dead_proc_pids }
    
L_LockersWithPendingBlacklistTrans ==   
    { l \in L :
        \E t \in lockers[l].transactions : t.blacklist_status = "pending_notify" }    
        
L_PendingBlacklistTransOf(l) ==   
    { t \in lockers[l].transactions : t.blacklist_status = "pending_notify" }        

\*--------------------------------------------------          
\* Invariants
\*--------------------------------------------------

\* Ensure that the variables hold expected types
L_TypeOK == 
    /\ lockers \in [L -> Locker]
    /\ next_locker_pid \in Nat

\* There can be only one leader locker
\* In fact this should not be required for consistency, but I want to know if this spec
\* can prove it can happen. To be removed in the future.    
L_OnlyOneLeaderLockerInvariant ==
    Cardinality(L_LeaderLockers) < 2
    
\* OK: There is no leader
\* OK: There is a leader and there is at least one started locker 
\* OK: There is a leader and there are no started lockers but there is a pending locker_down to process
NoLeaderLockersNoPendingLockerDown ==
    /\ \A l \in L : lockers[l].status = "stopped"
    /\ ~M_ExistsPendingLockerDownEntry
                    
L_LeaderLockerOrPendingLockerDownInvariant ==                    
    ~NoLeaderLockersNoPendingLockerDown

\*--------------------------------------------------          
\* Next state formulae
\*--------------------------------------------------

L_Init == 
    /\ lockers = [l \in L |-> NoLocker]
    /\ next_locker_pid = 1
    /\ dead_proc_pids = {}
    /\ locker_action = "init"
    
L_BecomeCandidate(l) ==
    /\ lockers[l].status = "pending_notify"
    /\ lockers' = [lockers EXCEPT ![l].status = "candidate"]
    /\ locker_action' = "LockerUp"
    /\ UNCHANGED << LVarsNoLockers >>
    
\* Part of the state change of leader election!
\* If there was a new leader, creates a new locker process 
\* If existing leader was reelected then no new locker process created 
\* Followers stop any locker processes they know of
L_SpawnLocker(n) ==
    /\ \/ /\ M_IsLeader(n)
          /\ lockers' = [l \in L |->
                            IF M_FollowerHasLocker(lockers[l].pid, n) THEN
                                NoLocker
                            ELSE
                                lockers[l]]
          /\ next_locker_pid' = next_locker_pid \* don't increment the next locker pid (no locker spawned)
       \/ /\ ~M_IsLeader(n)
          /\ \E locker \in L : 
             /\ lockers[locker].status = "stopped"
             /\ lockers' = [l \in L |->
                            IF locker = l THEN
                                [lockers[l] EXCEPT !.pid = next_locker_pid,
                                                   !.node = n,
                                                   !.status = "pending_notify"]
                            ELSE 
                            
                                IF M_FollowerHasLocker(lockers[l].pid, n) THEN
                                    NoLocker
                                ELSE
                                    lockers[l]]
             /\ next_locker_pid' = next_locker_pid + 1
    /\ locker_action' = "Locker spawned"  
    /\ UNCHANGED << dead_proc_pids >>
    
    
\* Kills a running locker that is not a leader (and therefore unmonitored)
L_NonLeaderLockerDies ==
    \E locker \in L : 
        /\ lockers[locker].status \notin { "stopped", "leader" }
        /\ lockers' = [l \in L |-> IF l = locker THEN NoLocker ELSE lockers[l]]
        /\ locker_action' = "NonLeaderLockerDown"
        /\ UNCHANGED << LVarsNoLockers >>
          
\* Kills a running leader locker
\* If there is a leader node with a monitor on the downed locker, 
\* then append a locker_down command to its log  
L_LeaderLockerDies(l) ==
    /\ lockers[l].status = "leader"
    /\ lockers' = [lk \in L |-> IF lk = l THEN NoLocker ELSE lockers[lk]]
    /\ locker_action' = "Leader locker died"
    /\ UNCHANGED << dead_proc_pids, next_locker_pid >> 


\* Nodes goes offline taking any lockers and processes hosted on it with it
\* If the leader locker survived, then clean up any transactions of any processes that died          
L_NodeGoesOffline(n, downed_proc_pids) ==
    LET dead_lockers == { l \in L : lockers[l].node = n }
    IN 
        /\ lockers' = [l \in L |-> IF l \in dead_lockers THEN 
                                            NoLocker 
                                        ELSE 
                                            lockers[l]]
        /\ dead_proc_pids' = dead_proc_pids \union downed_proc_pids
        /\ locker_action' = "NodeGoesOffline n=" \o ToString(n)
        /\ UNCHANGED << next_locker_pid >>

        
L_RejectNewLocker(pid) ==
    lockers' = [l \in L |->
                    IF lockers[l].pid = pid THEN
                        [lockers[l] EXCEPT !.status = "stopped"]
                    ELSE
                        lockers[l]]
    /\ locker_action' = "RejectNewLocker pid=" \o ToString(pid)
    /\ UNCHANGED << dead_proc_pids, next_locker_pid>>

L_ConfirmNewLocker(confirmed_pid, old_pid) ==
    /\ lockers' = [l \in L |->
                    IF lockers[l].pid = confirmed_pid THEN
                        [lockers[l] EXCEPT !.status = "leader"]
                    ELSE
                        IF lockers[l].pid = old_pid THEN
                            NoLocker
                        ELSE
                            lockers[l]]
    /\ locker_action' = "ConfirmNewLocker pid=" \o ToString(confirmed_pid)
    /\ UNCHANGED << dead_proc_pids, next_locker_pid>>

UpdateTransactions(transactions, tid, blacklist_status) ==
    { IF t.tid = tid THEN [t EXCEPT !.blacklist_status = blacklist_status] ELSE t : t \in transactions }

\* Changes the blacklist status of the transaction to confirmed
\* Releases all locks held my that transaction
L_ConfirmBlacklisting(locker_pid, tid) ==
    /\ lockers' = [l \in L |->
                IF lockers[l].pid = locker_pid THEN
                    [lockers[l] EXCEPT !.transactions = UpdateTransactions(@, tid, "confirmed"),
                                       !.locks = [k \in K |-> 
                                                    IF @[k].tid = tid THEN
                                                        [@[k] EXCEPT !.tid = 0]
                                                    ELSE @[k]]]
                ELSE
                    lockers[l]]
    /\ locker_action' = "ConfirmBlacklisting tid=" \o ToString(tid)
    /\ UNCHANGED << dead_proc_pids, next_locker_pid >>
    
    
\* A process that is running and doesn't currently have an open transaction
\* calls the locker to open a transaction and get a tid
L_LastTid(locker) ==
    lockers[locker].last_tid

L_StartTransaction(locker, process_pid) ==
     /\ lockers[locker].status = "leader"
     /\ lockers' = [l \in L |-> 
                    IF l = locker THEN
                        [lockers[l] EXCEPT !.last_tid = @ + 1,
                                           !.transactions = @ \union { [pid |-> process_pid, 
                                                                        tid |-> lockers[l].last_tid + 1,
                                                                        blacklist_status |-> "none"] }]
                    ELSE
                        lockers[l]]
     /\ locker_action' = "StartTransaction process pid=" \o ToString(process_pid)
     /\ UNCHANGED << dead_proc_pids, next_locker_pid >>
     
RemoveTransaction(transactions, tid) == 
    { t \in transactions : t.tid # tid }

\* A locker cleans up all transaction data of a given tid
L_CleanUpTransaction(locker_pid, tid) ==
    \E l \in L : 
        /\ lockers[l].pid = locker_pid
        /\ lockers' = [lockers EXCEPT ![l].transactions = RemoveTransaction(@, tid),
                                      ![l].locks = [k \in K |->
                                                        IF @[k].tid = tid THEN [tid |-> 0, version |-> @[k].version]
                                                        ELSE @[k]]]
        /\ locker_action' = "CleanUpTransaction tid=" \o ToString(tid)
    /\ UNCHANGED << dead_proc_pids, next_locker_pid >>                                                             
     
\* A running process dies
\* If it has an open transaction in a locker, the locker cleans it up
L_ProcessDies(pid) ==
    /\ dead_proc_pids' = dead_proc_pids \union { pid }
    /\ locker_action' = "ProcessDies pid=" \o ToString(pid)
    /\ UNCHANGED << lockers, next_locker_pid >>                                
                                
\* A locker blacklists the tid locally only as there is no current leader
L_BlacklistsLocally(locker_pid, tid) ==
    /\ \E l \in L : 
        /\ lockers[l].pid = locker_pid
        /\ lockers' = [lockers EXCEPT ![l].transactions = UpdateTransactions(@, tid, "pending_notify")]
        /\ locker_action' = "Locally blacklisted"
    /\ UNCHANGED << dead_proc_pids, next_locker_pid >>

               
\* A locker may have previously blacklisted a transaction, but there had been no leader
\* Now that there is a leader, the blacklist command is sent and the status is updated to pending_result          
L_LeaderNotifiedOfBlacklisting(l, transaction) ==
    /\ transaction.blacklist_status = "pending_notify"
    /\ lockers' = [lockers EXCEPT ![l] =
                        [lockers[l] EXCEPT !.transactions = UpdateTransactions(@, transaction.tid, "pending_result")]]
    /\ locker_action' = "Locker transaction updated to pending_result t=" \o ToString(transaction.tid)
    /\ UNCHANGED << dead_proc_pids, next_locker_pid >>                                
                                
                                
\* The key has no lock on this locker
L_LockIsFree(locker_pid, key) ==
    \E l \in L : 
        /\ lockers[l].pid = locker_pid 
        /\ lockers[l].locks[key].tid = 0

LockerOf(locker_pid) ==
    CHOOSE l \in L : lockers[l].pid = locker_pid
        
L_CurrentLockVersion(locker_pid, key) ==
    lockers[LockerOf(locker_pid)].locks[key].version                                      
                                
L_AcquireLockSucceeds(locker_pid, tid, key) ==
    /\ L_LockerExists(locker_pid)
    /\ L_HasValidTransaction(locker_pid, tid)
    /\ L_LockIsFree(locker_pid, key)
    /\ LET locker == LockerOf(locker_pid)
       IN lockers' = [l \in L |->
                            IF l = locker THEN
                                [lockers[l] EXCEPT !.locks = [@ EXCEPT ![key] = [tid |-> tid,
                                                                                 version |-> @.version + 1]]]
                            ELSE
                                lockers[l]]
    /\ locker_action' = "AcquireLockSucceeds tid=" \o ToString(tid) \o " k=" \o ToString(key)
    /\ UNCHANGED << dead_proc_pids, next_locker_pid >>                                
                                
                                
                                
=============================================================================