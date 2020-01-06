------------------------------ MODULE Mnevis ------------------------------
EXTENDS Integers, FiniteSets, TLC, Sequences

CONSTANTS N, \* set of all nodes 
          K,  \* set of all keys
          L, \* set of all locker processes
          T, \* set of all transactions

          \* Various status values for machines, lockers, locks and transactions (originally were strings but changed to constants for model checking perf)
          Offline, Candidate, Follower, Leader,
          NotStarted, Started, Stopped, PendingNotifyUp,
          PendingBlacklistNotify, PendingBlacklistConfirm, Blacklisted, CleanedUp,
          LockNotGranted, LockGranted,
          PendingConsistentRead, PendingCommitResult, Committed, Aborted,
          \* Raft Commands (originally were strings but changed to constants for model checking perf)
          Nop, LockerUpCommand, LockerDownCommand, CommitCommand, BlacklistCommand,
          Up, Down

VARIABLES leader,
          machine,
          locker,
          locker_next_term,
          transaction,
          \* meta data used for debugging, state constraints or as a model for invariants to be checked against
          node_change_ctr,
          link,
          link_change_ctr, 
          ooo_writes, 
          write_log

\*--------------------------------------------------          
\* Type definitions
\*--------------------------------------------------
NodeOrNone == { 0 } \union N
TransactionOrNone == { 0 } \union T
LockerOrNone == { 0 } \union L

\* machines
MachineStatus == {Leader, Follower, Offline }

NoOpEntry == [index: Nat, command: Nop]
LockerUpEntry == [index: Nat, command: LockerUpCommand, from: L, locker: L]
LockerDownEntry == [index: Nat, command: LockerDownCommand, locker: L]
CommitEntry == [index: Nat, command: CommitCommand, from: T, transaction: T]
BlacklistEntry == [index: Nat, command: BlacklistCommand, from: L, transaction: T]
Entry == NoOpEntry \union LockerUpEntry \union LockerDownEntry \union CommitEntry \union BlacklistEntry

WriteLogValue == [transaction: T, value: Nat, log_index: Nat]
StoreValue == [value: Nat, version: Nat, log: SUBSET WriteLogValue]

Machine == [status: MachineStatus,
            curr_locker: LockerOrNone,
            monitored_locker: LockerOrNone,
            term: Nat,
            log: SUBSET Entry, 
            last_index: Nat, 
            committed_index: Nat, 
            applied_index: Nat,
            kvs: [K -> StoreValue],
            blacklist: SUBSET Nat,
            applied_trans: SUBSET T,
            discarded_trans: SUBSET T]

\* lockers
Lock == [transaction: TransactionOrNone, version: Nat]
Locker == [status: { NotStarted, PendingNotifyUp, Candidate, Leader, Stopped }, 
           node: NodeOrNone, 
           term: Nat, 
           locks: [K -> Lock],
           transactions: [T -> { NotStarted, Started, PendingBlacklistNotify, 
                                 PendingBlacklistConfirm, Blacklisted, CleanedUp }],
           down_notified: BOOLEAN]

\* process transactions
TranLock == [status: { LockNotGranted, LockGranted }, version: Nat] 
Transaction == [ node: NodeOrNone,
                 locker: L,
                 writes: [K -> Nat],
                 locks: [K -> TranLock],
                 status: { NotStarted, Started, PendingConsistentRead, PendingCommitResult, Committed, Aborted },
                 down_notified: BOOLEAN]
                

\* meta
LinkStatus == { Up, Down }
         
\*--------------------------------------------------          
\* Null/empty representations
\*--------------------------------------------------

NoStoreValue == [value |-> 0, version |-> 0, log |-> {}]
NoEntry == [index |-> 0, command |-> Nop]
NoCommitTran == [locker |-> 0, writes |-> [k \in K |-> 0]]
NoLock == [transaction |-> 0, version |-> 0]
NoTranLock == [status |-> LockNotGranted, version |-> 0]

\*--------------------------------------------------
\*--------------------------------------------------          
\* State constraints
\*--------------------------------------------------
\*--------------------------------------------------

TermLessThanEq(machine_term) == \A n \in N : machine[n].term <= machine_term
        
LastIndexLessThanEq(log_index) == \A n \in N : machine[n].last_index <= log_index

LockerTermLessThanEq(locker_term) == \A l \in L : locker[l].term <= locker_term

\*--------------------------------------------------
\*--------------------------------------------------          
\* Section 1 - Invariants
\*--------------------------------------------------
\*--------------------------------------------------

\* Ensure that the variables hold expected types
TypeOK == 
    /\ leader \in NodeOrNone
    /\ machine \in [N -> Machine] 
    /\ locker \in [L -> Locker] 
    /\ locker_next_term \in Nat   
    /\ transaction \in [T -> Transaction]
    /\ node_change_ctr \in Nat
    /\ link \in [N -> [N -> LinkStatus]]
    /\ link_change_ctr \in Nat
    /\ ooo_writes \in Nat
    /\ write_log \in [K -> SUBSET WriteLogValue]
    
NoOutOfOrderWritesInvariant ==
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
                /\ machine[n].applied_index >= write.log_index
                /\ ~\E sv \in machine[n].kvs[k].log : 
                    /\ write.transaction = sv.transaction
                    /\ write.value       = sv.value
                    /\ write.log_index   = sv.log_index

NoDataLossInvariant == ~DataLoss

\*--------------------------------------------------
\*--------------------------------------------------          
\* Section 2 - Init and Next state formulae
\*--------------------------------------------------
\*--------------------------------------------------

Init == 
    /\ leader = 0
    /\ machine = [n \in N |-> [status           |-> Candidate,
                               curr_locker      |-> 0,
                               monitored_locker |-> 0,
                               term             |-> 0,
                               log              |-> {},
                               last_index       |-> 0, 
                               committed_index  |-> 0, 
                               applied_index    |-> 0,
                               kvs              |-> [k \in K |-> NoStoreValue],
                               blacklist        |-> {},
                               applied_trans    |-> {},
                               discarded_trans  |-> {}]] 
    /\ locker = [l \in L |-> [status        |-> NotStarted, 
                              node          |-> 0,
                              term          |-> 0, 
                              locks         |-> [k \in K |-> NoLock], 
                              transactions  |-> [t \in T |-> NotStarted],
                              down_notified |-> FALSE]]
    /\ locker_next_term = 1
    /\ transaction = [t \in T |-> [node          |-> 0,
                                   locker        |-> 0,
                                   writes        |-> [k \in K |-> 0],
                                   locks         |-> [k \in K |-> NoTranLock],
                                   status        |-> NotStarted,
                                   down_notified |-> FALSE]]
    /\ link = [n \in N |-> [m \in N |-> Up]]
    /\ link_change_ctr = 0
    /\ node_change_ctr = 0
    /\ ooo_writes = 0
    /\ write_log = [k \in K |-> {}]
\*    /\ action = ""

\*--------------------------------------------------
\* Visibilities
\*--------------------------------------------------

ReachableNodes(n) ==
    { m \in N : link[n][m] = Up /\ machine[m].status # Offline }

SeesMajority(reachable_nodes) ==
    Cardinality(reachable_nodes) >= (Cardinality(N) \div 2) + 1

LinkOk(n, m) ==
    \/ n = m
    \/ /\ n # m
       /\ link[n][m] = Up
       
LinkDown(n, m) ==
    /\ n # m
    /\ link[n][m] = Down

LoseLink(n, m) ==
    /\ n # m
    /\ LinkOk(n, m)
    /\ link' = [nd \in N |->
                    CASE nd = n -> [link[n] EXCEPT ![m] = Down]
                    []   nd = m -> [link[m] EXCEPT ![n] = Down]
                    [] OTHER -> link[nd]]
    /\ link_change_ctr' = link_change_ctr + 1
    /\ UNCHANGED << leader, machine, locker, locker_next_term, transaction, node_change_ctr, 
                    ooo_writes, write_log >>
                    
RestoreLink(n, m) ==
    /\ n # m
    /\ LinkDown(n, m)
    /\ link' = [nd \in N |->
                    CASE nd = n -> [link[n] EXCEPT ![m] = Up]
                    []   nd = m -> [link[m] EXCEPT ![n] = Up]
                    [] OTHER -> link[nd]]
    /\ link_change_ctr' = link_change_ctr + 1
    /\ UNCHANGED << leader, machine, locker, locker_next_term, transaction, node_change_ctr, 
                    ooo_writes, write_log >>
                    
LoseAllLinks(n) ==
    /\ \A m \in N : LinkOk(n, m)
    /\ link' = [nd \in N |->
                    IF nd = n THEN [m \in N |-> Down]
                    ELSE [link[nd] EXCEPT ![n] = Down]]
    /\ link_change_ctr' = link_change_ctr + 1
    /\ UNCHANGED << leader, machine, locker, locker_next_term, transaction, node_change_ctr, 
                    ooo_writes, write_log >>
                    
RestoreAllLinks(n) ==
    /\ \E m \in N : LinkDown(n, m)
    /\ link' = [nd \in N |->
                    IF nd = n THEN [m \in N |-> Up]
                    ELSE [link[nd] EXCEPT ![n] = Up]]
    /\ link_change_ctr' = link_change_ctr + 1
    /\ UNCHANGED << leader, machine, locker, locker_next_term, transaction, node_change_ctr, 
                    ooo_writes, write_log >>                    

MachineVisibleFromNode(n, from_node) ==
    /\ machine[n].status # Offline
    /\ LinkOk(n, from_node)
    
LockerVisibleFromNode(l, from_node) ==
    /\ locker[l].status \notin { NotStarted, Stopped }
    /\ LinkOk(locker[l].node, from_node)
    
ProcessVisibleFromNode(t, from_node) ==
    /\ transaction[t].status \notin { NotStarted, Stopped }
    /\ LinkOk(transaction[t].node, from_node)
    
\* -------------------------------------------------------
\* Append entry to log
\* -------------------------------------------------------        
    
\* Create an entry according to the command
CreateEntry(command, from, data, index) ==
    CASE command = LockerUpCommand   -> [index |-> index, command |-> command, from |-> from, locker |-> data]
      [] command = LockerDownCommand -> [index |-> index, command |-> command, locker |-> data]
      [] command = CommitCommand      -> [index |-> index, command |-> command, from |-> from, transaction |-> data]
      [] command = BlacklistCommand   -> [index |-> index, command |-> command, from |-> from, transaction |-> data]
      [] OTHER -> NoOpEntry    

\* append a command to the leader's log
AppendCommandToLeader(command, from, data) ==
    /\ leader # 0
    /\ machine' = [machine EXCEPT ![leader] = 
                    [@ EXCEPT !.last_index = @ + 1,
                              !.log        = @ \union { 
                                                 CreateEntry(command,
                                                             from,
                                                             data, 
                                                             machine[leader].last_index + 1)
                                                        }]]  
    
\* -------------------------------------------------------
\* Monitor events
\* These are actions that are triggered by things like a node going down or a link going down
\* There can be a series of monitors that can fire and we process each of those before
\* other steps (in order to reduce the state space).
\* -------------------------------------------------------

PendingLockerDown(l) ==
    /\ locker[l].node # 0
    /\ locker[l].down_notified = FALSE
    /\ machine[locker[l].node].status = Leader
    /\ machine[locker[l].node].monitored_locker = l
    /\ ~LockerVisibleFromNode(l, locker[l].node)

MonitorNotifiesOfLockerDown ==
    \E l \in L :
        /\ PendingLockerDown(l)
        /\ AppendCommandToLeader(LockerDownCommand, "", l)
        /\ locker' = [locker EXCEPT ![l] = [@ EXCEPT !.down_notified = TRUE]]
        /\ UNCHANGED << leader, locker_next_term, transaction, node_change_ctr, 
                        link, link_change_ctr, ooo_writes, write_log >> 

PendingProcessDown(t) ==
    LET l == transaction[t].locker
    IN
        /\ l # 0
        /\ locker[l].status = Leader
        /\ ~ProcessVisibleFromNode(t, locker[l].node)
        /\ transaction[t].down_notified = FALSE 
        

MonitorNotifiesOfProcessDown ==
    \E t \in T :
        /\ PendingProcessDown(t)
        /\ locker' = [locker EXCEPT ![transaction[t].locker] =
                        [@ EXCEPT !.transactions = 
                            [@ EXCEPT ![t] = PendingBlacklistNotify]]] 
        /\ transaction' = [transaction EXCEPT ![t] = [@ EXCEPT !.down_notified = TRUE]]
        /\ UNCHANGED << leader, machine, locker_next_term, node_change_ctr, 
                        link, link_change_ctr, ooo_writes, write_log >>
                

PendingBlacklist(l) ==
    /\ locker[l].status = Leader
    /\ leader # 0
    /\ \E t \in T :
        /\ locker[l].transactions[t] = PendingBlacklistNotify

LockerBlacklistsTransaction ==
    \E l \in L :
        /\ locker[l].status = Leader
        /\ leader # 0
        /\ \E t \in T :
            /\ locker[l].transactions[t] = PendingBlacklistNotify
            /\ AppendCommandToLeader(BlacklistCommand, l, t)
            /\ locker' = [locker EXCEPT ![l] = 
                            [@ EXCEPT !.transactions =
                                [@ EXCEPT ![t] = PendingBlacklistConfirm]]]
            /\ UNCHANGED << leader, locker_next_term, transaction, node_change_ctr, 
                            link, link_change_ctr, ooo_writes, write_log >>


MonitorEventsToProcess ==
    \/ \E t \in T : PendingProcessDown(t)
    \/ \E l \in L : \/ PendingLockerDown(l)
                    \/ PendingBlacklist(l)

ProcessMonitorEvent ==
    IF \E t \in T : PendingProcessDown(t) THEN
        MonitorNotifiesOfProcessDown
    ELSE IF \E l \in L : PendingLockerDown(l) THEN
            MonitorNotifiesOfLockerDown
         ELSE IF \E l \in L : PendingBlacklist(l) THEN
                LockerBlacklistsTransaction
              ELSE
                FALSE
                
\* -------------------------------------------------------
\* Node Starts/Stops
\* -------------------------------------------------------

MachineDiesOnNode(n) ==
    /\ machine[n].status # Offline
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.status = Offline]]
    /\ \/ /\ leader  = n
          /\ leader' = 0
       \/ /\ leader  # n
          /\ leader' = leader
    /\ node_change_ctr' = node_change_ctr + 1

LockersDie(lockers) ==
    locker' = [l \in L |-> IF l \in lockers THEN [locker[l] EXCEPT !.status = Stopped] ELSE locker[l]]

LockersDieOnNode(n) ==
    LET dead_lockers == { l \in L : locker[l].node = n }
    IN LockersDie(dead_lockers)

TransactionsDieOnNode(n) ==
    transaction' = [t \in T |->
                        IF transaction[t].node = n THEN
                            [transaction[t] EXCEPT !.status = Aborted]
                        ELSE
                            transaction[t]]

NodeGoesOffline(n) ==
    /\ machine[n].status # Offline
    /\ MachineDiesOnNode(n)
    /\ LockersDieOnNode(n)
    /\ TransactionsDieOnNode(n)
    /\ UNCHANGED << locker_next_term, link, link_change_ctr, ooo_writes, write_log >>
    

MachineStarts(n) ==
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.status = Offline]]
    /\ node_change_ctr' = node_change_ctr + 1
    
NodeGoesOnline(n) ==
    /\ machine[n].status = Offline
    /\ MachineStarts(n)
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, link, link_change_ctr, ooo_writes, write_log >>  

\* -------------------------------------------------------
\* Leader election
\* -------------------------------------------------------

CanBeLeader(n, reachable_nodes) ==
    /\ SeesMajority(reachable_nodes)
    /\ \A node \in reachable_nodes : 
        /\ machine[n].last_index >= machine[node].last_index
        /\ machine[n].term       >= machine[node].term

MachineWinsLeaderElection(n, reachable_nodes) ==
    /\ leader # n
    /\ \/ leader = 0
       \/ /\ leader # 0
          /\ ~MachineVisibleFromNode(leader, n)
    /\ CanBeLeader(n, reachable_nodes)
    /\ machine' = [nd \in N |->
                    IF nd = n THEN
                        [machine[n] EXCEPT !.status = Leader,
                                           !.term   = @ + 1]
                    ELSE
                        IF nd \in reachable_nodes THEN
                            [machine[n] EXCEPT !.status          = Follower,
                                               !.term            = @ + 1,
                                               !.log             = IF machine[nd].last_index > machine[n].last_index 
                                                                       THEN machine[n].log 
                                                                       ELSE @,
                                               !.last_index      = IF @ > machine[n].last_index 
                                                                       THEN machine[n].last_index 
                                                                       ELSE @,
                                               !.committed_index = IF @ > machine[n].committed_index 
                                                                       THEN machine[n].committed_index 
                                                                       ELSE @]
                        ELSE [machine[n] EXCEPT !.status = Follower]]
    /\ leader' = n

LockerIsOnFollowerNode(l) ==
    /\ locker[l].node # 0
    /\ locker[l].node # leader
    /\ machine[locker[l].node].status # Offline

\* Followers stop any local lockers they know of
MachineFollowersStopLocalLockers ==
    LET lockers_to_stop == { l \in L : LockerIsOnFollowerNode(l) }
    IN 
        /\ LockersDie(lockers_to_stop)
        /\ UNCHANGED << locker_next_term >>

LockerStartsOthersStop(n, l) ==
    /\ locker[l].status = NotStarted
    /\ locker' = [lk \in L |-> IF lk = l THEN [locker[lk] EXCEPT !.status = PendingNotifyUp,
                                                                 !.node = n,
                                                                 !.term = locker_next_term]
                                ELSE IF LockerIsOnFollowerNode(lk) 
                                     THEN [locker[lk] EXCEPT !.status = Stopped] 
                                     ELSE locker[lk]]
    /\ locker_next_term' = locker_next_term + 1
    
    
\* Part of the state change of leader election!
LockerStarts(n) ==
          \* If existing leader was reelected then no new locker process created 
          \* Followers stop any locker processes they know of
    /\ \/ /\ leader = n
          /\ MachineFollowersStopLocalLockers
          \* If there was a new leader, creates a new locker process
          \* Followers stop any locker processes they know of
       \/ /\ leader # n
          /\ \E l \in L : LockerStartsOthersStop(n, l)
               

PerformLeaderElection ==
    \E n \in N :
        LET reachable_nodes == ReachableNodes(n)
        IN /\ machine[n].status # Offline
           /\ MachineWinsLeaderElection(n, reachable_nodes)
           /\ LockerStarts(n)
           /\ UNCHANGED << transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
        
\* -------------------------------------------------------
\* Locker actions
\* -------------------------------------------------------        

LockerBecomesCandidate(l) ==
    locker' = [locker EXCEPT ![l] = [@ EXCEPT !.status = Candidate]]

LockerSendsLockerUp ==
    /\ leader # 0
    /\ \E l \in L :
        /\ locker[l].status = PendingNotifyUp
        /\ MachineVisibleFromNode(leader, locker[l].node)
        /\ LockerBecomesCandidate(l)
        /\ AppendCommandToLeader(LockerUpCommand, l, l)
        /\ UNCHANGED << leader, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
        
\* A leader locker dies and if it is being monitored by the leader machine, 
\* a locker_down command is added to leader machine's log    
LockerDies ==
    \E l \in L :
        /\ locker[l].status \notin { NotStarted, Stopped }
        /\ LockersDie({l})
        /\ UNCHANGED << leader, machine, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
        


\*------------------------------------------------------------
\* TRANSACTIONS, WRITES and LOCKS
\*------------------------------------------------------------

\* A process that is running and doesn't currently have an open transaction
\* calls the locker to open a transaction and get a tid
\* The transaction is recorded in the locker and the tid is stored in the process
ProcessStoresTransactionState(l, t, n) ==
    transaction' = [transaction EXCEPT ![t] = 
                        [@ EXCEPT !.status = Started,
                                  !.locker = l,
                                  !.node = n]]  

LockerStoresTransactionState(l, t) ==
    locker' = [locker EXCEPT ![l] = 
                    [@ EXCEPT !.transactions = 
                        [@ EXCEPT ![t] = Started]]]


ProcessStartsTransaction(t) ==
    /\ transaction[t].status = NotStarted
    /\ leader # 0
    /\ LET l == machine[leader].monitored_locker
       IN
            /\ l # 0
            /\ locker[l].status = Leader
            /\ \E n \in N :
                    /\ MachineVisibleFromNode(leader, n)
                    /\ LockerVisibleFromNode(l, n)
                    /\ ProcessStoresTransactionState(l, t, n)
                    /\ LockerStoresTransactionState(l, t)
     /\ UNCHANGED << leader, machine, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    

\* The key is not locked on this locker
LockIsFree(l, key) ==
    locker[l].locks[key].transaction = 0

\* Return the current lock version of this key
CurrentLockVersion(l, key) ==
    locker[l].locks[key].version    
    
\* Grants the lock of the provided key to the provided tid                        
LockerGrantsLock(l, t, key) ==
    /\ locker[l].status = Leader \* transaction locker still leader
    /\ LockIsFree(l, key) \* the lock is free
    /\ locker[l].transactions[t] = Started \* the locker still sees this transaction
    /\ locker' = [locker EXCEPT ![l] = 
                    [@ EXCEPT !.locks = 
                        [@ EXCEPT ![key] = [transaction |-> t,
                                            version     |-> @.version + 1]]]]

ProcessLockGranted(t, key, lock_version) ==
    transaction' = [transaction EXCEPT ![t] = 
                        [@ EXCEPT !.locks = 
                            [@ EXCEPT ![key] = [status |-> LockGranted,
                                                version |-> lock_version]]]]
    
    
ProcessLockGrantedForConsistentReadSucceeds(t, key, lock_version) ==
    transaction' = [transaction EXCEPT ![t] = 
                        [@ EXCEPT !.status = PendingConsistentRead, 
                                  !.locks = [@ EXCEPT ![key] = [status |-> LockGranted,
                                                                version |-> lock_version]]]]
    
\* Given that the process is running and has an open transaction
\* and that the locker exists and the the lock is free, then acquire the lock
ProcessAcquiresLock(t, key) ==
    /\ transaction[t].status = Started
    /\ transaction[t].locks[key].status # LockGranted
    /\ LET l == transaction[t].locker
       IN
        /\ LockerVisibleFromNode(l, transaction[t].node)
        /\ LockerGrantsLock(l, t, key)
        /\ \/ ProcessLockGranted(t, key, CurrentLockVersion(l, key) + 1)
           \/ ProcessLockGrantedForConsistentReadSucceeds(t, key, CurrentLockVersion(l, key)  + 1)
    /\ UNCHANGED << leader, machine, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >> 
    
LockerAbortsTransaction(t, l) ==
    locker' = [locker EXCEPT ![l] = [@ EXCEPT !.transactions = [@ EXCEPT ![t] = Aborted],
                                              !.locks = [k \in K |-> 
                                                            IF @[k].transaction = t 
                                                            THEN [@[k] EXCEPT !.transaction = 0] 
                                                            ELSE @[k]]]]
    
ProcessAbortsTransaction(t) ==
    transaction' = [transaction EXCEPT ![t] = [@ EXCEPT !.status = Aborted]] 
    
TransactionLockerOk(l, t) ==
    \/ LockerVisibleFromNode(l, transaction[t].node)
    \/ locker[l].status # Leader
    \/ locker[l].transactions[t] # Started
    
    
\* The process is running and has an open transaction
\* Depending on the type of failure, we either start a new transaction, retry or abort
ProcessFailsToAcquireLock(t, key) == 
    /\ transaction[t].status = Started
    /\ transaction[t].locks[key].status # LockGranted
    /\ LET l == transaction[t].locker
       IN
            \/ /\ ~TransactionLockerOk(l, t)
               /\ ProcessAbortsTransaction(t)
               /\ UNCHANGED << leader, machine, locker, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
            \/ /\ TransactionLockerOk(l, t)
               /\ ~LockIsFree(l, key)
               /\ ProcessAbortsTransaction(t)
               /\ LockerAbortsTransaction(t, l)
               /\ UNCHANGED << leader, machine, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>

IsLocallyConsistent(n, key) ==
    /\ leader # 0
    /\ machine[n].status # Offline
    /\ machine[n].kvs[key].version = machine[leader].kvs[key].version
    
ReadLocalValue(n, key) == machine[n].kvs[key].value

WriteInteger(t, key) ==
    (locker[transaction[t].locker].term * 100000) + (transaction[t].locks[key].version * 100)    
            
\* Performs a consistent read, and adds the incremented value as a write to the transaction               
ProcessAddsIncrementWriteToTransaction(t, key) ==
    /\ transaction[t].status = PendingConsistentRead
    /\ transaction[t].locks[key].status = LockGranted
    /\ transaction[t].writes[key] = 0
    /\ IsLocallyConsistent(transaction[t].node, key)
    /\ LET new_value == ReadLocalValue(transaction[t].node, key) + 1
       IN transaction' = [transaction EXCEPT ![t] = 
                                [@ EXCEPT !.status = Started,
                                          !.writes = [@ EXCEPT ![key] = new_value]]]
    /\ UNCHANGED << leader, machine, locker, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>

\* Adds a write to the transaction
ProcessAddsWriteToTransaction(t, key) ==
    /\ transaction[t].status = PendingConsistentRead
    /\ transaction[t].locks[key].status = LockGranted
    /\ transaction[t].writes[key] = 0
    /\ LET new_value == WriteInteger(t, key)
       IN transaction' = [transaction EXCEPT ![t] = 
                                [@ EXCEPT !.status = Started,
                                          !.writes = [@ EXCEPT ![key] = new_value]]]
    /\ UNCHANGED << leader, machine, locker, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    
\* The transaction (including all its writes) is sent as a commit command 
\* and all transaction data is cleaned both in the locker and the process
ProcessCommitsTransaction(t) ==
    /\ leader # 0
    /\ transaction[t].status = Started
    /\ MachineVisibleFromNode(leader, transaction[t].node) 
    /\ AppendCommandToLeader(CommitCommand, t, t)
    /\ transaction' = [transaction EXCEPT ![t] = [@ EXCEPT !.status = PendingCommitResult]]
    /\ UNCHANGED << leader, locker, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>

\* A transaction in status pending_result timesout and aborts
\*ProcessTimeoutsTransactionCommit(t) ==
\*    /\ leader # 0
\*    /\ transaction[t].status = PendingCommitResult
\*    /\ LET l == transaction[t].locker
\*       IN
\*            \/ /\ ~TransactionLockerOk(l, t)
\*               /\ ProcessAbortsTransaction(t)
\*               /\ UNCHANGED << leader, machine, locker, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
\*            \/ /\ TransactionLockerOk(l, t)
\*               /\ ProcessAbortsTransaction(t)
\*               /\ LockerAbortsTransaction(t, l)
\*               /\ UNCHANGED << leader, machine, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>

\*--------------------------------------------------
\*  Command Replication
\*--------------------------------------------------

\* One big replication fest (to limit state explosion - we're not modelling Raft itself here)
\* When the leader sees a majority then all reachable nodes get all leader messages 
\* and committed indexes are all updated
MachineReplicatesCommand ==
    /\ leader # 0
    /\ LET reachable_nodes == ReachableNodes(leader)
       IN
        /\ SeesMajority(reachable_nodes)
        /\ machine' = [n \in N |->
                        IF n = leader THEN [machine[n] EXCEPT !.committed_index = machine[n].last_index]
                        ELSE IF n \in reachable_nodes THEN 
                                 [machine[n] EXCEPT !.last_index = machine[leader].last_index,
                                                    !.committed_index = machine[leader].last_index,
                                                    !.log = machine[leader].log]
                             ELSE machine[n]]
        /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>


\*--------------------------------------------------
\*  APPLY locker_up command
\*--------------------------------------------------

MachineConfirmsLocker(new_locker, old_locker) ==
    locker' = [l \in L |->
                IF l = new_locker THEN
                    [locker[l] EXCEPT !.status = Leader]
                ELSE
                    IF old_locker # 0 /\ l = old_locker THEN
                        [locker[l] EXCEPT !.status = Stopped]
                    ELSE
                        locker[l]]
                        
MachineRejectsLocker(rejected_locker) ==
    locker' = [l \in L |->
                IF l = rejected_locker THEN
                    [locker[l] EXCEPT !.status = Stopped]
                ELSE
                    locker[l]]                        

NewLockerHasHigherTerm(n, new_locker) ==
    \/ machine[n].curr_locker = 0
    \/ /\ machine[n].curr_locker # 0 
       /\ locker[machine[n].curr_locker].term < locker[new_locker].term

LeaderAppliesLockerUpAndConfirms(n, entry) ==
    /\ n = leader
    /\ NewLockerHasHigherTerm(n, entry.locker)
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.curr_locker = entry.locker,
                                                   !.monitored_locker = entry.locker,
                                                   !.applied_index = entry.index]]  
    /\ IF LockerVisibleFromNode(entry.locker, leader) THEN
          /\ MachineConfirmsLocker(entry.locker, machine[leader].curr_locker)
          /\ UNCHANGED << leader, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
       ELSE
          UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    
LeaderAppliesLockerUpAndRejects(n, entry) ==
    /\ n = leader
    /\ ~NewLockerHasHigherTerm(n, entry.locker)
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index]]  
    /\ IF LockerVisibleFromNode(entry.locker, leader) THEN
          /\ MachineRejectsLocker(entry.locker)
          /\ UNCHANGED << leader, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
       ELSE
          UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
   
FollowerAppliesLockerUp(n, entry) == 
    /\ machine[n].status = Follower
    /\ NewLockerHasHigherTerm(n, entry.locker)
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.curr_locker = entry.locker,
                                                   !.applied_index = entry.index]]  
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
          
FollowerDiscardsLockerUp(n, entry) == 
    /\ machine[n].status = Follower
    /\ ~NewLockerHasHigherTerm(n, entry.locker)
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>

ApplyLockerUpCommand(n, entry) ==
    /\ entry.command = LockerUpCommand
    /\ \/ LeaderAppliesLockerUpAndConfirms(n, entry)
       \/ LeaderAppliesLockerUpAndRejects(n, entry)
       \/ FollowerAppliesLockerUp(n, entry)
       \/ FollowerDiscardsLockerUp(n, entry)

\*--------------------------------------------------
\*  APPLY locker_down command
\*--------------------------------------------------

MachineStartsLocker(n) ==
    LET l == CHOOSE l \in L : locker[l].status = NotStarted
    IN 
        /\  locker' = [locker EXCEPT ![l] = 
                            [@ EXCEPT !.status = PendingNotifyUp,
                                      !.node = n,
                                      !.term = locker_next_term]]
        /\ locker_next_term' = locker_next_term + 1

LeaderAppliesLockerDownAndStartsNewLocker(n, entry) ==
    /\ n = leader
    /\ machine[n].curr_locker = entry.locker
    /\ \E l \in L : locker[l].status = NotStarted
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.curr_locker = 0,
                                                   !.applied_index = entry.index]]
    /\ MachineStartsLocker(n)
    /\ UNCHANGED << leader, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    
LeaderDiscardsLockerDown(n, entry) ==
    /\ n = leader
    /\ machine[n].curr_locker # entry.locker
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    
FollowerAppliesLockerDown(n, entry) ==
    /\ machine[n].status = Follower
    /\ machine[n].curr_locker = entry.locker
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.curr_locker = 0,
                                                   !.applied_index = entry.index]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    
FollowerDiscardsLockerDown(n, entry) ==
    /\ machine[n].status = Follower
    /\ machine[n].curr_locker # entry.locker
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    
ApplyLockerDownCommand(n, entry) ==
    /\ entry.command = LockerDownCommand
    /\ \/ LeaderAppliesLockerDownAndStartsNewLocker(n, entry)
       \/ LeaderDiscardsLockerDown(n, entry)
       \/ FollowerAppliesLockerDown(n, entry)
       \/ FollowerDiscardsLockerDown(n, entry)

\*--------------------------------------------------
\*  APPLY commit command
\*--------------------------------------------------

\* For every key in the kvs, if there is a corresponding write in the transaction
\* then set its value with the write and add it to the log of writes to that key
\* increment the applied index
ExecuteWrites(n, t, log_index) ==
    LET writes == transaction[t].writes
    IN
        /\ machine' = [machine EXCEPT ![n] = 
                        [@ EXCEPT !.applied_index = log_index,
                                  !.applied_trans = @ \union {t},
                                  !.kvs = [k \in K |->
                                            IF writes[k] > 0 THEN
                                                [@[k] EXCEPT !.value   = writes[k],
                                                             !.version = @ + 1,
                                                             !.log     = @ \union 
                                                                                {[transaction |-> t, 
                                                                                 value        |-> writes[k],
                                                                                 log_index    |-> log_index]}]                                                    
                                            ELSE
                                                @[k]]]] 
    
        /\ \/ \E k \in K :
                /\ writes[k] > 0
                /\ writes[k] < machine[n].kvs[k].value
                /\ ooo_writes' = ooo_writes + 1
           \/ ooo_writes' = ooo_writes
           
MODEL_AppendToWriteLogModel(n, t, entry) ==
    LET writes == transaction[t].writes
    IN
        write_log' = [k \in K |->
                        IF writes[k] > 0 THEN
                            write_log[k] \union {[transaction |-> t,
                                                  value       |-> writes[k],
                                                  log_index   |-> entry.index]}
                        ELSE
                            write_log[k]]           

\* To be able to commit a transaction it must have the current locker 
\* and must not be blacklisted
CanApplyCommit(n, entry) ==
    /\ machine[n].curr_locker = transaction[entry.transaction].locker
    /\ entry.transaction \notin machine[n].blacklist 

ProcessNotifiesLockerToCleanUp(l, t) ==
    locker' = [locker EXCEPT ![l] = 
                    [@ EXCEPT !.transactions = 
                            [@ EXCEPT ![t] = CleanedUp]]]
    
MachineNotifiesProcessOfResult(n, entry, transaction_status) ==
    LET t == entry.transaction
        l == transaction[entry.transaction].locker
    IN
        /\ transaction' = [transaction EXCEPT ![t] = [@ EXCEPT !.status = transaction_status]]
        /\ IF LockerVisibleFromNode(l, transaction[t].node) THEN
                ProcessNotifiesLockerToCleanUp(l, t)
           ELSE
                UNCHANGED << locker >>                

LeaderAppliesCommit(n, entry) ==
    /\ n = leader
    /\ CanApplyCommit(n, entry)
    /\ ExecuteWrites(n, entry.transaction, entry.index)
    /\ MODEL_AppendToWriteLogModel(n, entry.transaction, entry)
    /\ IF ProcessVisibleFromNode(entry.transaction, n) THEN
          /\ MachineNotifiesProcessOfResult(n, entry, Committed)
          /\ UNCHANGED << leader, locker_next_term, node_change_ctr, link, link_change_ctr >>
       ELSE
          UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr >> 

LeaderDiscardsCommit(n, entry) ==
    /\ n = leader
    /\ ~CanApplyCommit(n, entry)
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index,
                                                   !.discarded_trans = @ \union {entry.transaction}]]
    /\ IF ProcessVisibleFromNode(entry.transaction, n) THEN
          /\ MachineNotifiesProcessOfResult(n, entry, Aborted)
          /\ UNCHANGED << leader, locker_next_term, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
       ELSE
          UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >> 

FollowerAppliesCommit(n, entry) ==
    /\ machine[n].status = Follower
    /\ CanApplyCommit(n, entry)
    /\ ExecuteWrites(n, entry.transaction, entry.index)
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, write_log >>
    
FollowerDiscardsCommit(n, entry) ==
    /\ machine[n].status = Follower
    /\ ~CanApplyCommit(n, entry)
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index,
                                                   !.discarded_trans = @ \union {entry.transaction}]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>  
       
ApplyCommitCommand(n, entry) ==
    /\ entry.command = CommitCommand
    /\ \/ LeaderAppliesCommit(n, entry)
       \/ LeaderDiscardsCommit(n, entry)
       \/ FollowerAppliesCommit(n, entry)
       \/ FollowerDiscardsCommit(n, entry)
    
\*--------------------------------------------------
\*  APPLY blacklist
\*--------------------------------------------------

MachineConfirmsBlacklisting(l, t) ==
    locker' = [locker EXCEPT ![l] = [@ EXCEPT !.transactions = [@ EXCEPT ![t] = Blacklisted]]]    

LeaderAppliesBlacklist(n, entry) ==
    /\ n = leader
    /\ machine[n].curr_locker = entry.from
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index,
                                                   !.blacklist = @ \union { entry.transaction }]]
    /\ IF LockerVisibleFromNode(entry.from, n) THEN
          /\ MachineConfirmsBlacklisting(entry.from, entry.transaction)
          /\ UNCHANGED << leader, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
       ELSE
          UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>

LeaderDiscardsBlacklist(n, entry) ==
    /\ n = leader
    /\ machine[n].curr_locker # entry.from
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>
    
FollowerAppliesBlacklist(n, entry) ==
    /\ machine[n].status = Follower
    /\ machine[n].curr_locker = entry.from
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index,
                                                   !.blacklist = @ \union { entry.transaction }]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>    

FollowerDiscardsBlacklist(n, entry) ==
    /\ machine[n].status = Follower
    /\ machine[n].curr_locker # entry.from
    /\ machine' = [machine EXCEPT ![n] = [@ EXCEPT !.applied_index = entry.index]]
    /\ UNCHANGED << leader, locker, locker_next_term, transaction, node_change_ctr, link, link_change_ctr, ooo_writes, write_log >>    

\* Apply the blacklist command
ApplyBlacklistCommand(n, entry) ==
    /\ entry.command = BlacklistCommand
    /\ \/ LeaderAppliesBlacklist(n, entry)
       \/ LeaderDiscardsBlacklist(n, entry)
       \/ FollowerAppliesBlacklist(n, entry)
       \/ FollowerDiscardsBlacklist(n, entry)
    

\*--------------------------------------------------
\*  APPLY command
\*-------------------------------------------------- 

MachineAppliesCommand ==
    \E n \in N :
        /\ machine[n].status # Offline 
        /\ machine[n].applied_index < machine[n].committed_index
        /\ LET entry == CHOOSE e \in machine[n].log : e.index = machine[n].applied_index + 1
           IN \/ ApplyLockerUpCommand(n, entry)
              \/ ApplyLockerDownCommand(n, entry)
              \/ ApplyCommitCommand(n, entry)
              \/ ApplyBlacklistCommand(n, entry)

\*--------------------------------------------------
\*  Next
\*-------------------------------------------------- 

(*
Non-optimized version, good for when using action enabling profiling
*)
\*Next ==
\*    \/ ProcessMonitorEvent
\*    \/ \E n \in N : \/ NodeGoesOnline(n)
\*                    \/ NodeGoesOffline(n) 
\*    \/ \E n, m \in N : \/ LoseLink(n, m)
\*                       \/ RestoreLink(n, m)
\*    \/ PerformLeaderElection
\*    \/ LockerSendsLockerUp
\*    \/ LockerDies
\*\*        \/ FalselyNotifyLockerOfProcessDown
\*    \/ MachineReplicatesCommand
\*    \/ MachineAppliesCommand
\*    \/ \E t \in T : 
\*        \/ ProcessStartsTransaction(t)
\*        \/ ProcessCommitsTransaction(t)
\*\*        \/ ProcessTimeoutsTransactionCommit(t)
\*        \/ \E key \in K :
\*           \/ ProcessAcquiresLock(t, key)
\*           \/ ProcessFailsToAcquireLock(t, key)
\*           \/ ProcessAddsWriteToTransaction(t, key)
\*           \/ ProcessAddsIncrementWriteToTransaction(t, key)

(*
Optimized to reduce the state space by firing monitor events immediately
after the loss of a node or link
*)
Next ==
    IF MonitorEventsToProcess THEN
        ProcessMonitorEvent
    ELSE
        \/ \E n \in N : \/ NodeGoesOnline(n)
                        \/ NodeGoesOffline(n)
                        \/ LoseAllLinks(n)
                        \/ RestoreAllLinks(n) 
        \/ \E n, m \in N : \/ LoseLink(n, m)
                           \/ RestoreLink(n, m)
        
        \/ PerformLeaderElection
        \/ LockerSendsLockerUp
        \/ LockerDies
\*        \/ FalselyNotifyLockerOfProcessDown
        \/ MachineReplicatesCommand
        \/ MachineAppliesCommand
        \/ \E t \in T : 
            \/ ProcessStartsTransaction(t)
            \/ ProcessCommitsTransaction(t)
\*            \/ ProcessTimeoutsTransactionCommit(t)
            \/ \E key \in K :
               \/ ProcessAcquiresLock(t, key)
               \/ ProcessFailsToAcquireLock(t, key)
               \/ ProcessAddsWriteToTransaction(t, key)
               \/ ProcessAddsIncrementWriteToTransaction(t, key)
    
    

=============================================================================
\* Modification History
\* Last modified Fri Jan 03 19:01:29 CET 2020 by GUNMETAL
\* Last modified Sun Dec 08 09:38:46 CET 2019 by GUNMETAL
\* Last modified Thu Dec 05 10:01:12 PST 2019 by jack
\* Created Tue Nov 19 05:11:10 PST 2019 by jack
