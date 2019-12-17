------------------------------ MODULE Process ------------------------------
EXTENDS Integers, FiniteSets, TLC, Sequences, Locker, Machine
CONSTANTS P, \* set of all processes
          R  \* retry same transaction limit
VARIABLES \* part of mnevis
          processes,
          \* meta
          next_proc_pid,
          process_action
       
P_vars == << processes, next_proc_pid, process_action >>       
       
\*--------------------------------------------------          
\* Type definitions
\*--------------------------------------------------          
TranLockStatus == { "none", "granted" }
TranLock == [status: TranLockStatus, version: Nat] 
TranStatus == { "none", "started", "pending_commit_result" }
Transaction == [tid: Nat,
                pid: Nat,
                writes: [K -> Nat],
                locks: [K -> TranLock],
                status: TranStatus,
                retries: Nat,
                log_index: Nat,
                leader_node: NodeHost,
                leader_term: Nat]
                
                
ProcessStatus == { "stopped", "running" }

Process == [node: NodeHost, 
            pid: Nat,
            locker_pid: Nat,
            transaction: Transaction,
            status: ProcessStatus]
            
\*--------------------------------------------------          
\* Null/empty representations
\*--------------------------------------------------

NoTranLock == [status |-> "none", version |-> 0]

NoTransaction == [tid |-> 0,
                  pid |-> 0,
                  writes |-> [k \in K |-> 0],
                  locks |-> [k \in K |-> NoTranLock],
                  status |-> "none",
                  retries |-> 0,
                  log_index |-> 0,
                  leader_node |-> 0,
                  leader_term |-> 0]

NoProcess == [node |-> 0, 
              pid |-> 0,
              locker_pid |-> 0,
              transaction |-> NoTransaction,
              status |-> "stopped"]
              
\*--------------------------------------------------
\* Helper state formulae
\*--------------------------------------------------

P_TidsLessThanEq(tid) ==
    \A p \in P : processes[p].transaction.tid <= tid
    
P_LiveProcesses ==
    { p \in P : processes[p].status = "running" }
    
\* The process has performed local writes
HasWrite(p, key) ==
    processes[p].transaction.writes[key] > 0

HasWrites(p) ==
    \E key \in K : HasWrite(p, key)
    
P_ProcessesWithOpenTransactions ==
    { 
        p \in P : /\ processes[p].status = "running"
                  /\ processes[p].transaction.status = "started" 
    }

P_ProcessesWithOpenTransactionsWithWrites ==
    { 
        p \in P : /\ processes[p].status = "running"
                  /\ processes[p].transaction.status = "started" 
                  /\ HasWrites(p)
    }

P_ProcessesWithPendingCommitTransactions ==
    { 
        p \in P : /\ processes[p].status = "running"
                  /\ processes[p].transaction.status = "pending_commit_result" 
    }

    
P_ProcessPidsOnNode(n) ==
    LET pcs == { pc \in P : processes[pc].node = n}
    IN { processes[p].pid : p \in pcs }
    
P_ProcessPid(p) ==
    processes[p].pid    

P_ProcessTid(p) ==
    processes[p].transaction.tid    
    
P_ProcessLockerPid(p) ==
    processes[p].locker_pid
    
P_ProcessTransaction(p) ==
    processes[p].transaction        
    
P_HaveMoreRetries(p) ==
    processes[p].transaction.retries > 0
    
\* The process has a lock on this key
P_HasLock(p, key) ==
    processes[p].transaction.locks[key].status = "granted"        
    
\*--------------------------------------------------          
\* Invariants
\*--------------------------------------------------

\* Ensure that the variables hold expected types
P_TypeOK == 
    /\ processes \in [P -> Process]
    /\ next_proc_pid \in Nat
    

\*--------------------------------------------------          
\* Properties for debugging
\*--------------------------------------------------

\* False invariant - A lock can only be held one process at a time, per locker
\* In fact if a false process down event is received by a lock process
\* two processes can believe they have the same lock
\* The protection against this is blacklisting 
P_LockHeldByOneProcessPerLocker ==
    ~\E k \in K :
        /\ \E  p1, p2 \in P_LiveProcesses : 
            /\ p1 # p2
            /\ processes[p1].locker_pid = processes[p2].locker_pid
            /\ processes[p1].transaction.status \in {"started", "pending_commit_result" }
            /\ processes[p2].transaction.status \in {"started", "pending_commit_result" }
            /\ processes[p1].transaction.locks[k].status = "granted"
            /\ processes[p2].transaction.locks[k].status = "granted"
            
\* False invariant - this can happen - used for debugging            
P_LockOnlyHeldByOneProcessGlobally ==
    ~\E k \in K :
        /\ \E  p1, p2 \in P_LiveProcesses : 
            /\ p1 # p2
            /\ processes[p1].transaction.status = "started"
            /\ processes[p2].transaction.status = "started"
            /\ processes[p1].transaction.locks[k].status = "granted"
            /\ processes[p2].transaction.locks[k].status = "granted"

\* False invariant - this can happen - used for debugging            
P_NoTwoProcessesWithDifferentLockerPids ==
    ~\E  p1, p2 \in P_LiveProcesses : 
        /\ processes[p1].locker_pid > 0
        /\ processes[p2].locker_pid > 0
        /\ processes[p1].locker_pid # processes[p2].locker_pid
        
        
\*--------------------------------------------------          
\* Next state formulae
\*--------------------------------------------------

P_Init == 
    /\ processes = [p \in P |-> NoProcess]
    /\ next_proc_pid = 1
    /\ process_action = "init"
            
\* Nodes goes offline taking any processes hosted on it with it         
P_NodeGoesOffline(n) ==
    /\ processes' = [p \in P |-> IF processes[p].node = n THEN NoProcess ELSE processes[p]]
    /\ process_action' = "NodeGoesOffline n=" \o ToString(n)
    /\ UNCHANGED << next_proc_pid >>        
        
\* A process that is running and doesn't currently have an open transaction
\* calls the locker to open a transaction and get a tid
P_StartTransaction(locker_pid, tid, p) ==
    /\ processes[p].status = "running"
    /\ processes[p].transaction.status = "none"
    /\ processes' = [processes EXCEPT ![p] = [processes[p] EXCEPT !.transaction.status = "started",
                                                                  !.transaction.tid = tid,
                                                                  !.transaction.pid = processes[p].pid,
                                                                  !.transaction.retries = R,
                                                                  !.locker_pid = locker_pid]]
        
    /\ process_action' = "StartTransaction (lock failed?) p=" \o ToString(p)
    /\ UNCHANGED << next_proc_pid >>
        
\* A process cleans up its local transaction data
P_CleanUpTransaction(p) ==
    /\ processes' = [processes EXCEPT ![p] = 
                        [processes[p] EXCEPT !.transaction = NoTransaction]]
    /\ process_action' = "CleanUpTransaction p=" \o ToString(p)
    /\ UNCHANGED << next_proc_pid >>                            
        
        
P_SetTransactionAsPendingConfirm(p, log_index, leader_node, leader_term) ==
    /\ processes' = [processes EXCEPT ![p] = 
                       [processes[p] EXCEPT !.transaction = 
                            [processes[p].transaction EXCEPT !.status = "pending_commit_result",
                                                             !.log_index = log_index,
                                                             !.leader_node = leader_node,
                                                             !.leader_term = leader_term]]]
    /\ process_action' = "SetTransactionAsPendingConfirm p=" \o ToString(p)                                                          
    /\ UNCHANGED << next_proc_pid >>        
        
        
\* A process starts on a live node
P_ProcessStarts(p) ==
    \E n \in M_LiveNodes : 
        /\ processes[p].status = "stopped"
        /\ processes' = [processes EXCEPT ![p].status = "running",
                                          ![p].pid = next_proc_pid,
                                          ![p].node = n]
        /\ next_proc_pid' = next_proc_pid + 1
        /\ process_action' = "ProcessStarts p=" \o ToString(p)
   
\* A running process dies
\* If it has an open transaction in a locker, the locker cleans it up
P_ProcessDies(p) ==
    /\ processes[p].status = "running"
    /\ processes' = [processes EXCEPT ![p] = NoProcess]
    /\ process_action' = "ProcessDies p=" \o ToString(p)
    /\ UNCHANGED << next_proc_pid >>

P_AcquireLockSucceeds(p, key, lock_version) ==
    /\ processes[p].status = "running"
    /\ processes[p].transaction.status = "started"
    /\ processes' = [processes EXCEPT ![p] = 
                              [processes[p] EXCEPT !.transaction.locks = 
                                                 [@ EXCEPT ![key] = [status |-> "granted",
                                                                     version |-> lock_version]]]]
    /\ process_action' = "AcquireLockSucceeds p=" \o ToString(p) \o " k=" \o ToString(key)
    /\ UNCHANGED << next_proc_pid >>

\* Decrement the retries counter, remove all locks and writes, ready to run the same transaction again
P_PrepareToRetrySameTransaction(p) ==
    /\ processes[p].transaction.retries > 0
    /\ processes' = [processes EXCEPT ![p] = 
                        [processes[p] EXCEPT !.transaction = 
                            [@ EXCEPT !.writes = [k \in K |-> 0],
                                      !.locks = [k \in K |-> NoTranLock],
                                      !.retries = @ - 1]]]
    /\ process_action' = "Lock failed. PrepareToRetrySameTransaction p=" \o ToString(p)
    /\ UNCHANGED << next_proc_pid >>

\* Because the locker_pid is also the term, we can create a number
\* that should always increase. This is not mnevis but a strategy
\* for an invariant that says that all writes of a given key must be larger than existing value
WriteInteger(p, key) ==
    (processes[p].locker_pid * 10000) + processes[p].transaction.locks[key].version
 

\* Given the process is running, has an open transaction and holds a lock on this key
\* Then perform a write
\* In this model, we write the lock version as we can use that as an invariant:
\*  - we should never perform a write that is lower than the existing value       
P_AddWriteToTransaction(p, key) ==
    /\ processes[p].status = "running"
    /\ processes[p].transaction.status = "started"
    /\ P_HasLock(p, key)
    /\ ~HasWrite(p, key)
    /\ processes' = [processes EXCEPT ![p] = 
                        [processes[p] EXCEPT !.transaction = 
                            [processes[p].transaction EXCEPT !.writes = 
                                [processes[p].transaction.writes EXCEPT ![key] = WriteInteger(p, key)]]]]
    /\ process_action' = "PerformWrite p=" \o ToString(p) \o " k=" \o ToString(key)
    /\ UNCHANGED << next_proc_pid >>

P_TransactionCommitValue(p) ==
    [tid |-> processes[p].transaction.tid,
     locker_pid |-> processes[p].locker_pid,
     writes |-> processes[p].transaction.writes]


          
=============================================================================