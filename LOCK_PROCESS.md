# Transaction lock process.

Each mnevis cluster has an additional process to handle locks. This process
is managed by the raft cluster, but is not a part of it.

The lock process improves lock latency. Since transactions
are transient and can be restarted, locks should not be a part of the
persistent state. There is no point in submitting lock operations
to the raft log.

The lock process is a stateful erlang process, handling lock requests from
transaction functions.

There should be only one lock process per mnevis cluster at a time.
To achieve that the lock process has a reference provided by the raft cluster
and transaction commit commands contain this reference. If a commit command
reference is not the current reference for the raft cluster - it will be rejected
and transaction should be replayed.

The lock process cannot process lock requests before being made current by the
raft cluster.

So the lock process can start issuing locks only after being made current by
the raft cluster and commits with its locks will not be valid
after another process is made current.

This will prevent transactions which used different lock process to concurrently
commit.

Reference to a lock process is a tuple with this lock PID and a lock term integer,
increased by the raft cluster for each lock process started.

Lock process is monitored by the raft cluster and the new one is started when
the cluster receives down message. It's also created when a raft node is elected
a leader.

A lock process can be in two states: `candidate` and `leader`. Candidate cannot
process lock messages and will try to register with the raft cluster. When
registered successfully, it will have a term and promote itself to leader.
The raft cluster can refuse registering if the current term is higher then the
requester term. On refusal the lock process will stop.

When registering a lock process the raft cluster will save the current
lock process reference to the `lock_process_cache` ETS table. This table is used
by transaction processes to get the current lock process. If there is no current
lock process or it's not available, the transaction function can request it from
the raft cluster. NOTE: maybe it should use the AUX state to not pollute the log.

TODO: cleanup old lock processes if a new one is started. Lock process can monitor
the lock cache ets table to see if it's not current anymore. The raft cluster
can stop the process when rejecting a commit. The lock process can issue requests
to the raft cluster to check if it's still current.

TODO: when transaction process cannot locate a lock process and asks the
raft cluster - create the new lock process if it does not exist.


Starting a lock process:

When a node is promoted to leader:
    - start a new lock process
    - inform an old lock proccess to stop

On down signal from the monitor:
    - start a new lock process

When transaction process requests a lock process:
    - ???????????


Lock process lifetime:

- start as a candidate
- candidate
    - send a ra command to the cluster
        - if returned OK - promote to leader
        - if returned STOP - stop the process
    - on lock requests - ask to try again later
- leader
    - reply to lock requests
        - monitor current transactions
    - if received STOP from ra cluster - stop the process

Transaction process lock:

- get the current lock process from the ETS table
- try to send a lock request
    - if there is no lock process - check the ets table again
        - if the same process in the table - ask the raft cluster about the current process
    - if the lock process is a candidate - wait and retry from the start
- on the first successfull lock request - receive the transaction ID
    - transaction ID and lock process reference should be kept for
        duration of the transaction function
- commit the transaction with the transaction ID and the lock reference
    - if refused - retry the transaction with the lock reference returned in
        the refusal message








