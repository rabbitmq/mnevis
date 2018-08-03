Statemachine:

State:
    last_transaction_id - incrementing number to generate transaction IDs
    transactions - a map from pid to transaction ID number
    (read_/write_)locks - maps of lock item to transaction ID. Read lock can have multiple transaction IDs
    transaction_locks - simple directed graph, where verticies are transactions and edges are locks
    committed_transactions - map from transaction ID to pid to ignore commit messages

Start:
    create a mnesia table to store committed transactions

Commands:
    start_transaction - generates a new transaction ID and monitors the process
                        cleans up all the old transactions for the process
        effects:
            monitor pid
    rollback - cleans up transaction, removing locks and maybe unlocking locked transactions
        pre_conditions:
            if there is no such transaction - return error
        effects:
            demonitor pid
            maybe send message to waiting locked transactions to unlock them
    commit - writes commit data and transaction ID to the mnesia DB,
             cleans up transaction, unlocking locked transactions
             records committed transaction ID to committed_transactions
        pre_conditions:
            if transaction is already committed - noop
            if there is no such transaction - return error
        effects:
            release_cursor if committed successfully
            maybe send message to waiting locked transactions to unlock them
    finish - cleans up committed transaction flag to make commit idempotent
        effects:
            demonitor pid
    lock - check locks and maybe aquire a new lock. If locked - record locking
           transactions to the state and return the `locked` error, if no locking
           transactions (only higher transaction IDs can be locking) - return
           the `locked_instant` error.
        pre_conditions:
            if there is no such transaction - return error
    read/index_read/match_object/etc. - try to aquire a lock and read data from
                                        the mnesia DB. Can return the locked error
                                        the read data or aborted error.
        pre_conditions:
            if there is no such transaction - return error
    prev/next - same as read, but can return `key_not_found` error.
    down - a monitored process is stopped, remove all the trasaction data
           from the state, same as rollback, but does not have preconditions
    create_table/delete-table - perform a mnesia operation. TODO!

Leader effects:
    remonitor transaction processes.


Locks:
    Locks are similar to mnesia. Read locks can be aquired by multiple transactions,
    write locks - only one.
    Write locks are locked by both write and read, read locks are locked by write.
    If transaction is locked by other transactions, the higher transaction IDs are
    recorded as locking in the transaction locks graph.
    Locking relation is a directed edge in the graph from locking to locked.
    When transaction finishes, it removes itself from the graph, leaving some
    transactions without locking. Such transactions should receive an unlock
    message




