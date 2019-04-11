Statemachine:

The machine is implemented in the mnevis_machine module.

The goal of this statemachine is to coordinate mnesia transactions in raft nodes.
The statemachine persists all commits, which are successfull transactions.
Commits contain information about the current lock process, which is a part of
the persisted state.
Lock process needs to register using ra commands to become current.
Transactions can ask the machine about the current lock process.

State:
    locker_status :: up | down - if locker registers - set to `up`, if it fails - set to `down`
    locker :: {Term, Pid} - current term and pid of the locker. New lockers are started with a higher term

Start:
    create a mnesia table to store committed transactions

Commands:
    commit - writes commit data to the mnesia DB,
             records committed transaction to committed_transactions table
        pre_conditions:
            if transaction is already committed - noop
            if transaction locker is not current - reject
        effects:
            release_cursor if committed successfully
    down - locker exited - set locker_status to `down` and start a new locker
    create_table/delete-table - perform a mnesia operation. TODO!

Leader effects:
    stop the old locker (it can be stopped already) and start a new one
