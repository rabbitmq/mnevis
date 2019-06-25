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
* commit - writes commit data to the mnesia DB, records committed transaction to committed_transactions table, updates table and record versions
  * pre-conditions:
    ```
    if transaction is already committed - noop
    if transaction locker is not current - reject
    if transaction is blacklisted - reject
    ```
  * effects:
    release_cursor if committed successfully
* down - locker exited - set locker_status to `down`
  * effects:
    mod_call to start a new locker
* locker_up - a new locker is trying to register
    ```
    if locker term is higher then the current
        set the new locker as current
        update local ETS cache
        reply with `confirm` and monitor the new locker process
    if locker term is lower or same but pid is different
        reply with `reject`
    ```
  * effects:
    if locker is registered
        monitor the new locker
        maybe mod_call to stop the previous if pid is different
* which_locker - get a current locker, this command may provides a transaction locker
    ```
    if there is no transaction locker
        reply with the current locker
    if the current term is higher
        reply with the current locker
    if the current locker is the same
        restart the current locker
    if the current term is lower
        reply with an error
    ```
  * pre-conditions:
    ```
    if the current locker status is down
        reply with an error
    ```

  * effects:
    new locker start if transaction provided same locker as the current
* blacklist - add transaction to the blacklist. blacklisted transactions cannot be committed. Locker blacklists transactions which exited before cleaning up.
  * pre-conditions:
    locker should be current
* create_table
* delete_table
* add_table_index
* del_table_index
* clear_table
* transform_table - table manipulation functions. Call mnesia functions.

Leader effects:
    stop the old locker (it can be stopped already) and start a new one
