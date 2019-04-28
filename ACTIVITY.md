Mnesia activity and custom transactions.

Dirty mnesia operations:

```
mnesia:dirty_write(...) # Send a change to every node in the table nodes
                        # On every node log the change and commit
                        # Notify subscriptions
```

Commit updates disk and ram copies. For ram copies it's similar to just updating
the ETS table.

Dirty operations support replication and load from remote node, although
if something fails it may fail on a single node and reads are not always
consistent with writes because they can happen on different nodes.

Mnesia transactions:

```
mnesia:transaction(   # Start a transaction
fun() ->              # Transaction function

    mnesia:read(...), # Transactional operations
    mnesia:write(...),
    ...
end).
```

Transactions manage locks and aborts.
The data written in a transaction will not be available for other transactions
before the transaction is committed.

The transactions can be aborted, discarding all the local transaction data.

The transactions can lock concurrent access to records and tables.
On locks transactions are restarted, so transaction function can run multiple times.

Errors in transaction will abort all the changes.

Mnesia transactions implementation:

Mnesia implements transactions as activities. Activity is a context in which
transaction function (or activity function) is called.

http://erlang.org/doc/man/mnesia.html#activity-4
http://erlang.org/doc/apps/mnesia/Mnesia_App_B.html

Activity is stared implicitly by `mnesia:transaction` or explicitly with `mnesia:activity`.

You can call `mnesia:transaction(Fun, Args)` as `mnesia:activity(transaction = AccessContext, Fun, Args, mnesia = AccessModule)`

To configure context for transaction function you can specify access context and
access module.
You can think of transactions as activity with access context `transaction`
and access module `mnesia`.

Access context can be `transaction` or `ets` or `sync_dirty` or `async_dirty`.

Access module is a module with callbacks for transactional functions in mnesia.
For `mnesia:read/1` called from a transaction, there will be `AccessMod:read/3`
called. Default module is `mnesia`, which handles default mnesia transactions.

```
mnesia:activity(transaction,
fun() ->
    mnesia:read(...),  # -----> AccessMod:read(...)
    mnesia:write(...), # -----> AccessMod:write(...)
    ...
end,
[],
AccessMod).
```

Custom access module:

Because all the transactional functions are forwarded to access module functions
you can modify them to do anything.

This allows us to implement custom transactions without actually modifying the
transaction functions in the client code.

Implementation:

On the higher level there is the `mnevis:transaction` function with the same
API as `mnesia:transaction`.

It creates a transaction context in the current process
(currently implemented with process dictionary).

Then it runs a mnesia activity with `ets` access context and the `mnevis` access module.

The access module implements callbacks for each transactional function, which
modify the local process transaction state and communicate with the raft cluster
to consistently read data.

Local transactional data contains writes and deletes performed in the transaction.

If activity succeeds, the commit command is sent to the raft cluster with
local writes and deletes.
Before the commit message local writes and deletes are not accessible from other
transactions.

Locks are coordinated with a separate lock process, which is managed by the raft
cluster.

If transaction is locked - the local data will be cleared and transaction
will be restarted.

If there is an abort, the activity will crash with `{aborted, Reason}` error and
transaction function returns `{aborted, Reason}`.

In many ways access module works the same way as mnesia access module, keeping
track of local writes and deletes.

It's also performs reads from the leader database for consistency reasons,
but it may be optimised in future.

If transaction is aborted, its locks are removed.





