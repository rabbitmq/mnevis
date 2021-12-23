# Deprecation
This project has been discontinued and the work is now focused on [Khepri](https://github.com/rabbitmq/khepri).

# Mnesia with Raft Consensus

This is an attempt to implement a highly consistent Mnesia transaction implementation on top of the Raft consensus algorithm.

The goal of this project is to remove Mnesia's clustering and distribution layer due to it's
highly opinionated and flawed approach to recovery from network partitions.

This project is created for RabbitMQ and will mostly target features required
by the RabbitMQ.

## Usage

You need to start a mnevis ra node and trigger election:

```
mnevis:start("my/ra/directory").
```

This will start a single node cluster. Not so useful.

To start a multi-node configuration, you will have to provide `initial_nodes`
environment variable to the `mnevis` application:

```
[
{mnevis, [{initial_nodes, [node1, node2]}]}
].
```

If you use nodenames without domains, the `inet_db:gethostname()` is used.

If your application includes mnevis as a dependency, you will also have to
provide a ra directory, because it's used by ra on application start,
**this should be fixed in future ra versions**

```
[
{ra, data_dir, "my/ra/directory"},
{mnevis, [{initial_nodes, [node1, node2]}]}
].
```

In this case you can run
```
application:ensure_all_started(mnevis),
mnevis_node:start().
```

To start a node.

To make sure all nodes are started, you can run
```
{ok, _, _} = ra:members(mnevis_node:node_id()).
```

If everything is fine, you should be able to use mnevis transactions.

If you want to integrate with existing mnesia configuration, **you should configure
mnesia first**, and then start a mnevis node.

**Mnesia should not be clustered on the same nodes as mnevis
this behaviour is undefined.**

TODO: more info on configuration


You can run transactions the same way as in mnesia:

```
{atomic, ok} = mnevis:transaction(fun() -> mnesia:write({foo, bar, baz}) end).
```

## How does it work?

This project is trying to mimic the mnesia API as close as it makes sense.

For example, to run a transaction:

```
%% Mnesia transaction
{atomic, ok} = mnesia:transaction(fun() ->
    ok = mnesia:write({foo, bar, baz}),
    [{foo, bar, baz}] = mnesia:read(foo, bar),
    ok = mnesia:write({foo, bar, bazz})
end).

%% Mnevis transaction
{atomic, ok} = mnevis:transaction(fun() ->
    ok = mnesia:write({foo, bar, baz}),
    [{foo, bar, baz}] = mnesia:read(foo, bar),
    ok = mnesia:write({foo, bar, bazz})
end).
```

Note that the transaction function code is exactly identical.
Mnevis is designed to support (most of) mnesia transaction functions as is.

#### Key facts

- Locks are coordinated using a lock process during a transaction
- Reads are performed locally after aquiring a lock. More on read synchronisation below.
- Writes are preformed on mnesia DB on commit.
- Commits are coordinated with the Raft cluster
- Commit runs a mnesia transaction, which can abort and it aborts a mnevis transaction.

### Transactions on the caller side

This project uses [mnesia activity](http://erlang.org/doc/man/mnesia.html#activity-4)
feature to implement a custom transaction layer for mnesia.

Little bit more on activities [here](./ACTIVITY.md)

The `mnevis` module implements the `mnesia_activity` behaviour, providing
APIs for mnesia operations (like read/write/delete/delete_object etc.)

`write`, `delete` and `delete_object` operations are "write operations".
They change the world state and should be isolated in a transaction,
while read operations should only check the locks.

There is more on operation convergence in the [`mnevis_context.erl`](./src/mnevis_context.erl)

For this purpose (similarly to mnesia transactions) there is an internal context
storage, which stores all the write operations until transaction is committed
or rolled back.

Each operation have to acquire a lock, which is done by calling the special lock
process. More on locks [here](./LOCK_PROCESS.md)

Reads get data from both context and the mnesia database. Reads from the database
require the record to be synchronised with consistent changes. This is done with
versions: each table/record has a version assigned (multiple records per version),
which is read from the ra cluster using consistent_query.
If version read from the cluster is higher than the local version - the transaction
process will wait for them to synchronise before performing a local read.

Locked transactions can be restarted.

**Nested transactions will restart the parent. Retries number for nested transaction
is ignored.**

Commit is a comand to the Raft cluster.

### State machine

Mnevis uses a single Ra node per erlang node with a constant nodeID:
`mnevis_node:node_id()`

The statemachine in the cluster does read and commit operations,
which access the mnesia database.

Also statemachine keeps track of the current lock process, which is used to
coordinate transaction locks to avoid conflicts.

See more in [STATEMACHINE.md](./STATEMACHINE.md)

### Locks

Locks are coordinated with a lock process. There should be only one active lock
process per mnevis cluster. This is achieved using coordination with raft. Only
transactions using the current lock process can be committed.

Transactions locate the current lock process by reading the lock_cache ETS table or
asking the raft cluster.

Transactions can attempt to lock a lock_item with a lock_kind. Each lock request
should contain the transaction ID or `undefined`.
If transaction ID is `undefined` - a new transaction will be created
and all locks for the old transaction associated with the transaction process
will be cleared, if there were any.

Locks work mostly the same way as [in mnesia](http://erlang.org/doc/man/mnesia.html#lock-2).

If a transaction is blocked by another transaction it should be restarted.
To avoid livelocks transactions wait for locking transactions with lower ID to
finish first and then restart. If there are no such transactions - it's restarted
instantly.

When transaction is restarted - all its locks and transaction context is cleared
but transaction ID stays registered with the lock process.

When lock process changes - transaction IDs start from 1 and committed transaction
table is cleaned up.

**Behaviour differences with mnesia**
Because locks are acquired cluster-wide and not on specific nodes, global locks
acquired with a `{global, LockTerm :: term(), Nodes :: [node()]}` lock item will
not scope on nodes. It will lock all nodes on `LockTerm`.

### Consistent reads

When transaction reads data it should first lock the responsive record.
After record is locked no other transaction can change the data locked.

This means that local database needs to progress only to the lock point to have
most up-to-date data for the read. Also reads may be cached.

To wait for local database to progress to the lock point versions are used.
Version is a key-value pair. Version key is either a table or a tuple with table
and a hash of the record key. Hash is used to limit the version key scope.
Version value is a monotonically increasing integer.

Versions are updated on commits. A change in commit will update both the table
version and each record hash version.

If a local record version is up-to-date with the cluster - it has all the latest
changes for the record.

After aquiring a lock, reads will request a consistent view of the version
(via consistent query) and wait for the local version to become equal or greater
than the consistent view.
NOTE: multiple records may use the same version key, so it can be greater.
After waiting for version sync the transaction process will continue with reading
the local data.

**This approach relies on lock items to correspond with the record keys.**

### Snapshotting and log replay

When a transaction is committed, the changes are saved to mnesia DB.
When log is replayed after a snapshot it will re-run some commits, which might
have already succeed. To avoid that the committed transactions IDs are saved
in a mnesia table and all operations for this transaction will be skipped and
return an error. The error will not be reported to the client, because if it's
a replay the client will not be there anymore.

A snapshot is requested via release_cursor on every successful commit operation.

This app is using a custom snapshotting module `mnesia_snapshot`. When a state
is about to be saved as a snapshot it will take a mnesia checkpoint with
the `prepare` callback.
The snapshot consists of the mnesia backup and the server state saved by `ra_log_snapshot`

## TODO

Startup of ra currently requires a global `data_dir` setting, which is not nice.

Startup configuration can be improved.

More docs?

There is a tutorial jepsen test which needs more scenarios https://github.com/hairyhum/jepsen.mnevis

There is a demo rabbitmq integration in the `mnevis-experimental` branch

More unit tests are needed for the context module.

More property tests are needed for multi-node setup.
