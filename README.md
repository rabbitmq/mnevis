# Mnesia with RA consensus.

An attempt to implement a high-consistent mnesia database based on Raft protocol.

The goal of this project is to remove mnesia clustering and distribution layer,
because it's causing many issues in presence of network errors.

This project is created for RabbitMQ and will mostly target features required
by the RabbitMQ.

## Usage

You need to start a mnevis ra node and trigger election:

```
mnevis:start("my/ra/directory"),
mnevis_node:trigger_election().
```

This will start a single node cluster. Not so useful.

To start a multi-node configuration, you will have to provide `initital_nodes`
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
mnevis_node:start(),
mnevis_node:trigger_election().
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

%% Ranesia transaction
{atomic, ok} = mnevis:transaction(fun() ->
    ok = mnesia:write({foo, bar, baz}),
    [{foo, bar, baz}] = mnesia:read(foo, bar),
    ok = mnesia:write({foo, bar, bazz})
end).
```

Note that the transaction function code is exactly identical.
Ramnesia is designed to support (most of) mnesia transaction functions as is.

#### Key facts

- For writes, locks are coordinated wit Raft during a transaction
- Reads are always performed in a Raft cluster (with some optimisations) on a leader node.
- Writes are preformed on mnesia DB on commit.
- Commit runs a mnesia transaction, which can abort and it aborts a mnevis transaction.

### Transactoins on the caller side

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
Each write operation have to aquire a lock, which is done by a command
to the Raft cluster.

Reads get data from both context and the mnesia database. All reads from mnesia
database are commands to the Raft cluster.

Locked transactions can be restarted.

**Nested transactions will restart the parent. Retries number for nested transaction
is ignored.**

Commit and rollback are commands to the Raft cluster.

### Raft cluster and lock management

Ramnesia uses a single Ra node per erlang node with a constant nodeID:
`mnevis_node:node_id()`

The statemachine in the cluster mostly does lock management. Read and commit
operations also access mnesia database.

Most of commands return a result and are called with `ra:send_and_await_consensus`

The first command in a transaction is `start_transaction`, which will return a
transaction ID.

Each transaction command is addressed with a transaction ID and the current
process Pid. Processes runing a transaction are monitored and transaction will
be cancelled if its process stops.

Most of transaction commands, except commit and rollback, check and aquire locks
on some resources. Locks work mostly the same way as [in mnesia](http://erlang.org/doc/man/mnesia.html#lock-2)

If a transaction is locked, the locking transactions will be recorded in a machine
state and raft cluster will send a message once the locked transaction is unlocked
(when locking transactions commit or rollback). To avoid deadlocks only transactions
with a higher transaction ID are recorded as locking.

When transaction get a response from the machine that it's locked - it will wait
to be unlocked if there are locking transactions recorded, or restart instantly if
there are no locking trasactions (with higher transaction ID).

When a transaction is restarted, its context store is reset but its transaction ID
kept the same (no start_transaction command). This allows better lock management.

Commit and rollback commands can be repeated multiple times until the transaction
process receives reply from the raft machine. Machine state keeps track of committed
transactions to not apply the changes multiple times.

See more in [STATEMACHINE.md](./STATEMACHINE.md)

### Snapshotting and log replay

When a transaction is committed, the changes are saved on disk to mnesia DB.
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

`select` operations are not implementing, which means that `qlc` quieries will not work.

Table manipulation functions are not fully implemented and tables should be pre-created
on all nodes with all the indexes for mnevis to work.

Startup of ra currrently requires a global `data_dir` setting, which is not nice.

Startup configuration can be improved.

More docs?

There is a tutorial jepsen test which needs more scenarios https://github.com/hairyhum/jepsen.mnevis

There is a demo rabbitmq integration in the `mnevis-experimental` branch

More unit tests are needed for the context module.

More property tests are needed for multi-node setup.

Committed transactions table may be size optimised to have
"all committed before" value and remove old IDs.

