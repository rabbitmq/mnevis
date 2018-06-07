-module(ramnesia).

-export([transaction/3]).
% -behaviour(mnesia_access).

%% mnesia_access behaviour
-export([
    lock/4,
    %% Write ops
    write/5,
    delete/5,
    delete_object/5,

    %% Read ops
    read/5,
    match_object/5,
    all_keys/4,
    first/3,
    last/3,
    index_match_object/6,
    index_read/6,
    table_info/4,
    prev/4,
    next/4
    ]).

-export([record_key/1]).

transaction(Fun, Args, Retries) ->
    transaction(Fun, Args, Retries, none).

transaction(_Fun, _Args, 0, Err) ->
    rollback_transaction(Err);
transaction(Fun, Args, Retries, _Err) ->
    try
        start_transaction_context(),
        mnesia:activity(ets, Fun, Args, ramnesia)
    of
        Res ->
            Context = get_transaction_context(),
            Writes = ramnesia_context:writes(Context),
            Deletes = ramnesia_context:deletes(Context),
            DeletesObject = ramnesia_context:deletes_object(Context),
            ok = execute_command(Context, commit, [Writes, Deletes, DeletesObject]),
            clean_transaction_context(),
            Res
    %% TODO: check for abort error
    catch error:Reason ->
        transaction(Fun, Args, Retries - 1, Reason)
    end.

rollback_transaction(Err) ->
    ok = execute_command(get_transaction_context(), rollback, []),
    Err.

start_transaction_context() ->
    Tid = run_ra_command({start_transaction, self()}),
    update_transaction_context(ramnesia_context:init(Tid)).

update_transaction_context(Context) ->
    put(ramnesia_transaction_context, Context).

clean_transaction_context() ->
    erase(ramnesia_transaction_context).

get_transaction_context() ->
    get(ramnesia_transaction_context).

lock(_ActivityId, _Opaque, LockItem, LockKind) ->
    execute_command(lock, [LockItem, LockKind]).

write(ActivityId, Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    Context1 = case mnesia:table_info(ActivityId, Opaque, Tab, type) of
        bag ->
            ramnesia_context:add_write_bag(Context, Tab, Rec, LockKind);
        Set when Set =:= set; Set =:= ordered_set ->
            ramnesia_context:add_write_set(Context, Tab, Rec, LockKind)
    end,
    update_transaction_context(Context1),
    execute_command(Context1, lock, [{Tab, record_key(Rec)}, LockKind]).

delete(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context = get_transaction_context(),
    Context1 = ramnesia_context:add_delete(Context, Tab, Key, LockKind),
    update_transaction_context(Context1),
    execute_command(Context1, lock, [{Tab, Key}, LockKind]).

delete_object(_ActivityId, _Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    Context1 = ramnesia_context:add_delete_object(Context, Tab, Rec, LockKind),
    update_transaction_context(Context1),
    execute_command(Context1, lock, [{Tab, record_key(Rec)}, LockKind]).

read(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context = get_transaction_context(),
    case ramnesia_context:read_from_context(Context, Tab, Key) of
        {written, set, Record} -> [Record];
        deleted -> [];
        {deleted_and_written, bag, Recs} -> Recs;
        _ ->
            RecList = execute_command(Context, read, [Tab, Key, LockKind]),
            ramnesia_context:filter_read_from_context(Context, Tab, Key, RecList)
    end.

match_object(_ActivityId, _Opaque, Tab, Pattern, LockKind) ->
    Context = get_transaction_context(),
    RecList = execute_command(Context, match_object, [Tab, Pattern, LockKind]),
    ramnesia_context:filter_match_from_context(Context, Tab, Pattern, RecList).

all_keys(_ActivityId, _Opaque, Tab, LockKind) ->
    execute_command(all_keys, [Tab, LockKind]).

%% TODO consider cache of written/deleted
first(_ActivityId, _Opaque, Tab) ->
    execute_command(first, [Tab]).

last(_ActivityId, _Opaque, Tab) ->
    execute_command(last, [Tab]).

prev(_ActivityId, _Opaque, Tab, Key) ->
    execute_command(prev, [Tab, Key]).

next(_ActivityId, _Opaque, Tab, Key) ->
    execute_command(next, [Tab, Key]).

index_match_object(_ActivityId, _Opaque, Tab, Pattern, Pos, LockKind) ->
    Context = get_transaction_context(),
    RecList = execute_command(Context, index_match_object, [Tab, Pattern, Pos, LockKind]),
    ramnesia_context:filter_match_from_context(Context, Tab, Pattern, RecList).

index_read(_ActivityId, _Opaque, Tab, SecondaryKey, Pos, LockKind) ->
    Context = get_transaction_context(),
    RecList = do_index_read(Context, Tab, SecondaryKey, Pos, LockKind),
    ramnesia_context:filter_index_from_context(Context, Tab, SecondaryKey, Pos, RecList).

table_info(ActivityId, Opaque, Tab, InfoItem) ->
    mnesia:table_info(ActivityId, Opaque, Tab, InfoItem).

execute_command(Command, Args) ->
    Context = get_transaction_context(),
    execute_command(Context, Command, Args).

execute_command(Context, Command, Args) ->
    Command = {Command, ramnesia_context:transaction_id(Context), self(), Args},
    run_ra_command(Command).

run_ra_command(Command) ->
    NodeId = ramnesia_node:node_id(),
    case ra:send_and_await_consensus(NodeId, Command) of
        {ok, {ok, Result}, _}    -> Result;
        %% TODO: some errors should restart, not abort.
        {ok, {error, Reason}, _} -> mnesia:abort(Reason);
        {error, Reason}          -> mnesia:abort(Reason);
        {timeout, _}             -> mnesia:abort(timeout)
    end.

do_index_read(Context, Tab, SecondaryKey, Pos, LockKind) ->
    execute_command(Context, index_read, [Tab, SecondaryKey, Pos, LockKind]).

record_key(Record) ->
    element(2, Record).



