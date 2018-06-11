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
    catch
        exit:{aborted, locked} ->
            io:format("Transaction locked. Retrying ~p times ~n", [Retries]),
            transaction(Fun, Args, Retries - 1, {aborted, locked});
        exit:{aborted, Reason} ->
            io:format("Transaction failed. Reason ~p Stacktrace ~p ~n", [Reason, erlang:get_stacktrace()]),
            rollback_transaction({aborted, Reason})
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

all_keys(ActivityId, Opaque, Tab, LockKind) ->
    Context = get_transaction_context(),
    AllKeys = execute_command(Context, all_keys, [Tab, LockKind]),
    case ramnesia_context:deletes_object(Context, Tab) of
        [] ->
            ramnesia_context:filter_all_keys_from_context(Context, Tab, AllKeys);
        Deletes ->
            DeletedKeys = lists:filtermap(fun({_, Rec, _}) ->
                Key = record_key(Rec),
                case read(ActivityId, Opaque, Tab, Key, LockKind) of
                    [] -> false;
                    _  -> {true, Key}
                end
            end,
            Deletes),
            ramnesia_context:filter_all_keys_from_context(Context, Tab, AllKeys -- DeletedKeys)
    end.

first(ActivityId, Opaque, Tab) ->
    Context = get_transaction_context(),
    Key = execute_command(Context, first, [Tab]),
    check_key(ActivityId, Opaque, Tab, Key, '$end_of_table', next, Context).

check_key(ActivityId, Opaque, Tab, Key, PrevKey, Direction, Context) ->
    NextFun = case Direction of
        next -> fun next/4;
        prev -> fun prev/4
    end,
    case {Key, key_inserted_between(Tab, PrevKey, Key, Direction, Context)} of
        {_, {ok, NewKey}}       -> NewKey;
        {'$end_of_table', none} -> '$end_of_table';
        {_, none} ->
            case ramnesia_context:key_deleted(Context, Tab, Key) of
                true ->
                    NextFun(ActivityId, Opaque, Tab, Key);
                false ->
                    case ramnesia_context:delete_object_for_key(Context, Tab, Key) of
                        [] -> Key;
                        _Recs ->
                            case read(ActivityId, Opaque, Tab, Key, read) of
                                [] -> NextFun(ActivityId, Opaque, Tab, Key);
                                _  -> Key
                            end
                    end
            end
    end.

-type table() :: atom().
-type context() :: ramnesia_context:context().
-type key() :: term().

-spec key_inserted_between(table(),
                           key() | '$end_of_table',
                           key() | '$end_of_table',
                           prev | next,
                           context()) -> {ok, key()} | none.
key_inserted_between(Tab, PrevKey, Key, Direction, Context) ->
    WriteKeys = lists:filter(fun(WKey) ->
        case Direction of
            next ->
                (PrevKey == '$end_of_table' orelse WKey > PrevKey)
                andalso
                (Key == '$end_of_table' orelse WKey =< Key);
            prev ->
                (PrevKey == '$end_of_table' orelse WKey < PrevKey)
                andalso
                (Key == '$end_of_table' orelse WKey >= Key)
        end
    end,
    [record_key(Rec) || {_, Rec, _} <- ramnesia_context:writes(Context, Tab)]),
    case WriteKeys of
        [] -> none;
        [NewKey | _] -> {ok, NewKey}
    end.

last(ActivityId, Opaque, Tab) ->
    Context = get_transaction_context(),
    Key = execute_command(Context, last, [Tab]),
    check_key(ActivityId, Opaque, Tab, Key, '$end_of_table', prev, Context).

prev(ActivityId, Opaque, Tab, Key) ->
    Context = get_transaction_context(),
    NewKey = execute_command(Context, prev, [Tab]),
    check_key(ActivityId, Opaque, Tab, NewKey, Key, prev, Context).

next(ActivityId, Opaque, Tab, Key) ->
    Context = get_transaction_context(),
    NewKey = execute_command(Context, next, [Tab]),
    check_key(ActivityId, Opaque, Tab, NewKey, Key, next, Context).

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
    RaCommand = {Command, ramnesia_context:transaction_id(Context), self(), Args},
    run_ra_command(RaCommand).

run_ra_command(Command) ->
    NodeId = ramnesia_node:node_id(),
    case ra:send_and_await_consensus(NodeId, Command) of
        {ok, {ok, Result}, _}    -> Result;
        {ok, {error, Reason}, _} -> mnesia:abort(Reason);
        {error, Reason}          -> mnesia:abort(Reason);
        {timeout, _}             -> mnesia:abort(timeout)
    end.

do_index_read(Context, Tab, SecondaryKey, Pos, LockKind) ->
    execute_command(Context, index_read, [Tab, SecondaryKey, Pos, LockKind]).

record_key(Record) ->
    element(2, Record).



