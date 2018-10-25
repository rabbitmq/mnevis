-module(mnevis).

-export([start/1]).

%% TODO: support table manipulation.
-export([create_table/2, delete_table/1]).

%% System info
-export([db_nodes/0, running_db_nodes/0]).

-export([transaction/3, transaction/1]).
-export([is_transaction/0]).

-compile(nowarn_deprecated_function).

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
    next/4,
    foldl/6,
    foldr/6,

    %% QLC
    select/5,
    select/6,
    select_cont/3,

    clear_table/4
    ]).

-type table() :: atom().
-type context() :: mnevis_context:context().
-type key() :: term().

-export([record_key/1]).

%% TODO: configure ra data dir for already started ra.
start(DataDir) ->
    _ = application:load(ra),
    application:set_env(ra, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(mnevis),
    mnevis_node:start().

-spec db_nodes() -> [node()].
db_nodes() ->
    {ok, Nodes, _L} = ra:members(mnevis_node:node_id()),
    [Node || {_, Node} <- Nodes].

-spec running_db_nodes() -> [node()].
running_db_nodes() ->
    {ok, Nodes, _L} = ra:members(mnevis_node:node_id()),
    [Node || {Name, Node} <- Nodes,
             pong == net_adm:ping(Node)
             andalso
             undefined =/= rpc:call(Node, erlang, whereis, Name)].

create_table(Tab, Opts) ->
    %% TODO: handle errors/retry
    {ok, R} = run_ra_command({create_table, Tab, Opts}),
    R.

delete_table(Tab) ->
    %% TODO: handle errors/retry
    {ok, R} = run_ra_command({delete_table, Tab}),
    R.

transaction(Fun) ->
    transaction(Fun, [], infinity).

transaction(Fun, Args, Retries) ->
    transaction(Fun, Args, Retries, none).

transaction(Fun, Args, Retries, Err) ->
    case is_transaction() of
        true ->
            {atomic, mnesia:activity(ets, Fun, Args, mnevis)};
        false ->
            transaction0(Fun, Args, Retries, Err)
    end.

%% Transaction implementation.
%% Can be called in recusively for retries
%% Handles aborted errors
transaction0(_Fun, _Args, 0, Err) ->
    ok = maybe_rollback_transaction(),
    clean_transaction_context(),
    {aborted, Err};
transaction0(Fun, Args, Retries, _Err) ->
    try
        cleanup_all_unlock_messages(),
        case is_retry() of
            %% We do not notify the machine about retry because retrying
            %% transaction state in the machine should not be different from
            %% the new transaction, except transaction_locks.
            %% Worst case - some stray unlock messages in the message box,
            %% which are cleaned up on transaction cleanup.
            true  -> ok;
            false -> start_transaction()
        end,
        Res = mnesia:activity(ets, Fun, Args, mnevis),
        ok = commit_transaction(),
        {atomic, Res}
    catch
        exit:{aborted, locked_instant} ->
            retry_locked_transaction(Fun, Args, Retries);
        exit:{aborted, locked} ->
            %% Thansaction is still there, but it's locks were cleared.
            %% Wait for unlocked message meaning that locking transactions finished
            Context = get_transaction_context(),
            Tid = mnevis_context:transaction_id(Context),
            wait_for_unlock(Tid),
            retry_locked_transaction(Fun, Args, Retries);
        exit:{aborted, Reason} ->
            ok = maybe_rollback_transaction(),
            {aborted, Reason};
        _:Reason ->
            Trace = erlang:get_stacktrace(),
            error_logger:warning_msg("Ramnesia transaction error ~p Stacktrace ~p~n", [Reason, Trace]),
            ok = maybe_rollback_transaction(),
            {aborted, {Reason, Trace}}
    after
        clean_transaction_context()
    end.

wait_for_unlock(Tid) ->
    receive {ra_event, _From, {machine, {mnevis_unlock, Tid}}} -> ok
    after 5000 -> ok
    end.

retry_locked_transaction(Fun, Args, Retries) ->
    NextRetries = case Retries of
        infinity -> infinity;
        R when is_integer(R) -> R - 1
    end,
    %% Reset transaction context
    Context = get_transaction_context(),
    Tid = mnevis_context:transaction_id(Context),
    Context1 = mnevis_context:init(Tid),
    update_transaction_context(mnevis_context:set_retry(Context1)),
    transaction0(Fun, Args, NextRetries, locked).

is_retry() ->
    case get_transaction_context() of
        undefined -> false;
        Context   -> mnevis_context:is_retry(Context)
    end.

is_transaction() ->
    case get_transaction_context() of
        undefined -> false;
        _ -> true
    end.

commit_transaction() ->
    Context = get_transaction_context(),
    Writes = mnevis_context:writes(Context),
    Deletes = mnevis_context:deletes(Context),
    DeletesObject = mnevis_context:deletes_object(Context),

    Context = get_transaction_context(),
    Tid = mnevis_context:transaction_id(Context),
    {ok, Leader} = execute_command_with_retry(Context, commit,
                                              [Writes, Deletes, DeletesObject]),
    %% Notify the machine to cleanup the commited transaction record
    ra:pipeline_command(Leader, {finish, Tid, self()}),
    ok.

maybe_rollback_transaction() ->
    case is_transaction() of
        true  -> rollback_transaction();
        false -> ok
    end.

rollback_transaction() ->
    {ok, _} = execute_command_with_retry(get_transaction_context(), rollback, []),
    ok.

start_transaction() ->
    {ok, Tid} = run_ra_command({start_transaction, self()}),
    update_transaction_context(mnevis_context:init(Tid)).

update_transaction_context(Context) ->
    put(mnevis_transaction_context, Context).

clean_transaction_context() ->
    cleanup_all_unlock_messages(),
    erase(mnevis_transaction_context).

get_transaction_context() ->
    get(mnevis_transaction_context).

%% Mnesia activity API

lock(_ActivityId, _Opaque, LockItem, LockKind) ->
    execute_command_ok(lock, [LockItem, LockKind]).

write(ActivityId, Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    Context1 = case table_info(ActivityId, Opaque, Tab, type) of
        bag ->
            mnevis_context:add_write_bag(Context, Tab, Rec, LockKind);
        Set when Set =:= set; Set =:= ordered_set ->
            mnevis_context:add_write_set(Context, Tab, Rec, LockKind)
    end,
    update_transaction_context(Context1),
    execute_command_ok(Context1, lock, [{Tab, record_key(Rec)}, LockKind]).

delete(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context = get_transaction_context(),
    Context1 = mnevis_context:add_delete(Context, Tab, Key, LockKind),
    update_transaction_context(Context1),
    execute_command_ok(Context1, lock, [{Tab, Key}, LockKind]).

delete_object(_ActivityId, _Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    Context1 = mnevis_context:add_delete_object(Context, Tab, Rec, LockKind),
    update_transaction_context(Context1),
    execute_command_ok(Context1, lock, [{Tab, record_key(Rec)}, LockKind]).

read(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context = get_transaction_context(),
    case mnevis_context:read_from_context(Context, Tab, Key) of
        {written, set, Record} ->
            mnevis_context:filter_read_from_context(Context, Tab, Key, [Record]);
        deleted -> [];
        {deleted_and_written, bag, Recs} -> Recs;
        _ ->
            RecList = execute_command_ok(Context, read, [Tab, Key, LockKind]),
            mnevis_context:filter_read_from_context(Context, Tab, Key, RecList)
    end.

match_object(_ActivityId, _Opaque, Tab, Pattern, LockKind) ->
    Context = get_transaction_context(),
    RecList = execute_command_ok(Context, match_object, [Tab, Pattern, LockKind]),
    mnevis_context:filter_match_from_context(Context, Tab, Pattern, RecList).

all_keys(ActivityId, Opaque, Tab, LockKind) ->
    Context = get_transaction_context(),
    AllKeys = execute_command_ok(Context, all_keys, [Tab, LockKind]),
    case mnevis_context:deletes_object(Context, Tab) of
        [] ->
            mnevis_context:filter_all_keys_from_context(Context, Tab, AllKeys);
        Deletes ->
            DeletedKeys = lists:filtermap(fun({_, Rec, _}) ->
                Key = record_key(Rec),
                case read(ActivityId, Opaque, Tab, Key, LockKind) of
                    [] -> {true, Key};
                    _  -> false
                end
            end,
            Deletes),
            mnevis_context:filter_all_keys_from_context(Context, Tab, AllKeys -- DeletedKeys)
    end.

first(ActivityId, Opaque, Tab) ->
    Context = get_transaction_context(),
    Key = execute_command_ok(Context, first, [Tab]),
    check_key(ActivityId, Opaque, Tab, Key, '$end_of_table', next, Context).

last(ActivityId, Opaque, Tab) ->
    Context = get_transaction_context(),
    Key = execute_command_ok(Context, last, [Tab]),
    check_key(ActivityId, Opaque, Tab, Key, '$end_of_table', prev, Context).

prev(ActivityId, Opaque, Tab, Key) ->
    Context = get_transaction_context(),
    NewKey = case execute_command(Context, prev, [Tab, Key]) of
        {ok, NKey} ->
            NKey;
        {error, {key_not_found, ClosestKey}} ->
            ClosestKey;
        {error, key_not_found} ->
            mnevis_context:prev_cached_key(Context, Tab, Key)
    end,
    check_key(ActivityId, Opaque, Tab, NewKey, Key, prev, Context).

next(ActivityId, Opaque, Tab, Key) ->
    Context = get_transaction_context(),
    NewKey = case execute_command(Context, next, [Tab, Key]) of
        {ok, NKey} ->
            NKey;
        {error, {key_not_found, ClosestKey}} ->
            ClosestKey;
        {error, key_not_found} ->
            mnevis_context:next_cached_key(Context, Tab, Key)
    end,
    check_key(ActivityId, Opaque, Tab, NewKey, Key, next, Context).

foldl(ActivityId, Opaque, Fun, Acc, Tab, LockKind) ->
    First = first(ActivityId, Opaque, Tab),
    do_foldl(ActivityId, Opaque, Fun, Acc, Tab, LockKind, First).

do_foldl(_ActivityId, _Opaque, _Fun, Acc, _Tab, _LockKind, '$end_of_table') ->
    Acc;
do_foldl(ActivityId, Opaque, Fun, Acc, Tab, LockKind, Key) ->
    Recs = read(ActivityId, Opaque, Tab, Key, LockKind),
    NewAcc = lists:foldl(Fun, Acc, Recs),
    Next = next(ActivityId, Opaque, Tab, Key),
    do_foldl(ActivityId, Opaque, Fun, NewAcc, Tab, LockKind, Next).

foldr(ActivityId, Opaque, Fun, Acc, Tab, LockKind) ->
    First = last(ActivityId, Opaque, Tab),
    do_foldr(ActivityId, Opaque, Fun, Acc, Tab, LockKind, First).

do_foldr(_ActivityId, _Opaque, _Fun, Acc, _Tab, _LockKind, '$end_of_table') ->
    Acc;
do_foldr(ActivityId, Opaque, Fun, Acc, Tab, LockKind, Key) ->
    Recs = read(ActivityId, Opaque, Tab, Key, LockKind),
    NewAcc = lists:foldr(Fun, Acc, Recs),
    Prev = prev(ActivityId, Opaque, Tab, Key),
    do_foldr(ActivityId, Opaque, Fun, NewAcc, Tab, LockKind, Prev).

index_match_object(_ActivityId, _Opaque, Tab, Pattern, Pos, LockKind) ->
    Context = get_transaction_context(),
    RecList = execute_command_ok(Context, index_match_object, [Tab, Pattern, Pos, LockKind]),
    mnevis_context:filter_match_from_context(Context, Tab, Pattern, RecList).

index_read(_ActivityId, _Opaque, Tab, SecondaryKey, Pos, LockKind) ->
    Context = get_transaction_context(),
    RecList = do_index_read(Context, Tab, SecondaryKey, Pos, LockKind),
    mnevis_context:filter_index_from_context(Context, Tab, SecondaryKey, Pos, RecList).

%% TODO: table can be not present on the current node or not up-to-date
%% Make table manipulation consistent.
table_info(ActivityId, Opaque, Tab, InfoItem) ->
    mnesia:table_info(ActivityId, Opaque, Tab, InfoItem).

clear_table(_ActivityId, _Opaque, _Tab, _Obj) ->
    mnesia:abort(nested_transaction).

%% TODO: QLC API.
select(_ActivityId, _Opaque, _Tab, _MatchSpec, _LockKind) ->
    mnesia:abort(not_implemented).

select(_ActivityId, _Opaque, _Tab, _MatchSpec, _Limit, _LockKind) ->
    mnesia:abort(not_implemented).

select_cont(_ActivityId, _Opaque, _Cont) ->
    mnesia:abort(not_implemented).

%% ==========================

-spec execute_command_ok(atom(), list()) -> term().
execute_command_ok(Command, Args) ->
    execute_command_ok(get_transaction_context(), Command, Args).

-spec execute_command_ok(context(), atom(), list()) -> term().
execute_command_ok(Context, Command, Args) ->
    case execute_command(Context, Command, Args) of
        {ok, Result}    -> Result;
        {error, Reason} -> mnesia:abort({command_error, Reason})
    end.

-spec execute_command(context(), atom(), list()) -> {ok, term()} | {error, term()}.
execute_command(Context, Command, Args) ->
    RaCommand = {Command, mnevis_context:transaction_id(Context), self(), Args},
    run_ra_command(RaCommand).

-spec run_ra_command(term()) -> {ok, term()} | {error, term()}.
run_ra_command(RaCommand) ->
    NodeId = mnevis_node:node_id(),
    case ra:process_command(NodeId, RaCommand) of
        {ok, {ok, Result}, _}               -> {ok, Result};
        {ok, {error, {aborted, Reason}}, _} -> mnesia:abort(Reason);
        {ok, {error, Reason}, _}            -> {error, Reason};
        {error, Reason}                     -> mnesia:abort(Reason);
        {timeout, _}                        -> mnesia:abort(timeout)
    end.

execute_command_with_retry(Context, Command, Args) ->
    Tid = mnevis_context:transaction_id(Context),
    RaCommand = {Command, Tid, self(), Args},
    NodeId = mnevis_node:node_id(),
    Leader = retry_ra_command(NodeId, RaCommand),
    {ok, Leader}.

retry_ra_command(NodeId, RaCommand) ->
    case ra:process_command(NodeId, RaCommand) of
        {ok, {ok, ok}, Leader}              -> Leader;
        {ok, {error, {aborted, Reason}}, _} -> mnesia:abort(Reason);
        {ok, {error, Reason}, _} -> mnesia:abort({apply_error, Reason});
        {error, _Reason}         -> timer:sleep(100),
                                    retry_ra_command(NodeId, RaCommand);
        {timeout, _}             -> retry_ra_command(NodeId, RaCommand)
    end.

do_index_read(Context, Tab, SecondaryKey, Pos, LockKind) ->
    execute_command_ok(Context, index_read, [Tab, SecondaryKey, Pos, LockKind]).

record_key(Record) ->
    element(2, Record).

check_key(ActivityId, Opaque, Tab, Key, PrevKey, Direction, Context) ->
    NextFun = case Direction of
        next -> fun next/4;
        prev -> fun prev/4
    end,
    case {Key, key_inserted_between(Tab, PrevKey, Key, Direction, Context)} of
        {_, {ok, NewKey}}       -> NewKey;
        {'$end_of_table', none} -> '$end_of_table';
        {_, none} ->
            case mnevis_context:key_deleted(Context, Tab, Key) of
                true ->
                    NextFun(ActivityId, Opaque, Tab, Key);
                false ->
                    case mnevis_context:delete_object_for_key(Context, Tab, Key) of
                        [] -> Key;
                        _Recs ->
                            %% read will take cached deletes into account
                            case read(ActivityId, Opaque, Tab, Key, read) of
                                [] -> NextFun(ActivityId, Opaque, Tab, Key);
                                _  -> Key
                            end
                    end
            end
    end.

-spec key_inserted_between(table(),
                           key() | '$end_of_table',
                           key() | '$end_of_table',
                           prev | next,
                           context()) -> {ok, key()} | none.
key_inserted_between(Tab, PrevKey, Key, Direction, Context) ->
    WriteKeys = lists:usort(lists:filter(fun(WKey) ->
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
    [record_key(Rec) || {_, Rec, _} <-
        mnevis_context:writes(Context, Tab) --
            mnevis_context:deletes_object(Context, Tab)
    ])),
    case WriteKeys of
        [] -> none;
        _  ->
            case Direction of
                next -> {ok, hd(WriteKeys)};
                prev -> {ok, lists:last(WriteKeys)}
            end
    end.

cleanup_all_unlock_messages() ->
    receive
        {ra_event, _From, {machine, {mnevis_unlock, Tid}}} ->
            error_logger:info_msg("Transaction unlock message for transaction ~p on process ~p", [Tid, self()]),
            cleanup_all_unlock_messages()
    after 0 ->
        ok
    end.