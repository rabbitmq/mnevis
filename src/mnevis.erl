-module(mnevis).

-export([start/1]).

-export([create_table/2, delete_table/1]).
-export([add_table_index/2, del_table_index/2]).
-export([clear_table/1]).
-export([table_info/2]).

%% TODO: invetigate safety of transform.
-export([transform_table/3, transform_table/4]).

%% System info
-export([db_nodes/0, running_db_nodes/0]).

-export([sync_transaction/1, sync_transaction/2, sync_transaction/3]).
-export([transaction/4, transaction/3, transaction/2, transaction/1]).
-export([is_transaction/0]).
-export([sync/0]).

%% Internal API.
-export([get_consistent_version/1]).

-compile(nowarn_deprecated_function).

-define(AQUIRE_LOCK_ATTEMPTS, 10).

%% TODO: better timeout value.
%% Maybe a smarter way to handle situations with no quorum
-define(CONSISTENT_QUERY_TIMEOUT, 60000).

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

-export_type([table/0]).

start(DataDir) ->
    _ = application:load(ra),
    application:set_env(ra, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(mnevis),
    mnevis_node:start().

-spec db_nodes() -> [node()].
db_nodes() ->
    % TODO mnevis
    % Take {error, nodedown} return into account
    {ok, Nodes, _L} = ra:members(mnevis_node:node_id()),
    [Node || {_, Node} <- Nodes].

-spec running_db_nodes() -> [node()].
running_db_nodes() ->
    {ok, Nodes, _L} = ra:members(mnevis_node:node_id()),
    [Node || {Name, Node} <- Nodes,
             pong == net_adm:ping(Node)
             andalso
             undefined =/= rpc:call(Node, erlang, whereis, [Name])].

create_table(Tab, Opts) ->
    %% TODO: handle errors/retry
    {ok, R} = run_ra_command({create_table, Tab, Opts}),
    R.

delete_table(Tab) ->
    %% TODO: handle errors/retry
    {ok, R} = run_ra_command({delete_table, Tab}),
    R.

add_table_index(Tab, AttrName) ->
    {ok, R} = run_ra_command({add_table_index, Tab, AttrName}),
    R.

del_table_index(Tab, AttrName) ->
    {ok, R} = run_ra_command({del_table_index, Tab, AttrName}),
    R.

clear_table(Tab) ->
    {ok, R} = run_ra_command({clear_table, Tab}),
    R.

table_info(Tab, Info) ->
    case consistent_table_info(Tab, Info) of
        {ok, Res}    -> Res;
        {error, Err} -> mnesia:abort({aborted, Err})
    end.

%% NOTE: transform table does not support closures
%% because transform is persisted to the raft log
%% If arguments are specified there will be
%% appended to the record M:F(Record, A1, A2 ...)
transform_table(Tab, {_, _, _} = MFA, NewAttributeList, NewRecordName) ->
    {ok, R} = run_ra_command({transform_table, Tab, MFA, NewAttributeList, NewRecordName}),
    R.

transform_table(Tab, {_, _, _} = MFA, NewAttributeList) ->
    {ok, R} = run_ra_command({transform_table, Tab, MFA, NewAttributeList}),
    R.

sync_transaction(Fun) ->
    transaction(Fun, [], infinity, [{wait_for_commit, true}]).

sync_transaction(Fun, Args) ->
    transaction(Fun, Args, infinity, [{wait_for_commit, true}]).

sync_transaction(Fun, Args, Retries) ->
    transaction(Fun, Args, Retries, [{wait_for_commit, true}]).

transaction(Fun) ->
    transaction(Fun, [], infinity).

transaction(Fun, Args) ->
    transaction(Fun, Args, infinity).

transaction(Fun, Args, Retries) ->
    transaction(Fun, Args, Retries, []).

transaction(Fun, Args, Retries, Options) ->
    case is_transaction() of
        true ->
            {atomic, mnesia:activity(ets, Fun, Args, ?MODULE)};
        false ->
            case transaction0(Fun, Args, Retries, none) of
                {atomic, Result, CommitResult} ->
                    case proplists:get_value(wait_for_commit, Options, false) of
                        true  -> wait_for_commit_to_apply(CommitResult);
                        false -> ok
                    end,
                    {atomic, Result};
                Other -> Other
            end
    end.

-spec sync() -> ok | {error, term()} | {timeout, term()}.
sync() ->
    sync(?CONSISTENT_QUERY_TIMEOUT).

-spec sync(non_neg_integer()) -> ok | {error, term()} | {timeout, term()}.
sync(Timeout) ->
    Versions = mnevis_read:all_table_versions(),
    case ra:consistent_query(mnevis_node:node_id(),
                             {mnevis_machine, compare_versions, [Versions]},
                             Timeout) of
        {ok, ok, _} -> ok;
        {ok, {version_mismatch, Mismatch}, _} ->
            mnevis_read:wait_for_versions(Mismatch);
        Other -> Other
    end.

wait_for_commit_to_apply(ok) -> ok;
wait_for_commit_to_apply({versions, Versions}) ->
    mnevis_read:wait_for_versions(Versions);
wait_for_commit_to_apply({transaction, Transaction}) ->
    wait_for_transaction(Transaction).

%% Transaction implementation.
%% Can be called in recusively for retries
%% Handles aborted errors
transaction0(_Fun, _Args, 0, Err) ->
    ok = maybe_cleanup_transaction(),
    clean_transaction_context(),
    {aborted, Err};
transaction0(Fun, Args, Retries, _Err) ->
    try
        cleanup_all_unlock_messages(),
        case get_transaction_context() of
            undefined ->
                %% TODO: do not use empty context
                InitContext = mnevis_context:init(),
                update_transaction_context(InitContext);
            _ -> ok
        end,
        Res = mnesia:activity(ets, Fun, Args, ?MODULE),
        CommitResult = commit_transaction(),
        {atomic, Res, CommitResult}
    catch
        exit:{aborted, locked_nowait} ->
            retry_same_transaction(Fun, Args, Retries, locked);
        exit:{aborted, locked} ->
            %% Thansaction is still there, but it's locks were cleared.
            %% Wait for unlocked message meaning that locking transactions finished
            Context = get_transaction_context(),
            Tid = mnevis_context:transaction_id(Context),
            wait_for_unlock(Tid),
            retry_same_transaction(Fun, Args, Retries, locked);
        exit:{aborted, locker_not_running} ->
            %% Locker failed during transaction
            retry_new_transaction(Fun, Args, Retries, locker_not_running);
        exit:{aborted, wrong_locker_term} ->
            %% Locker changed during transaction
            retry_new_transaction(Fun, Args, Retries, wrong_locker_term);
        exit:{aborted, {lock_proc_not_found, Reason}} ->
            %% Need to retry finding the locker process
            %% TODO: maybe wait?
            retry_new_transaction(Fun, Args, Retries, {lock_proc_not_found, Reason});

        %% We retry the same transaction so the locks will be still in place,
        %% preventing updates. This can cause system to be locked more than
        %% needed. Needs benchmarking.
        exit:{aborted, Reason} ->
            ok = maybe_cleanup_transaction(),
            {aborted, Reason};
        E:R ->
            error_logger:warning_msg("Mnevis transaction error ~p:~p~n", [E, R]),
            ok = maybe_cleanup_transaction(),
            {aborted, R}
    after
        clean_transaction_context()
    end.

wait_for_unlock(Tid) ->
    receive {mnevis_unlock, Tid} -> ok
    after 5000 -> ok
    end.

%% Remove the transaction context and retry.
%% This retry is called when the locker process is incorrect or unknown.
retry_new_transaction(Fun, Args, Retries, Error) ->
    NextRetries = case Retries of
        infinity -> infinity;
        R when is_integer(R) -> R - 1
    end,
    % Context = get_transaction_context(),
    % error_logger:warning_msg("Retrying transaction ~p with error ~p",
        % [mnevis_context:transaction(Context), Error]),
    clean_transaction_context(),

    transaction0(Fun, Args, NextRetries, Error).

%% Keep the transaction ID and locker, but remove temp data and retry.
%% This retry is called when transaction was locked.
retry_same_transaction(Fun, Args, Retries, Error) ->
    NextRetries = case Retries of
        infinity -> infinity;
        R when is_integer(R) -> R - 1
    end,
    Context = get_transaction_context(),
    %% The context should have a transaction to be locked
    mnevis_context:assert_transaction(Context),
    % error_logger:warning_msg("Retrying transaction ~p with error ~p",
        % [mnevis_context:transaction(Context), Error]),
    Context1 = mnevis_context:cleanup_changes(Context),
    update_transaction_context(Context1),
    transaction0(Fun, Args, NextRetries, Error).

is_transaction() ->
    case get_transaction_context() of
        undefined -> false;
        _ -> true
    end.

commit_transaction() ->
    Context = get_transaction_context(),
    case mnevis_context:transaction(Context) of
        undefined -> ok;
        Transaction ->
            Writes = mnevis_context:writes(Context),
            Deletes = mnevis_context:deletes(Context),
            DeletesObject = mnevis_context:deletes_object(Context),
            CommitResult = case mnevis_context:is_empty(Context) of
                true -> ok;
                _ ->
                    case {Writes, Deletes, DeletesObject} of
                        {[], [], []} ->
                            %% Read-only commits
                            read_only_commit(Context);
                        _ ->
                            {ok, Result, _} =
                                execute_command_with_retry(Context, commit,
                                                           {Writes,
                                                            Deletes,
                                                            DeletesObject}),
                            case Result of
                                {committed, Versions} ->
                                    {versions, Versions};
                                skipped ->
                                    {transaction, mnevis_context:transaction(Context)}
                            end
                    end
            end,
            cleanup_transaction(Transaction),
            CommitResult
    end.

read_only_commit(Context) ->
    Locker = mnevis_context:locker(Context),
    case ra:consistent_query(mnevis_node:node_id(),
                             {mnevis_machine, check_locker, [Locker]}) of
        {ok, ok, _}              -> ok;
        {ok, {error, Reason}, _} -> mnesia:abort(Reason);
        {error, Reason} -> mnesia:abort(Reason);
        {timeout, _}    -> mnesia:abort(timeout)
    end.

maybe_cleanup_transaction() ->
    Context = get_transaction_context(),
    case mnevis_context:transaction(Context) of
        undefined   -> ok;
        Transaction -> cleanup_transaction(Transaction)
    end,
    ok.

cleanup_transaction({TransactionId, Locker}) ->
    mnevis_lock_proc:cleanup(TransactionId, Locker).

update_transaction_context(Context) ->
    put(mnevis_transaction_context, Context).

clean_transaction_context() ->
    cleanup_all_unlock_messages(),
    erase(mnevis_transaction_context).

get_transaction_context() ->
    get(mnevis_transaction_context).

%% Mnesia activity API

lock(_ActivityId, _Opaque, LockItem, LockKind) ->
    with_lock(get_transaction_context(), LockItem, LockKind, fun(_) -> ok end).

write(ActivityId, Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    with_lock(Context, {Tab, record_key(Rec)}, LockKind, fun(_) ->
        Context1 = get_transaction_context(),
        Context2 = case maybe_safe_table_info(ActivityId, Opaque, Tab, type) of
            bag ->
                mnevis_context:add_write_bag(Context1, Tab, Rec, LockKind);
            Set when Set =:= set; Set =:= ordered_set ->
                mnevis_context:add_write_set(Context1, Tab, Rec, LockKind)
        end,
        update_transaction_context(Context2),
        ok
    end).

delete(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context = get_transaction_context(),
    with_lock(Context, {Tab, Key}, LockKind, fun(_) ->
        Context1 = get_transaction_context(),
        Context2 = mnevis_context:add_delete(Context1, Tab, Key, LockKind),
        update_transaction_context(Context2),
        ok
    end).

delete_object(_ActivityId, _Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    with_lock(Context, {Tab, record_key(Rec)}, LockKind, fun(_) ->
        Context1 = get_transaction_context(),
        Context2 = mnevis_context:add_delete_object(Context1, Tab, Rec, LockKind),
        update_transaction_context(Context2),
        ok
    end).

read(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context0 = get_transaction_context(),
    case mnevis_context:read(Context0, Tab, Key) of
        {written, set, Record} ->
            mnevis_context:filter_read(Context0, Tab, Key, [Record]);
        deleted -> [];
        {deleted_and_written, bag, Recs} -> Recs;
        _ ->
            ReadQuery = {dirty_read, [Tab, Key]},
            with_lock_and_version(Context0, {Tab, Key}, LockKind, fun() ->
                Context1 = get_transaction_context(),
                %% We read here separately, because lock kind may be different from the
                %% previous.
                %% We use dirty operations here because we want to call them
                %% directly via erlang:apply/3
                {RecList, Context2} = execute_local_read_query(ReadQuery, Context1),
                mnevis_context:filter_read(Context2, Tab, Key, RecList)
            end)
    end.

execute_local_read_query({Op, Args} = ReadSpec, Context) ->
    case mnevis_context:get_read(Context, ReadSpec) of
        {ok, CachedRecList} ->
            {CachedRecList, Context};
        {error, not_found} ->
            RecList = erlang:apply(mnesia, Op, Args),
            NewContext = mnevis_context:add_read(Context, ReadSpec, RecList),
            update_transaction_context(NewContext),
            {RecList, NewContext}
    end.


match_object(_ActivityId, _Opaque, Tab, Pattern, LockKind) ->
    Context0 = get_transaction_context(),
    with_lock_and_version(Context0, {table, Tab}, LockKind, fun() ->
        Context1 = get_transaction_context(),
        {RecList, Context2} = execute_local_read_query({dirty_match_object, [Tab, Pattern]}, Context1),
        mnevis_context:filter_match(Context2, Tab, Pattern, RecList)
    end).

all_keys(ActivityId, Opaque, Tab, LockKind) ->
    Context = get_transaction_context(),
    with_lock_and_version(Context, {table, Tab}, LockKind, fun() ->
        Context1 = get_transaction_context(),
        {AllKeys, Context2} = execute_local_read_query({dirty_all_keys, [Tab]}, Context1),

        case mnevis_context:deletes_object(Context2, Tab) of
            [] ->
                mnevis_context:filter_all_keys(Context2, Tab, AllKeys);
            Deletes ->
                DeletedKeys = lists:filtermap(fun({_, Rec, _}) ->
                    Key = record_key(Rec),
                    case read(ActivityId, Opaque, Tab, Key, LockKind) of
                        [] -> {true, Key};
                        _  -> false
                    end
                end,
                Deletes),
                mnevis_context:filter_all_keys(Context2, Tab, AllKeys -- DeletedKeys)
        end
    end).

first(ActivityId, Opaque, Tab) ->
    Context = get_transaction_context(),
    with_lock_and_version(Context, {table, Tab}, read, fun() ->
        Context1 = get_transaction_context(),
        {Key, Context2} = execute_local_read_query({dirty_first, [Tab]}, Context1),
        check_key(ActivityId, Opaque, Tab, Key, '$end_of_table', next, Context2)
    end).

last(ActivityId, Opaque, Tab) ->
    Context = get_transaction_context(),
    with_lock_and_version(Context, {table, Tab}, read, fun() ->
        Context1 = get_transaction_context(),
        {Key, Context2} = execute_local_read_query({dirty_last, [Tab]}, Context1),
        check_key(ActivityId, Opaque, Tab, Key, '$end_of_table', prev, Context2)
    end).

prev(ActivityId, Opaque, Tab, Key) ->
    Context = get_transaction_context(),
    with_lock_and_version(Context, {table, Tab}, read, fun() ->
        Context1 = get_transaction_context(),
        try
            {NewKey, Context2} = execute_local_read_query({dirty_prev, [Tab, Key]}, Context1),
            check_key(ActivityId, Opaque, Tab, NewKey, Key, prev, Context2)
        catch
            exit:{aborted, {badarg, [Tab, Key]}} ->
                closest_key(prev, Tab, Key)
        end
    end).

next(ActivityId, Opaque, Tab, Key) ->
    Context = get_transaction_context(),
    with_lock_and_version(Context, {table, Tab}, read, fun() ->
        Context1 = get_transaction_context(),
        try
            {NewKey, Context2} = execute_local_read_query({dirty_next, [Tab, Key]}, Context1),
            check_key(ActivityId, Opaque, Tab, NewKey, Key, next, Context2)
        catch
            exit:{aborted, {badarg, [Tab, Key]}} ->
                closest_key(next, Tab, Key)
        end
    end).

-spec closest_key(prev | next, mnevis:table(), Key) -> Key.
closest_key(prev, Tab, Key) ->
    case mnesia:table_info(Tab, type) of
        ordered_set ->
           closest_prev(Tab, Key);
        _ ->
           mnesia:dirty_last(Tab)
    end;
closest_key(next, Tab, Key) ->
    case mnesia:table_info(Tab, type) of
        ordered_set ->
           closest_next(Tab, Key);
        _ ->
           mnesia:dirty_first(Tab)
    end.

-spec closest_next(mnevis:table(), Key) -> Key.
closest_next(Tab, Key) ->
    First = mnesia:dirty_first(Tab),
    closest_next(Tab, Key, First).

-spec closest_next(mnevis:table(), Key, Key) -> Key.
closest_next(_Tab, _Key, '$end_of_table') ->
    '$end_of_table';
closest_next(Tab, Key, CurrentKey) ->
    case Key < CurrentKey of
        true  -> CurrentKey;
        false -> closest_next(Tab, Key, mnesia:dirty_next(Tab, CurrentKey))
    end.

-spec closest_prev(mnevis:table(), Key) -> Key.
closest_prev(Tab, Key) ->
    First = mnesia:dirty_last(Tab),
    closest_prev(Tab, Key, First).

-spec closest_prev(mnevis:table(), Key, Key) -> Key.
closest_prev(_Tab, _Key, '$end_of_table') ->
    '$end_of_table';
closest_prev(Tab, Key, CurrentKey) ->
    case Key > CurrentKey of
        true  -> CurrentKey;
        false -> closest_prev(Tab, Key, mnesia:dirty_prev(Tab, CurrentKey))
    end.

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
    with_lock_and_version(Context, {table, Tab}, LockKind, fun() ->
        Context1 = get_transaction_context(),
        {RecList, Context2} = execute_local_read_query({dirty_index_match_object, [Tab, Pattern, Pos]}, Context1),
        mnevis_context:filter_match(Context2, Tab, Pattern, RecList)
    end).

index_read(_ActivityId, _Opaque, Tab, SecondaryKey, Pos, LockKind) ->
    Context = get_transaction_context(),
    with_lock_and_version(Context, {table, Tab}, LockKind, fun() ->
        Context1 = get_transaction_context(),
        {RecList, Context2} = execute_local_read_query({dirty_index_read, [Tab, SecondaryKey, Pos]}, Context1),
        mnevis_context:filter_index(Context2, Tab, SecondaryKey, Pos, RecList)
    end).

table_info(_ActivityId, _Opaque, Tab, InfoItem) ->
    case consistent_table_info(Tab, InfoItem) of
        {ok, Result}              -> Result;
        {error, {no_exists, Tab}} -> mnesia:abort({no_exists, Tab, InfoItem})
    end.

% TODO first two args are unused
consistent_table_info(Tab, InfoItem) ->
    case ra:consistent_query(mnevis_node:node_id(),
                             {mnevis_machine, safe_table_info, [Tab, InfoItem]}) of
        {ok, Result, _} ->
            Result;
        {error, Err} ->
            mnesia:abort(Err);
        {timeout, Timeout} ->
            mnesia:abort({table_info_timeout, Timeout})
    end.

local_table_info(ActivityId, Opaque, Tab, InfoItem) ->
    mnesia:table_info(ActivityId, Opaque, Tab, InfoItem).

clear_table(_ActivityId, _Opaque, _Tab, _Obj) ->
    mnesia:abort(nested_transaction).

select(_ActivityId, _Opaque, Tab, MatchSpec, LockKind) ->
    Context = get_transaction_context(),
    LockItem = select_lock_item(Tab, MatchSpec),
    with_lock_and_version(Context, LockItem, LockKind, fun() ->
        Context1 = get_transaction_context(),
        %% Optimisation. If there are no records in the context
        %% the code is much more simple.
        case mnevis_context:has_changes_for_table(Tab, Context1) of
            false ->
                {RecList, _Context2} =
                    execute_local_read_query({dirty_select, [Tab, MatchSpec]}, Context1),
                RecList;
            true ->
                select_with_context(Tab, MatchSpec, Context1)
        end
    end).

select_with_context(Tab, MatchSpec, Context0) ->
    case MatchSpec of
        %% Optimisation. No additional checks required
        [{_, _, ['$_']}] ->
            {RecList, Context1} =
                execute_local_read_query({dirty_select, [Tab, MatchSpec]}, Context0),
            mnevis_context:filter_match_spec(Context1, Tab, MatchSpec, RecList);
        %% A single match expression.
        %% Select original records and transform them after filete
        [{H, C, T}] ->
            RecordMatchSpec = [{H, C, ['$_']}],
            {RecList, Context1} =
                execute_local_read_query({dirty_select, [Tab, RecordMatchSpec]}, Context0),
            FilteredList = mnevis_context:filter_match_spec(Context1, Tab, RecordMatchSpec, RecList),
            %% Transform records to final values
            %% using a match spec without conditions
            Compiled = ets:match_spec_compile([{H, [], T}]),
            ets:match_spec_run(FilteredList, Compiled);
        _ ->
            %% Complex match spec.
            %% Need to associate each expression with its own set of results
            %% Each match spec result is a tagged record,
            %% which is transformed to the final form by running the match
            %% spec again without conditions.
            %% This is exreemely inefficient, but mnesia also does not recommend
            %% to run select with continuation after write/delete.
            {ReversedRecordMatchSpec, TransformsMap} =
                lists:foldl(fun({H, C, T}, {I, Spec, Transforms}) ->
                    %% Index
                    {I + 1,
                    %% Each expression result record will be tagged with the index
                     [{H, C, [{{I, '$_'}}]} | Spec],
                    %% Mapping from index to result type
                     maps:put(I, ets:match_spec_compile([{H, [], T}]), Transforms)}
                end),
            %% Keep original order
            RecordMatchSpec = lists:reverse(ReversedRecordMatchSpec),
            {TaggedRecList0, Context1} =
                execute_local_read_query({dirty_select, [Tab, RecordMatchSpec]}, Context0),
            TaggedRecList =
                mnevis_context:filter_tagged_match_spec(Context1, Tab, RecordMatchSpec, TaggedRecList0),
            %% TODO: this should be possible to do with a single match spec.
            lists:map(fun({Tag, Rec}) ->
                Transform = maps:get(Tag, TransformsMap),
                %% Transform should always match
                [Res] = ets:match_spec_run([Rec], Transform),
                Res
            end,
            TaggedRecList)
    end.

select_lock_item(Tab, MatchSpec) ->
    case MatchSpec of
        [{HeadPat,_, _}] when is_tuple(HeadPat), tuple_size(HeadPat) > 2 ->
            Key = element(2, HeadPat),
            case mnesia:has_var(Key) of
                false -> {Tab, Key};
                true  -> {table, Tab}
            end;
        _ -> {table, Tab}
    end.

%% TODO: Implement limit. This may require using undocumented APIs
select(ActivityId, Opaque, Tab, MatchSpec, _Limit, LockKind) ->
    %% Get all the messages in the first batch.
    %% Mnesia docs mention that the Limit here is not a requirement
    %% This should be enough to support (inefficient) QLC API
    Result = select(ActivityId, Opaque, Tab, MatchSpec, LockKind),
    {Result, '$end_of_table'}.

select_cont(_ActivityId, _Opaque, '$end_of_table') ->
    '$end_of_table';
select_cont(_ActivityId, _Opaque, _Cont) ->
    mnesia:abort(not_implemented).


%% ==========================

%% Internal public functions

-spec get_consistent_version(mnevis_lock:lock_item()) ->
    {ok, mnevis_context:read_version()} |
    {error, term()}.
get_consistent_version(LockItem) ->
    case ra:consistent_query(mnevis_node:node_id(),
                             {mnevis_machine, get_item_version, [LockItem]},
                             ?CONSISTENT_QUERY_TIMEOUT) of
        {ok, Result, _} ->
            Result;
        {error, Err} ->
            {error, Err};
        {timeout, TO} ->
            {error, {timeout, TO}}
    end.

%% ==========================

%% Internal functions

% TODO
% -spec execute_command(context(), atom(), term()) -> {ok, term()} | {error, term()}.
% execute_command(Context, Command, Args) ->
%     mnevis_context:assert_transaction(Context),
%     Transaction = mnevis_context:transaction(Context),
%     RaCommand = {Command, Transaction, Args},
%     run_ra_command(RaCommand).


-spec run_ra_command(term()) -> {ok, term()} | {error, term()}.
run_ra_command(RaCommand) ->
    NodeId = mnevis_node:node_id(),
    case ra:process_command(NodeId, RaCommand) of
        {ok, {ok, Result}, _}                    -> {ok, Result};
        {ok, {error, {aborted, Reason}}, _}      -> mnesia:abort(Reason);
        {ok, {error, Reason}, _}                 -> {error, Reason};
        {error, Reason}                          -> mnesia:abort(Reason);
        {timeout, _}                             -> mnesia:abort(timeout)
    end.

execute_command_with_retry(Context, Command, Args) ->
    mnevis_context:assert_transaction(Context),
    Transaction = mnevis_context:transaction(Context),
    RaCommand = {Command, Transaction, Args},
    NodeId = mnevis_node:node_id(),
    {ok, Result, Leader} = retry_ra_command(NodeId, RaCommand),
    {ok, Result, Leader}.

retry_ra_command(NodeId, RaCommand) ->
    %% TODO: better timeout value?
    case ra:process_command(NodeId, RaCommand) of
        {ok, {ok, Result}, Leader}          -> {ok, Result, Leader};
        {ok, {error, {aborted, Reason}}, _} -> mnesia:abort(Reason);
        {ok, {error, Reason}, _}            -> mnesia:abort({apply_error, Reason});
        {error, _Reason}                    -> timer:sleep(100),
                                               retry_ra_command(NodeId, RaCommand);
        {timeout, _}                        -> retry_ra_command(NodeId, RaCommand)
    end.

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
        {mnevis_unlock, Tid} ->
            error_logger:info_msg("Transaction unlock message for transaction ~p on process ~p", [Tid, self()]),
            cleanup_all_unlock_messages()
    after 0 ->
        ok
    end.

with_lock_and_version(Context, LockItem, LockKind, Fun) ->
    with_lock(Context, LockItem, LockKind, fun(Result) ->
        case Result of
            cached ->
                Fun();
            {ok, Version} ->
                mnevis_read:wait_for_versions([Version]),
                case LockItem of
                    {_, _} ->
                        mark_version_up_to_date(LockItem);
                    {global, _, _} ->
                        ok
                end,
                Fun();
            {error, no_exists} ->
                Fun();
            {error, Err} ->
                mnesia:abort(Err)
        end
    end,
    lock_and_version).

mark_version_up_to_date(LockItem) ->
    Context0 = get_transaction_context(),
    Context1 = mnevis_context:mark_version_up_to_date(LockItem, Context0),
    update_transaction_context(Context1).

with_lock(Context, LockItem, LockKind, Fun) ->
    with_lock(Context, LockItem, LockKind, Fun, lock).

with_lock(Context, LockItem, LockKind, Fun, Mode) ->
    case do_acquire_lock(Context, LockItem, LockKind, Mode) of
        {ok, Response, Context1} ->
            Context2 = mnevis_context:set_lock_acquired(LockItem, LockKind, Context1),
            update_transaction_context(Context2),
            Fun(Response);
        {error, Err, Context1} ->
            update_transaction_context(Context1),
            mnesia:abort(Err)
    end.

-spec do_acquire_lock(mnevis_context:context(),
                      mnevis_lock:lock_item(),
                      mnevis_lock:lock_kind(),
                      mnevis_lock_proc:lock_method()) ->
    {ok, term(), context()} |
    {error, locked, context()} |
    {error, locked_nowait, context()}.
do_acquire_lock(Context, LockItem, LockKind, Mode) ->
    case mnevis_context:has_transaction(Context) of
        true ->
            do_acquire_lock_with_existing_transaction(Context, LockItem, LockKind, Mode);
        false ->
            do_acquire_lock_with_new_transaction(Context, LockItem, LockKind, Mode, ?AQUIRE_LOCK_ATTEMPTS)
    end.

-spec do_acquire_lock_with_existing_transaction(mnevis_context:context(),
                                                mnevis_lock:lock_item(),
                                                mnevis_lock:lock_kind(),
                                                mnevis_lock_proc:lock_method()) ->
    {ok, context()} |
    {ok, term(), context()} |
    {error, locked, context()} |
    {error, locked_nowait, context()}.
do_acquire_lock_with_existing_transaction(Context, LockItem, LockKind, Mode) ->
    Tid = mnevis_context:transaction_id(Context),
    case lock_already_acquired(LockItem, LockKind, mnevis_context:locks(Context)) of
        true ->
            case Mode of
                lock ->
                    {ok, ok, Context};
                lock_and_version ->
                    case mnevis_context:is_version_up_to_date(LockItem, Context) of
                        true  ->
                            {ok, cached, Context};
                        false ->
                            {ok, get_consistent_version(LockItem), Context}
                    end
            end;
        false ->
            Locker = mnevis_context:locker(Context),
            LockRequest = {lock, Tid, self(), LockItem, LockKind, Mode},
            case mnevis_lock_proc:try_lock_call(Locker, LockRequest) of
                {ok, Tid, Response}           -> {ok, Response, Context};
                {error, {locked, Tid}}        -> {error, locked, Context};
                {error, {locked_nowait, Tid}} -> {error, locked_nowait, Context};
                {error, Err}                  -> mnesia:abort(Err)
            end
    end.

lock_already_acquired({table, _} = LockItem, LockKind, Locks) ->
    lock_already_acquired_1(LockItem, LockKind, Locks);
lock_already_acquired({Tab, Key}, LockKind, Locks) ->
    lock_already_acquired_1({table, Tab}, LockKind, Locks)
    orelse
    lock_already_acquired_1({Tab, Key}, LockKind, Locks);
lock_already_acquired({global, _, _} = LockItem, LockKind, Locks) ->
    lock_already_acquired_1(LockItem, LockKind, Locks).

lock_already_acquired_1(LockItem, LockKind, Locks) ->
    case {LockKind, maps:get(LockItem, Locks, none)} of
        {read,  read}  -> true;
        {read,  write} -> true;

        {write, read}  -> false;
        {write, write} -> true;

        {_, none}      -> false
    end.

do_acquire_lock_with_new_transaction(_Context, _LockItem, _LockKind, _Mode, 0) ->
    mnesia:abort({unable_to_acquire_lock, no_promoted_lock_processes});
do_acquire_lock_with_new_transaction(Context, LockItem, LockKind, Mode, _Attempts) ->
    ok = mnevis_context:assert_no_transaction(Context),
    Locker = case mnevis_lock_proc:locate() of
        {ok, L}      -> L;
        {error, Err} -> mnesia:abort(Err)
    end,
    LockRequest = {lock, undefined, self(), LockItem, LockKind, Mode},
    case retry_lock_call(Locker, LockRequest) of
        {ok, Tid1, Response} ->
            NewContext = mnevis_context:set_transaction({Tid1, Locker}, Context),
            {ok, Response, NewContext};
        {error, {locked, Tid1}} ->
            NewContext = mnevis_context:set_transaction({Tid1, Locker}, Context),
            {error, locked, NewContext};
        {error, {locked_nowait, Tid1}} ->
            NewContext = mnevis_context:set_transaction({Tid1, Locker}, Context),
            {error, locked_nowait, NewContext};
        %% TODO: handle different failures (wait and retry). Monitor that
        {error, Error} ->
            mnesia:abort(Error)
    end.

retry_lock_call(_Locker, _LockRequest, 0) ->
    mnesia:abort({unable_to_acquire_lock, no_promoted_lock_processes});
retry_lock_call(Locker, LockRequest, Attempts) ->
    case mnevis_lock_proc:try_lock_call(Locker, LockRequest) of
        {error, locker_not_running} ->
            %% This function should block, so it's safe to recursively call
            case mnevis_lock_proc:ensure_lock_proc(Locker) of
                {ok, NewLocker} ->
                    retry_lock_call(NewLocker, LockRequest, Attempts);
                {error, {command_error, Reason}} ->
                    mnesia:abort({lock_proc_not_found, Reason});
                {error, Reason} ->
                    mnesia:abort(Reason)
            end;
        Other -> Other
    end.

wait_for_transaction(Transaction) ->
    wait_for_transaction(Transaction, 2000).

wait_for_transaction(Transaction, 0) ->
    error({no_more_attempts_waiting_for_transaction_to_apply_locally, Transaction});
wait_for_transaction(Transaction, Attempts) ->
    %% TODO: mnesia subscribe.
    case ets:lookup(committed_transaction, Transaction) of
        [] -> timer:sleep(100),
              wait_for_transaction(Transaction, Attempts - 1);
        _  -> ok
    end.

maybe_safe_table_info(ActivityId, Opaque, Tab, Key) ->
    case catch local_table_info(ActivityId, Opaque, Tab, Key) of
        {'EXIT', {aborted, {no_exists, Tab, Key}}} ->
            case consistent_table_info(Tab, Key) of
                {ok, Result}              -> Result;
                {error, {no_exists, Tab}} -> mnesia:abort({no_exists, Tab})
            end;
        Res -> Res
    end.

