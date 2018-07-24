-module(ramnesia_machine).
-behaviour(ra_machine).

-include_lib("ra/include/ra.hrl").

-export([
         init/1,
         apply/4,
         leader_effects/1,
         eol_effects/1,
         tick/2,
         overview/1]).

-type config() :: map().
-type command() :: term().

-type reply() :: {ok, term()} | {error, term()}.
-type reply(T) :: {ok, T} | {error, term()}.
-type reply(T, E) :: {ok, T} | {error, E}.


-record(state, {last_transaction_id = 0,
                transactions = #{},
                committed_transactions = #{},
                read_locks = #{},
                write_locks = #{},
                transaction_locks = simple_dgraph:new()}).

-ifdef (TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type state() :: #state{}.
-type transaction_id() :: integer().

-type table() :: atom().
-type lock_item() :: {table(), term()} | {table, table()} | {global, term(), [node()]}.
-type lock_kind() :: read | write.
-type change() :: {table(), term(), lock_kind()}.

-type apply_result() :: {state(), ra_machine:effects(), reply()}.

-type apply_result(T) :: {state(), ra_machine:effects(), reply(T)}.

-type apply_result(T, Err) :: {state(), ra_machine:effects(), reply(T, Err)}.

%% Ra machine callbacks

-spec init(config()) -> {state(), ra_machine:effects()}.
init(_Conf) ->
    {#state{}, []}.

-spec apply(map(), command(), ra_machine:effects(), state()) ->
    {state(), ra_machine:effects()} | {state(), ra_machine:effects(), reply()}.

apply(_RaftIdx, {start_transaction, Source}, Effects0, State) ->
    %% Cleanup stale transactions for the source.
    %% Ignore demonitor effect, because there will be a monitor effect
    {State1, _, _} = case transaction_for_source(Source, State) of
        {ok, OldTid}            -> cleanup(OldTid, Source, State);
        {error, no_transaction} -> {State, [], ok}
    end,
    with_pre_effects(Effects0, start_transaction(State1, Source));

apply(_RaftIdx, {rollback, Tid, Source, []}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                cleanup(Tid, Source, State)
            end));

apply(_RaftIdx, {commit, Tid, Source, [Writes, Deletes, DeletesObject]}, Effects0, State) ->
    with_pre_effects(Effects0,
        maybe_skip_committed(Tid, Source, State,
            fun() ->
                with_transaction(Tid, Source, State,
                    fun() ->
                        commit(Tid, Source, Writes, Deletes, DeletesObject, State)
                    end)
            end));

apply(_Meta, {finish, Tid, Source}, Effects0, State) ->
    with_pre_effects(Effects0, cleanup_commmitted(Tid, Source, State));

apply(_RaftIdx, {lock, Tid, Source, [LockItem, LockKind]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                lock(LockItem, LockKind, Tid, Source, State)
            end));

apply(_RaftIdx, {read, Tid, Source, [Tab, Key, LockKind]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({Tab, Key}, LockKind, Tid, Source, State,
                    fun() ->
                        {ok, mnesia:dirty_read(Tab, Key)}
                    end)
            end));

apply(_RaftIdx, {index_read, Tid, Source, [Tab, SecondaryKey, Pos, LockKind]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, LockKind, Tid, Source, State,
                    fun() ->
                        {ok, mnesia:dirty_index_read(Tab, SecondaryKey, Pos)}
                    end)
            end));

apply(_RaftIdx, {match_object, Tid, Source, [Tab, Pattern, LockKind]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, LockKind, Tid, Source, State,
                    fun() ->
                        {ok, mnesia:dirty_match_object(Tab, Pattern)}
                    end)
            end));

apply(_RaftIdx, {index_match_object, Tid, Source, [Tab, Pattern, Pos, LockKind]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, LockKind, Tid, Source, State,
                    fun() ->
                        {ok, mnesia:dirty_index_match_object(Tab, Pattern, Pos)}
                    end)
            end));

apply(_RaftIdx, {all_keys, Tid, Source, [Tab, LockKind]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, LockKind, Tid, Source, State,
                    fun() ->
                        {ok, mnesia:dirty_all_keys(Tab)}
                    end)
            end));

apply(_RaftIdx, {first, Tid, Source, [Tab]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, read, Tid, Source, State,
                    fun() ->
                        {ok, mnesia:dirty_first(Tab)}
                    end)
            end));

apply(_RaftIdx, {last, Tid, Source, [Tab]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, read, Tid, Source, State,
                    fun() ->
                        {ok, mnesia:dirty_last(Tab)}
                    end)
            end));

apply(_RaftIdx, {prev, Tid, Source, [Tab, Key]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, read, Tid, Source, State,
                    fun() ->
                        try {ok, mnesia:dirty_prev(Tab, Key)}
                        catch
                            exit:{aborted, {badarg, [Tab, Key]}} ->
                                case mnesia:table_info(Tab, type) of
                                    ordered_set ->
                                    %% TODO: make error not aborted
                                        {error, {aborted,
                                            {key_not_found,
                                             closest_prev(Tab, Key)}}};
                                    _ ->
                                        {error, {aborted,
                                            {key_not_found,
                                             mnesia:dirty_last(Tab)}}}
                                end
                        end
                    end)
            end));

apply(_RaftIdx, {next, Tid, Source, [Tab, Key]}, Effects0, State) ->
    with_pre_effects(Effects0,
        with_transaction(Tid, Source, State,
            fun() ->
                with_lock_catch_abort({table, Tab}, read, Tid, Source, State,
                    fun() ->
                        try {ok, mnesia:dirty_next(Tab, Key)}
                        catch
                            exit:{aborted, {badarg, [Tab, Key]}} ->
                                case mnesia:table_info(Tab, type) of
                                    ordered_set ->
                                        {error, {aborted,
                                            {key_not_found,
                                             closest_next(Tab, Key)}}};
                                    _ ->
                                        {error, {aborted,
                                            {key_not_found,
                                             mnesia:dirty_first(Tab)}}}
                                end
                        end
                    end)
            end));
%% TODO: return type for create_table
apply(_RaftIdx, {create_table, Tab, Opts}, Effects0, State) ->
    {State, Effects0, {ok, mnesia:create_table(Tab, Opts)}};

apply(_RaftIdx, {delete_table, Tab}, Effects0, State) ->
    {State, Effects0, {ok, mnesia:delete_table(Tab)}};

apply(_RaftIdx, {down, Source, _Reason}, Effects0, State) ->
    case transaction_for_source(Source, State) of
        {ok, Tid}               ->
            with_pre_effects(Effects0, cleanup(Tid, Source, State));
        {error, no_transaction} ->
            {State, Effects0, ok}
    end.

-spec leader_effects(state()) -> ra_machine:effects().
leader_effects(State) -> remonitor_sources(State).

-spec eol_effects(state()) -> ra_machine:effects().
eol_effects(_State) -> [].

-spec tick(TimeMs :: integer(), state()) -> ra_machine:effects().
tick(_Time, _State) -> [].

-spec overview(state()) -> map().
overview(_State) -> #{}.

%% ==========================

%% Top level helpers

-spec start_transaction(state(), pid()) ->
    apply_result(transaction_id()).
start_transaction(State, Source) ->
    #state{last_transaction_id = LastTid,
           transactions = Transactions} = State,
    Tid = LastTid + 1,

    {State#state{last_transaction_id = Tid,
                 transactions = maps:put(Source, Tid, Transactions)},
     [{monitor, process, Source}],
     {ok, Tid}}.

-spec commit(transaction_id(), pid(), [change()], [change()], [change()], state()) -> apply_result(ok).
commit(Tid, Source, Writes, Deletes, DeletesObject, State0) ->
%% TODO: record committed transaction number.
    case mnesia:transaction(fun() ->
            _ = apply_deletes(Deletes),
            _ = apply_writes(Writes),
            _ = apply_deletes_object(DeletesObject),
            ok
        end) of
        {atomic, ok} ->
            State1 = cleanup_locks(Tid, State0),
            {Effects, State2} = cleanup_transaction_locks(Tid, State1),
            State3 = mark_committed(Tid, Source, State2),
            {State3, Effects, {ok, ok}};
        {aborted, Reason} ->
            {State0, [], {error, {aborted, Reason}}}
    end.

-spec cleanup(transaction_id(), pid(), state()) ->
    apply_result(ok).
cleanup(Tid, Source, State0) ->
    State1 = cleanup_locks(Tid, State0),
    {UnlockedEffects, State2} = cleanup_transaction_locks(Tid, State1),

    %% Remove source from transactions.
    #state{ transactions = Transactions,
            committed_transactions = Committed } = State2,
    Transactions1 = maps:remove(Source, Transactions),
    Committed1 = maps:remove(Tid, Committed),
    State3 = State2#state{transactions = Transactions1,
                          committed_transactions = Committed1},

    {State3, UnlockedEffects ++ [{demonitor, process, Source}], {ok, ok}}.

-spec cleanup_commmitted(transaction_id(), pid(), state()) -> apply_result().
cleanup_commmitted(Tid, Source, State) ->
    #state{ committed_transactions = Committed } = State,
    case maps:get(Tid, Committed, not_found) of
        not_found ->
            {State, [], {error, transaction_not_committed}};
        Source ->
            %% TODO: simpler cleanup since there should be no locks
            cleanup(Tid, Source, State);
        OtherSource ->
            {State, [], {error, {wrong_transaction_source, OtherSource}}}
    end.

-spec lock(lock_item(), lock_kind(), transaction_id(), pid(), state()) ->
    apply_result(ok, locked | locked_instant).
lock(LockItem, LockKind, Tid, Source, State) ->
    case locking_transactions(LockItem, LockKind, Tid, State) of
        [] ->
            {apply_lock(LockItem, LockKind, Tid, State), [], {ok, ok}};
        Tids ->
            %% To avoid cycles only wait for higher transaction Ids
            LockingTids = [ LockingTid
                            || LockingTid <- Tids,
                            LockingTid > Tid ],
            Error = case LockingTids of
                [] -> {error, locked_instant};
                _ -> {error, locked}
            end,
            %% Cleanup locks for the locked transaction.
            %% The transaction will be restarted with the same ID
            State1 = cleanup_locks(Tid, State),
            State2 = lock_transaction(Tid, Source, LockingTids, State1),
            {State2, [], Error}
    end.

%% ==========================

%% Mnesia operations

-spec apply_deletes([change()]) -> [ok].
apply_deletes(Deletes) ->
    [ok = mnesia:delete(Tab, Key, LockKind)
     || {Tab, Key, LockKind} <- Deletes].

-spec apply_deletes_object([change()]) -> [ok].
apply_deletes_object(DeletesObject) ->
    [ok = mnesia:delete_object(Tab, Rec, LockKind)
     || {Tab, Rec, LockKind} <- DeletesObject].

-spec apply_writes([change()]) -> [ok].
apply_writes(Writes) ->
    [ok = mnesia:write(Tab, Rec, LockKind)
     || {Tab, Rec, LockKind} <- Writes].

-spec closest_next(table(), Key) -> Key.
closest_next(Tab, Key) ->
    First = mnesia:dirty_first(Tab),
    closest_next(Tab, Key, First).

-spec closest_next(table(), Key, Key) -> Key.
closest_next(_Tab, _Key, '$end_of_table') ->
    '$end_of_table';
closest_next(Tab, Key, CurrentKey) ->
    case Key < CurrentKey of
        true  -> CurrentKey;
        false -> closest_next(Tab, Key, mnesia:dirty_next(Tab, CurrentKey))
    end.

-spec closest_prev(table(), Key) -> Key.
closest_prev(Tab, Key) ->
    First = mnesia:dirty_last(Tab),
    closest_prev(Tab, Key, First).

-spec closest_prev(table(), Key, Key) -> Key.
closest_prev(_Tab, _Key, '$end_of_table') ->
    '$end_of_table';
closest_prev(Tab, Key, CurrentKey) ->
    case Key > CurrentKey of
        true  -> CurrentKey;
        false -> closest_prev(Tab, Key, mnesia:dirty_prev(Tab, CurrentKey))
    end.

%% ==========================

%% State modification

-spec lock_transaction(transaction_id(), pid(), [transaction_id()], state()) -> state().
lock_transaction(LockedTid, Source, LockingTids0, State) ->
    LockingTids = [ LockingTid || LockingTid <- LockingTids0,
                                  LockingTid > LockedTid ],

    #state{transaction_locks = TLGraph0} = State,

    %% Locked transaction is a graph vertex with the source as a label
    TLGraph1 = simple_dgraph:add_vertex(TLGraph0, LockedTid, Source),

    %% Discard all the old locks.
    %% We don't want to delete the vertex, because it can also be
    %% a locker for other transactions.
    OldLocks = simple_dgraph:in_edges(TLGraph1, LockedTid),
    TLGraph2 = simple_dgraph:del_edges(TLGraph1, OldLocks),

    TLGraph3 = lists:foldl(fun(LockingTid, TLGraphAcc) ->
        %% We should not rewrite the locker vertex labels
        %% as they can be locked by other transactions
        TLGraphAcc1 = simple_dgraph:ensure_vertex(TLGraphAcc, LockingTid),
        {ok, G} = simple_dgraph:add_edge(TLGraphAcc1, LockingTid, LockedTid),
        G
    end,
    TLGraph2,
    LockingTids),
    State#state{transaction_locks = TLGraph3}.

-spec apply_lock(lock_item(), lock_kind(), transaction_id(), state()) -> state().
apply_lock(LockItem, write, Tid, State = #state{write_locks = WLocks}) ->
    State#state{ write_locks = maps:put(LockItem, Tid, WLocks) };
apply_lock(LockItem, read, Tid, State = #state{read_locks = RLocks}) ->
    OldLocks = maps:get(LockItem, RLocks, []) -- [Tid],
    State#state{ read_locks = maps:put(LockItem, [Tid | OldLocks], RLocks) }.

-spec cleanup_locks(transaction_id(), state()) -> state().
cleanup_locks(Tid, State) ->
    #state{ read_locks = RLocks,
            write_locks = WLocks} = State,
    %% Remove Tid from write locks
    WLocks1 = maps:filter(fun(_K, LockTid) -> LockTid =/= Tid end, WLocks),
    %% Remove Tid from read locks
    RLocks1 = maps:filter(fun(_K, Tids) -> Tids =/= [] end,
                maps:map(fun(_K, Tids) -> Tids -- [Tid] end, RLocks)),
    State#state{read_locks = RLocks1, write_locks = WLocks1}.

-spec mark_committed(transaction_id(), pid(), state()) -> state().
%% TODO: remove the transaction?
%% We don't want commands to be executable on the committed transaction.
%% Only commit should be executable
mark_committed(Tid, Source, State) ->
    #state{ committed_transactions = Committed } = State,
    State#state{committed_transactions = maps:put(Tid, Source, Committed)}.

-spec cleanup_transaction_locks(transaction_id(), state()) -> {ra_machine:effects(), state()}.
%% TODO make this notification smarter to not send to dead processes.
cleanup_transaction_locks(Tid, State) ->
    #state{transaction_locks = TLGraph0} = State,

    LockedTids = simple_dgraph:out_neighbours(TLGraph0, Tid),

    TLGraph1 = simple_dgraph:del_vertex(TLGraph0, Tid),

    UnlockedEffects = lists:filtermap(fun(LockedTid) ->
        %% If no lockers left
        case simple_dgraph:in_neighbours(TLGraph1, LockedTid) of
            [] ->
                %% We expect the label to be there
                {ok, LockedSource} = simple_dgraph:vertex_label(TLGraph1, LockedTid),
                case transaction_for_source(LockedSource, State) of
                    {ok, LockedTid}         ->
                        {true, {send_msg, LockedSource, {ramnesia_unlock, LockedTid}}};
                    {error, no_transaction} -> false;
                    %% A different transaction started by the locked source
                    _                       -> false
                end;
            _ ->
                false
        end
    end,
    LockedTids),
    {UnlockedEffects, State#state{ transaction_locks = TLGraph1 }}.

%% ==========================

%% Lock helpers

-spec locking_transactions(lock_item(), lock_kind(), transaction_id(), state()) -> [transaction_id()].
locking_transactions(LockItem, LockKind, Tid, State) ->
    item_locked_transactions(LockItem, LockKind, Tid, State)
    ++
    table_locking_transactions(LockItem, LockKind, Tid, State).

-spec item_locked_transactions(lock_item(), lock_kind(), transaction_id(), state()) -> [transaction_id()].
item_locked_transactions(LockItem, read, Tid, State) ->
    write_locking_transactions(LockItem, Tid, State);
item_locked_transactions(LockItem, write, Tid, State) ->
    write_locking_transactions(LockItem, Tid, State)
    ++
    read_locking_transactions(LockItem, Tid, State).

-spec table_locking_transactions(lock_item(), lock_kind(), transaction_id(), state()) -> [transaction_id()].
%% Table key is a table key.
table_locking_transactions({table, _Tab} = LockItem, LockKind, Tid, State) ->
    item_locked_transactions(LockItem, LockKind, Tid, State);
%% Record key should check the table key
table_locking_transactions({Tab, _Key}, LockKind, Tid, State) ->
    item_locked_transactions({table, Tab}, LockKind, Tid, State);
%% Global keys are never table locked
table_locking_transactions({global, _, _}, _LockKind, _Tid, _State) ->
    [].

-spec write_locking_transactions(lock_item(), transaction_id(), state()) -> [transaction_id()].
write_locking_transactions(LockItem, Tid, #state{write_locks = WLocks}) ->
    case maps:get(LockItem, WLocks, not_found) of
        not_found    -> [];
        Tid          -> [];
        DifferentTid -> [DifferentTid]
    end.

-spec read_locking_transactions(lock_item(), transaction_id(), state()) -> [transaction_id()].
read_locking_transactions(LockItem, Tid, #state{read_locks = RLocks}) ->
    case maps:get(LockItem, RLocks, not_found) of
        not_found -> [];
        []        -> [];
        [Tid]     -> [];
        Tids      -> lists:usort(Tids) -- [Tid]
    end.

%% ==========================

-spec remonitor_sources(state()) -> ra_machine:effects().
remonitor_sources(#state{transactions = Transactions}) ->
    Sources = maps:keys(Transactions),
    [{monitor, process, Source} || Source <- Sources].

-spec transaction_for_source(pid(), state()) -> {ok, transaction_id()} | {error, no_transaction}.
transaction_for_source(Source, #state{transactions = Transactions}) ->
    case maps:get(Source, Transactions, no_transaction) of
        no_transaction -> {error, no_transaction};
        Tid            -> {ok, Tid}
    end.

%% Adding effects to the apply_result.
-spec with_pre_effects(ra_machine:effects(), apply_result(T, E)) -> apply_result(T, E).
with_pre_effects(Effects0, {State, Effects, Result}) ->
    {State, Effects0 ++ Effects, Result}.

%% Functional helpers to skip ops.

%% Combine with_lock and catch_abort
-spec with_lock_catch_abort(lock_item(), lock_kind(), transaction_id(), pid(), state(),
                            fun(() -> {ok, R} | {error, E})) ->
    apply_result(R, E | locked | locked_instant | {aborted, term()}).
with_lock_catch_abort(LockItem, LockKind, Tid, Source, State, Fun) ->
    with_lock(LockItem, LockKind, Tid, Source, State, fun(State1) ->
        catch_abort(State1, Fun)
    end).

-spec with_lock(lock_item(), lock_kind(), transaction_id(), pid(), state(),
                fun((state()) -> apply_result(T, E))) ->
    apply_result(T, E | locked | locked_instant).
with_lock(LockItem, LockKind, Tid, Source, State, Fun) ->
    case lock(LockItem, LockKind, Tid, Source, State) of
        {State1, Effects, {ok, ok}} -> with_pre_effects(Effects, Fun(State1));
        LockedApplyResult           -> LockedApplyResult
    end.

-spec catch_abort(state(), fun(() -> {ok, R} | {error, E})) ->
    apply_result(R, E | {aborted, term()}).
%% TODO: clean locks on abort
catch_abort(State, Fun) ->
    try
        {State, [], Fun()}
    catch exit:{aborted, Reason} ->
        {State, [], {error, {aborted, Reason}}}
    end.

-spec with_transaction(transaction_id(), pid(),
                       state(),
                       fun(() -> apply_result(T, E))) ->
    apply_result(T, E | {wrong_transaction_id, transaction_id()} | no_transaction_for_pid).
with_transaction(Tid, Source, State, Fun) ->
    %% TODO: maybe check if transaction ID exists for other source.
    %% TODO:
    case transaction_for_source(Source, State) of
        {error, no_transaction} -> {State, [], {error, no_transaction_for_pid}};
        {ok, Tid}               -> Fun();
        {ok, DifferentTid}      -> {State, [], {error, {wrong_transaction_id, DifferentTid}}}
    end.

-spec maybe_skip_committed(transaction_id(), pid(),
                           state(),
                           fun(() -> apply_result(T, E))) ->
    apply_result(T | ok, E | {wrong_transaction_source, pid()}).
maybe_skip_committed(Tid, Source, State, Fun) ->
    #state{ committed_transactions = Committed } = State,
    case maps:get(Tid, Committed, not_found) of
        not_found ->
            Fun();
        Source ->
            {State, [], {ok, ok}};
        OtherSource ->
            {State, [], {error, {wrong_transaction_source, OtherSource}}}
    end.

-ifdef(TEST).
-include("ramnesia_machine.eunit").
-endif.
