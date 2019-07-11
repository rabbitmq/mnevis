-module(mnevis_lock).

-export([init/1, lock/5, cleanup/3, monitor_down/4]).

-export([item_version_key/2]).

-record(state, {last_transaction_id :: transaction_id(),
                transactions = #{} :: #{pid() => transaction_id()},
                monitors = #{} :: #{pid() => term()},
                read_locks = #{} :: #{lock_item() => map_sets:set(transaction_id())},
                write_locks = #{} :: #{lock_item() => transaction_id()},
                reverse_read_locks = #{} :: #{transaction_id() => map_sets:set(lock_item())},
                reverse_write_locks = #{} :: #{transaction_id() => map_sets:set(lock_item())},
                transaction_locks = simple_dgraph:new() :: simple_dgraph:graph()}).

-type state() :: #state{}.
-type transaction_id() :: integer().

-type lock_item() :: {mnevis:table(), term()} | {table, mnevis:table()} | {global, term(), [node()]}.
-type lock_kind() :: read | write.

-type lock_request() :: {lock, transaction_id() | undefined, pid(),
                               lock_item(), lock_kind()}.

-type lock_result() :: {ok, transaction_id()} |
                       {error, {locked, transaction_id()}} |
                       {error, {locked_nowait, transaction_id()}} |
                       {error, no_transaction_for_pid} |
                       {error, {wrong_transaction_id, transaction_id()}}.

-export_type([transaction_id/0, lock_item/0, lock_kind/0, lock_request/0, lock_result/0]).

-define(VERSION_HASH_RESOLUTION, 1000).

-ifdef (TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec init(transaction_id()) -> state().
init(LastTid) ->
    #state{last_transaction_id = LastTid}.

-spec lock(transaction_id(), pid(), lock_item(), lock_kind(), state()) -> {lock_result(), state()}.
lock(Tid0, Source, LockItem, LockKind, State0) ->
    case Tid0 of
        undefined ->
            {Tid1, State1} = start_transaction(Source, State0),
            lock_internal(Tid1, Source, LockItem, LockKind, State1);
        _ ->
            case transaction_for_source(Source, State0) of
                {error, no_transaction} ->
                    {{error, no_transaction_for_pid}, State0};
                {ok, Tid0}               ->
                    lock_internal(Tid0, Source, LockItem, LockKind, State0);
                {ok, DifferentTid}      ->
                    {{error, {wrong_transaction_id, DifferentTid}}, State0}
            end
    end.

-spec cleanup(transaction_id(), pid(), state()) -> state().
cleanup(Tid, Source, State0) ->
    State1 = cleanup_transaction(Tid, Source, State0),
    demonitor_source(Source, State1).

-spec monitor_down(reference(), pid(), term(), state()) ->
    {transaction_id() | none, state()}.
monitor_down(_MRef, Source, _Info, State) ->
    case transaction_for_source(Source, State) of
        {ok, Tid}               ->
            {Tid, demonitor_source(Source, State)};
        {error, no_transaction} ->
            {none, State}
    end.

-spec item_version_key(mnevis:table(), term()) -> {mnevis:table(), integer()}.
item_version_key(Tab, Key) ->
    {Tab, erlang:phash2(Key, ?VERSION_HASH_RESOLUTION)}.

-spec demonitor_source(pid(), state()) -> state().
demonitor_source(Source, State = #state{monitors = Monitors}) ->
    case maps:get(Source, Monitors, none) of
        none -> ok;
        Monitor -> demonitor(Monitor, [flush])
    end,
    State#state{monitors = maps:remove(Source, Monitors)}.

-spec start_transaction(pid(), state()) -> {transaction_id(), state()}.
start_transaction(Source, State0) ->
    #state{last_transaction_id = LastTid,
           transactions = Transactions,
           monitors = Monitors} = State0,
    State1 = case transaction_for_source(Source, State0) of
        {error, no_transaction} -> State0;
        {ok, OldTid}            -> cleanup_transaction(OldTid, Source, State0)
    end,
    Tid = LastTid + 1,
    %% TODO: should we remonitor?
    State2 = demonitor_source(Source, State1),
    Monitor = monitor(process, Source),

    {Tid, State2#state{last_transaction_id = Tid,
                       transactions = maps:put(Source, Tid, Transactions),
                       monitors = maps:put(Source, Monitor, Monitors)}}.

-spec transaction_for_source(pid(), state()) -> {ok, transaction_id()} | {error, no_transaction}.
transaction_for_source(Source, #state{transactions = Transactions}) ->
    case maps:get(Source, Transactions, no_transaction) of
        no_transaction -> {error, no_transaction};
        Tid            -> {ok, Tid}
    end.

-spec lock_internal(transaction_id(), pid(), lock_item(), lock_kind(), state()) -> {lock_result(), state()}.
lock_internal(Tid, Source, LockItem0, LockKind, State0) ->
    %% If we run lock operation - the current transaction is not locked anymore
    %% It might have been restarted by timeout. We need to cleanup it's lockers
    State1 = unlock_transaction(Tid, State0),

    LockItem = normalize_lock_item(LockItem0),
    case locking_transactions(LockItem, LockKind, Tid, State1) of
        [] ->
            {{ok, Tid}, apply_lock(LockItem, LockKind, Tid, State1)};
        Tids ->
            %% To avoid cycles only wait for lower transaction Ids
            LockingTids = [ LockingTid
                            || LockingTid <- Tids,
                            LockingTid < Tid ],
            Error = case LockingTids of
                [] ->
                    {error, {locked_nowait, Tid}};
                _ ->
                    {error, {locked, Tid}}
            end,
            %% Cleanup locks for the locked transaction.
            %% The transaction will be restarted with the same ID
            State2 = cleanup_locks(Tid, State1),
            State3 = lock_transaction(Tid, Source, LockingTids, State2),
            {Error, State3}
    end.

normalize_lock_item({global, Key, _Nodes}) -> {global, Key, ignore_nodes};
normalize_lock_item(Other) -> Other.

-spec apply_lock(lock_item(), lock_kind(), transaction_id(), state()) -> state().
apply_lock(LockItem, write, Tid, State = #state{write_locks = WLocks, reverse_write_locks = RWLocks}) ->
    OldReverseLocks = maps:get(Tid, RWLocks, map_sets:new()),
    RWLocks1 = maps:put(Tid, map_sets:add_element(LockItem, OldReverseLocks), RWLocks),
    State#state{ write_locks = maps:put(LockItem, Tid, WLocks),
                 reverse_write_locks = RWLocks1};
apply_lock(LockItem, read, Tid, State = #state{read_locks = RLocks, reverse_read_locks = RRLocks}) ->

    OldLocks = maps:get(LockItem, RLocks, map_sets:new()),
    OldReverseLocks = maps:get(Tid, RRLocks, map_sets:new()),

    RLocks1 = maps:put(LockItem, map_sets:add_element(Tid, OldLocks), RLocks),
    RRLocks1 = maps:put(Tid, map_sets:add_element(LockItem, OldReverseLocks), RRLocks),

    State#state{ read_locks = RLocks1,
                 reverse_read_locks = RRLocks1 }.

-spec unlock_transaction(transaction_id(), state()) -> state().
unlock_transaction(Tid, State) ->
    %% Remove incoming edges for the transaction.
    #state{transaction_locks = TLGraph0} = State,
    OldLocks = simple_dgraph:in_edges(TLGraph0, Tid),
    TLGraph1 = simple_dgraph:del_edges(TLGraph0, OldLocks),
    State#state{transaction_locks = TLGraph1}.


-spec cleanup_locks(transaction_id(), state()) -> state().
cleanup_locks(Tid, State) ->
    #state{ read_locks = RLocks,
            write_locks = WLocks,
            reverse_read_locks = RRLocks,
            reverse_write_locks = RWLocks} = State,
    %% Remove Tid from write locks
    WriteLocked = maps:get(Tid, RWLocks, map_sets:new()),
    WLocks1 = maps:without(map_sets:to_list(WriteLocked), WLocks),
    %% Remove Tid from read locks
    ReadLocked = maps:get(Tid, RRLocks, map_sets:new()),
    RLocks1 = lists:foldl(
        fun(RLock, RLocks0) ->
            Tids = maps:get(RLock, RLocks0),
            NewTids = map_sets:del_element(Tid, Tids),
            case map_sets:size(NewTids) == 0 of
                true  -> maps:remove(RLock, RLocks0);
                false -> maps:put(RLock, NewTids, RLocks0)
            end
        end,
        RLocks,
        map_sets:to_list(ReadLocked)),
    State#state{read_locks = RLocks1, write_locks = WLocks1,
                reverse_read_locks = maps:remove(Tid, RRLocks),
                reverse_write_locks = maps:remove(Tid, RWLocks)}.

-spec lock_transaction(transaction_id(), pid(), [transaction_id()], state()) -> state().
lock_transaction(LockedTid, Source, LockingTids0, State) ->
    LockingTids = [ LockingTid || LockingTid <- LockingTids0,
                                  LockingTid < LockedTid ],

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

%% ==========================

%% Lock check helpers

-type lock_search() :: single | all.

-spec locking_transactions(lock_item(), lock_kind(), transaction_id(), state()) -> [transaction_id()].
locking_transactions(LockItem, LockKind, Tid, State) ->
    Locking = case LockItem of
        {table, _} ->
            locking_transactions(LockItem, LockKind, single, Tid, State)
            ++
            locking_transactions(LockItem, LockKind, all, Tid, State);
        {Tab, _Key} ->
            locking_transactions(LockItem, LockKind, single, Tid, State)
            ++
            locking_transactions({table, Tab}, LockKind, single, Tid, State);
        {global, _, _} ->
            locking_transactions(LockItem, LockKind, single, Tid, State)
    end,
    lists:usort(Locking).

-spec locking_transactions(lock_item(), lock_kind(), lock_search(),
                           transaction_id(), state()) -> [transaction_id()].
locking_transactions(LockItem, read, LockSearch, Tid, State) ->
    write_locking_transactions(LockItem, LockSearch, Tid, State);
locking_transactions(LockItem, write, LockSearch, Tid, State) ->
    write_locking_transactions(LockItem, LockSearch, Tid, State)
    ++
    read_locking_transactions(LockItem, LockSearch, Tid, State).

-spec write_locking_transactions(lock_item(), lock_search(),
                                 transaction_id(), state()) -> [transaction_id()].
write_locking_transactions(LockItem, single, Tid, #state{write_locks = WLocks}) ->
    case maps:get(LockItem, WLocks, not_found) of
        not_found    -> [];
        Tid          -> [];
        DifferentTid -> [DifferentTid]
    end;
write_locking_transactions({table, Tab}, all, Tid, #state{write_locks = WLocks}) ->
    Intersecting = maps:filter(fun
                                ({Table, _}, TransactionId) ->
                                    Table =:= Tab andalso TransactionId =/= Tid;
                                ({global, _, _}, _) ->
                                    false
                               end,
                               WLocks),
    maps:values(Intersecting).

-spec read_locking_transactions(lock_item(), lock_search(),
                                transaction_id(), state()) -> [transaction_id()].
read_locking_transactions(LockItem, single, Tid, #state{read_locks = RLocks}) ->
    case maps:get(LockItem, RLocks, not_found) of
        not_found -> [];
        MapSet when is_map(MapSet) ->
            WithoutTid = map_sets:del_element(Tid, MapSet),
            case map_sets:size(WithoutTid) == 0 of
                true  -> [];
                false -> map_sets:to_list(WithoutTid)
            end
    end;
read_locking_transactions({table, Tab}, all, Tid, #state{read_locks = RLocks}) ->
    maps:fold(fun
                ({Table, _}, MapSet, Locking) when Table =:= Tab ->
                    WithoutTid = map_sets:del_element(Tid, MapSet),
                    case map_sets:size(WithoutTid) == 0 of
                        true  -> Locking;
                        false -> map_sets:to_list(WithoutTid) ++ Locking
                    end;
                (_, _, Locking) ->
                    Locking
              end,
              [],
              RLocks).

-spec cleanup_transaction(transaction_id(), pid(), state()) -> state().
cleanup_transaction(Tid, Source, State0) ->
    #state{ transactions = Transactions} = State0,
    State1 = cleanup_locks(Tid, State0),
    State2 = cleanup_transaction_locks(Tid, State1),
    State2#state{transactions = maps:remove(Source, Transactions)}.

-spec cleanup_transaction_locks(transaction_id(), state()) -> state().
cleanup_transaction_locks(Tid, State) ->
    #state{transaction_locks = TLGraph0} = State,

    LockedTids = simple_dgraph:out_neighbours(TLGraph0, Tid),

    TLGraph1 = simple_dgraph:del_vertex(TLGraph0, Tid),

    lists:foreach(fun(LockedTid) ->
        %% If no lockers left
        case simple_dgraph:in_neighbours(TLGraph1, LockedTid) of
            [] ->
                %% We expect the label to be there
                {ok, LockedSource} = simple_dgraph:vertex_label(TLGraph1, LockedTid),
                case transaction_for_source(LockedSource, State) of
                    {ok, LockedTid}         ->
                        LockedSource ! {mnevis_unlock, LockedTid};
                    %% We expect dead transactions to be claned up by the monitor.
                    {error, no_transaction} -> ok;
                    %% A different transaction started by the locked source
                    _                       -> ok
                end;
            _ ->
                ok
        end
    end,
    LockedTids),
    State#state{ transaction_locks = TLGraph1 }.

-ifdef(TEST).
-include("mnevis_lock.eunit").
-endif.