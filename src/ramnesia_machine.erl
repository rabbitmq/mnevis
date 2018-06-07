-module(ramnesia_machine).
-behaviour(ra_machine).

-include_lib("ra/include/ra.hrl").

-export([
         init/1,
         apply/3,
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
                read_locks = #{},
                write_locks = #{}}).

-type state() :: map().
-type transaction_id() :: integer().

-type table() :: atom().
-type lock_item() :: {table(), term()} | {table, table()} | {global, term(), [node()]}.
-type lock_kind() :: read | write.
-type change() :: {table(), term(), lock_kind()}.

-spec init(config()) -> {state(), ra_machine:effects()}.
init(_Conf) ->
    {#{}, []}.

-spec apply(ra_index(), command(), state()) ->
    {state(), ra_machine:effects()} | {state(), ra_machine:effects(), reply()}.

apply(_RaftIdx, {start_transaction, Source}, State) ->
    %% Cleanup stale transactions for the source.
    {State1, Effects, _} = case transaction_for_source(Source, State) of
        %% TODO: cleanup monitors
        {ok, OldTid}            -> cleanup(OldTid, Source, State);
        {error, no_transaction} -> {State, [], ok}
    end,
    {State2, Effects1, Reply} = start_transaction(State1, Source),
    %% TODO: how do I apply demonitor and monitor?
    {State2, Effects ++ Effects1, Reply};

apply(_RaftIdx, {rollback, Tid, Source, []}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            cleanup(Tid, Source, State)
        end);

apply(_RaftIdx, {commit, Tid, Source, [Writes, Deletes, DeletesObject]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            commit(Tid, Source, Writes, Deletes, DeletesObject, State)
        end);

apply(_RaftIdx, {lock, Tid, Source, [LockItem, LockKind]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            lock(LockItem, LockKind, Tid, State)
        end);

apply(_RaftIdx, {read, Tid, Source, [Tab, Key, LockKind]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({Tab, Key}, LockKind, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_read(Tab, Key) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {index_read, Tid, Source, [Tab, SecondaryKey, Pos, LockKind]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, LockKind, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_index_read(Tab, SecondaryKey, Pos) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {match_object, Tid, Source, [Tab, Pattern, LockKind]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, LockKind, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_match_object(Tab, Pattern) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {index_match_object, Tid, Source, [Tab, Pattern, Pos, LockKind]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, LockKind, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_index_match_object(Tab, Pattern, Pos) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {all_keys, Tid, Source, [Tab, LockKind]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, LockKind, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_all_keys(Tab) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {first, Tid, Source, [Tab]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, read, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_first(Tab) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {last, Tid, Source, [Tab]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, read, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_last(Tab) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {prev, Tid, Source, [Tab, Key]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, read, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_prev(Tab, Key) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {next, Tid, Source, [Tab, Key]}, State) ->
    with_transaction(Tid, Source, State,
        fun() ->
            case lock({table, Tab}, read, Tid, State) of
                {State1, Effects, {ok, ok}} ->
                    try mnesia:dirty_next(Tab, Key) of
                        RecList ->
                            {State1, Effects, RecList}
                    catch error:{aborted, Reason} ->
                        {State, [], {error, {aborted, Reason}}}
                    end;
                Other -> Other
            end
        end);

apply(_RaftIdx, {down, Source, _Reason}, State) ->
    case transaction_for_source(Source, State) of
        {ok, Tid}               -> cleanup(Tid, Source, State);
        {error, no_transaction} -> {State, [], ok}
    end.

-spec leader_effects(state()) -> ra_machine:effects().
leader_effects(State) -> remonitor_sources(State).

-spec eol_effects(state()) -> ra_machine:effects().
eol_effects(_State) -> [].

-spec tick(TimeMs :: integer(), state()) -> ra_machine:effects().
tick(_Time, _State) -> [].

-spec overview(state()) -> map().
overview(_State) -> #{}.

-spec lock(lock_item(), lock_kind(), transaction_id(), state()) ->
        {state(), ra_machine:effects(), reply(ok, locked)}.
lock(LockItem, LockKind, Tid, State) ->
    case is_locked(LockItem, LockKind, Tid, State) of
        true  -> {State, [], {error, locked}};
        false -> {apply_lock(LockItem, LockKind, Tid, State), [], {ok, ok}}
    end.

-spec apply_lock(lock_item(), lock_kind(), transaction_id(), state()) -> state().
apply_lock(LockItem, write, Tid, State = #state{write_locks = WLocks}) ->
    State#state{ write_locks = maps:put(LockItem, Tid, WLocks) };
apply_lock(LockItem, read, Tid, State = #state{read_locks = RLocks}) ->
    OldLocks = maps:get(LockItem, RLocks, []) -- [Tid],
    State#state{ read_locks = maps:put(LockItem, [Tid | OldLocks], RLocks) }.

-spec is_locked(lock_item(), lock_kind(), transaction_id(), state()) -> locked | free.
is_locked(LockItem, read, Tid, State) ->
    write_locked(LockItem, Tid, State);
is_locked(LockItem, write, Tid, State) ->
    write_locked(LockItem, Tid, State) orelse read_locked(LockItem, Tid, State).

-spec write_locked(lock_item(), transaction_id(), state()) -> true | false.
write_locked(LockItem, Tid, #state{write_locks = WLocks}) ->
    case maps:get(LockItem, WLocks, not_found) of
        not_found     -> false;
        Tid           -> false;
        _DifferentTid -> true
    end.

-spec read_locked(lock_item(), transaction_id(), state()) -> true | false.
read_locked(LockItem, Tid, #state{read_locks = RLocks}) ->
    case maps:get(LockItem, RLocks, not_found) of
        not_found -> false;
        []        -> false;
        [Tid]     -> false;
        _         -> true
    end.

-spec commit(transaction_id(), pid(), [change()], [change()], [change()], state()) ->
        {state(), ra_machine:effects(), reply(ok)}.
commit(Tid, Source, Writes, Deletes, DeletesObject, State) ->
    case mnesia:transaction(fun() ->
            apply_deletes(Deletes),
            apply_deletes_object(DeletesObject),
            apply_writes(Writes),
            ok
        end) of
        {atomic, ok} ->
            cleanup(Tid, Source, State);
        {aborted, Reason} ->
            {State, [], {error, {aborted, Reason}}}
    end.

-spec apply_deletes([change()]) -> ok.
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

-spec cleanup(transaction_id(), pid(), state()) -> {state(), ra_machine:effects(), reply(ok)}.
cleanup(Tid, Source, State) ->
    #state{ transactions = Transactions,
            read_locks = RLocks,
            write_locks = WLocks } = State,
    %% Remove source from transactions.
    Transactions1 = maps:remove(Source, Transactions),
    %% Remove Tid from write locks
    WLocks1 = maps:filter(fun(_K, LockTid) -> LockTid =/= Tid end, WLocks),
    %% Remove Tid from read locks
    RLocks1 = maps:filter(fun(_K, Tids) -> Tids =/= [] end,
                maps:map(fun(_K, Tids) -> Tids -- [Tid] end, RLocks)),
    {State#state{transactions = Transactions1,
                 read_locks = RLocks1,
                 write_locks = WLocks1},
     [{demonitor, Source}],
     {ok, ok}}.

-spec with_transaction(transaction_id(), pid(), state(),
                       fun(() -> {state(), ra_machine:effects(), reply()})) ->
        {state(),
         ra_machine:effects(),
         reply()
         | {error, {wrong_transaction_id, transaction_id()}
         | {error, no_transaction_for_pid}}}.
with_transaction(Tid, Source, State = #state{transactions = Transactions}, Fun) ->
    %% TODO: maybe check if transaction ID exists.
    case maps:get(Source, Transactions, not_found) of
        not_found    -> {State, [], {error, no_transaction_for_pid}};
        Tid          -> Fun();
        DifferentTid -> {State, [], {error, {wrong_transaction_id, DifferentTid}}}
    end.

-spec transaction_for_source(pid(), state()) -> {ok, transaction_id()} | {error, no_transaction}.
transaction_for_source(Source, #state{transactions = Transactions}) ->
    case maps:get(Source, Transactions, no_transaction) of
        no_transaction -> {error, no_transaction};
        Tid            -> {ok, Tid}
    end.

-spec start_transaction(state(), pid()) -> {state(), ra_machine:effects(), reply(transaction_id())}.
start_transaction(State, Source) ->
    #state{last_transaction_id = LastTid,
           transactions = Transactions} = State,
    Tid = LastTid + 1,

    {State#state{last_transaction_id = Tid,
                 transactions = maps:put(Source, Tid, Transactions)},
     [{monitor, process, Source}],
     {ok, Tid}}.

remonitor_sources(#{locks := CurrentLocks}) ->
    Sources = lock_sources(CurrentLocks),
    [{monitor, process, Source} || Source <- Sources].

lock_sources(CurrentLocks) ->
    lists:usort([Source || {Source, _Lock} <- CurrentLocks]).
