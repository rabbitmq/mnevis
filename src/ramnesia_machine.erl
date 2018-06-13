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

-ifdef (TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type state() :: #state{}.
-type transaction_id() :: integer().

-type table() :: atom().
-type lock_item() :: {table(), term()} | {table, table()} | {global, term(), [node()]}.
-type lock_kind() :: read | write.
-type change() :: {table(), term(), lock_kind()}.

-spec init(config()) -> {state(), ra_machine:effects()}.
init(_Conf) ->
    {#state{}, []}.

-spec apply(ra_index(), command(), state()) ->
    {state(), ra_machine:effects()} | {state(), ra_machine:effects(), reply()}.

apply(_RaftIdx, {start_transaction, Source}, State) ->
    %% Cleanup stale transactions for the source.
    %% Ignore demonitor effect, because there will be a monitor effect
    {State1, _, _} = case transaction_for_source(Source, State) of
        {ok, OldTid}            -> cleanup(OldTid, Source, State);
        {error, no_transaction} -> {State, [], ok}
    end,
    start_transaction(State1, Source);

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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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
                            {State1, Effects, {ok, RecList}}
                    catch exit:{aborted, Reason} ->
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

-spec is_locked(lock_item(), lock_kind(), transaction_id(), state()) -> boolean().
is_locked(LockItem, LockKind, Tid, State) ->
    is_item_locked(LockItem, LockKind, Tid, State)
    orelse
    is_table_locked(LockItem, LockKind, Tid, State).

-spec is_item_locked(lock_item(), lock_kind(), transaction_id(), state()) -> boolean().
is_item_locked(LockItem, read, Tid, State) ->
    write_locked(LockItem, Tid, State);
is_item_locked(LockItem, write, Tid, State) ->
    write_locked(LockItem, Tid, State)
    orelse
    read_locked(LockItem, Tid, State).

-spec is_table_locked(lock_item(), lock_kind(), transaction_id(), state()) -> boolean().
%% Table key is a table key.
is_table_locked({table, _Tab} = LockItem, LockKind, Tid, State) ->
    is_item_locked(LockItem, LockKind, Tid, State);
%% Record key should check the table key
is_table_locked({Tab, _Key}, LockKind, Tid, State) ->
    is_item_locked({table, Tab}, LockKind, Tid, State);
%% Global keys are never table locked
is_table_locked({global, _, _}, _LockKind, _Tid, _State) ->
    false.

-spec write_locked(lock_item(), transaction_id(), state()) -> boolean().
write_locked(LockItem, Tid, #state{write_locks = WLocks}) ->
    case maps:get(LockItem, WLocks, not_found) of
        not_found     -> false;
        Tid           -> false;
        _DifferentTid -> true
    end.

-spec read_locked(lock_item(), transaction_id(), state()) -> boolean().
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
            _ = apply_deletes(Deletes),
            _ = apply_deletes_object(DeletesObject),
            _ = apply_writes(Writes),
            ok
        end) of
        {atomic, ok} ->
            cleanup(Tid, Source, State);
        {aborted, Reason} ->
            {State, [], {error, {aborted, Reason}}}
    end.

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

-spec with_transaction(transaction_id(), pid(), state(), fun(() -> {state(), ra_machine:effects(), reply()})) -> {state(), ra_machine:effects(), reply() | {error, {wrong_transaction_id, transaction_id()} | {error, no_transaction_for_pid}}}.
with_transaction(Tid, Source, State = #state{transactions = Transactions}, Fun) ->
    %% TODO: maybe check if transaction ID exists for other source.
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

remonitor_sources(#state{transactions = Transactions}) ->
    Sources = maps:keys(Transactions),
    [{monitor, process, Source} || Source <- Sources].

-ifdef(TEST).

% Machine behaviour:

% start_transaction:
%     cleanup any old transations for the pid
%     add the pid to transactions with unique transaction ID
%     monitor the pid
%     return the unique transaction ID

% %% Create a transaction
% [] -> Pid -> [{Pid, TID0, []}]
% %% Replace existing transaction for the process
% [{Pid,TID1, []}] -> Pid -> [{Pid,TID2, []}]
% %% Clean all locks when replacing a transaction
% [{Pid,TID1, [L1, L2]}] -> Pid -> [{Pid,TID2, []}]
% %% Keep other process transaction locks
% [{Pid1,TID1, [...]}] -> Pid -> [{Pid1,TID1, [...]},{Pid,TID2, []}]

start_transaction_test() ->
    InitState = #state{},
    Source = self(),
    {#state{transactions = #{Source := Tid}, last_transaction_id = Tid},
     [{monitor, process, Source}],
     {ok, Tid}} = ramnesia_machine:apply(none, {start_transaction, Source}, InitState).

start_transaction_cleanup_test() ->
    Source = self(),
    InitState = #state{transactions = #{Source => 1}, last_transaction_id = 1},
    {#state{transactions = #{Source := Tid}, last_transaction_id = Tid},
     [{monitor, process, Source}],
     {ok, Tid}} = ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    true = Tid =/= 1.

start_transaction_cleanup_locks_test() ->
    Source = self(),
    Tid = 1,
    Source1 = spawn(fun() -> ok end),
    Source2 = spawn(fun() -> ok end),
    Tid1 = 2,
    Tid2 = 3,
    LastTid = 3,
    InitState = #state{transactions = #{Source => Tid,
                                        Source1 => Tid1,
                                        Source2 => Tid2},
                       last_transaction_id = LastTid,
                       write_locks = #{writelock => Tid, writelock_1 => Tid1},
                       read_locks = #{readlock => [Tid],
                                      readlock_1 => [Tid, Tid1],
                                      readlock_2 => [Tid2]}},
    {#state{transactions = #{Source := NewTid, Source1 := Tid1, Source2 := Tid2},
            last_transaction_id = NewTid,
            write_locks = WLocks,
            read_locks = RLocks},
     [{monitor, process, Source}],
     {ok, NewTid}} = ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    true = Tid =/= NewTid,
    WLocks = #{writelock_1 => Tid1},
    RLocks = #{readlock_1 => [Tid1], readlock_2 => [Tid2]}.

% rollback:
%     fail if transaction does not match the pid
%     cleanup locks for transaction
%     cleanup transaction for the pid
%     demonitor the pid
%     return ok

% %% Error if no transaction
% [] -> TID, Pid -> error: no transaction
% %% Error if wrong transaction
% [{Pid, TID1, [...]}] -> TID, Pid -> error: wrong transaction
% %% Cleanup transaction
% [{Pid, TID, [...]}] -> TID, Pid -> []

rollback_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {rollback, Tid, Source, []}, InitState).

rollback_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {rollback, Tid, Source, []}, InitState).

rollback_cleanup_test() ->
    Source = self(),
    InitState = #state{},
    {State = #state{last_transaction_id = LastTid}, [{monitor, process, Source}], {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, [{demonitor, Source}], _} =
        ramnesia_machine:apply(none, {rollback, Tid, Source, []}, State),
    Expected = #state{last_transaction_id = LastTid},
    Expected = State1.

rollback_cleanup_locks_test() ->
    Source = self(),
    Tid = 1,
    Source1 = spawn(fun() -> ok end),
    Tid1 = 2,
    Source2 = spawn(fun() -> ok end),
    Tid2 = 3,
    LastTid = 3,
    InitState = #state{transactions = #{Source => Tid,
                                        Source1 => Tid1,
                                        Source2 => Tid2},
                       last_transaction_id = LastTid,
                       write_locks = #{writelock => Tid,
                                       writelock_1 => Tid1},
                       read_locks = #{readlock => [Tid],
                                      readlock_1 => [Tid, Tid1],
                                      readlock_2 => [Tid2]}},
    {State1, [{demonitor, Source}], _} =
        ramnesia_machine:apply(none, {rollback, Tid, Source, []}, InitState),
    Expected = #state{last_transaction_id = LastTid,
                      transactions = #{Source1 => Tid1, Source2 => Tid2},
                      write_locks = #{writelock_1 => Tid1},
                      read_locks = #{readlock_1 => [Tid1], readlock_2 => [Tid2]}},
    Expected = State1,
    {State2, [{demonitor, Source1}], _} =
        ramnesia_machine:apply(none, {rollback, Tid1, Source1, []}, State1),
    {State3, [{demonitor, Source2}], _} =
        ramnesia_machine:apply(none, {rollback, Tid2, Source2, []}, State2),
    State3 = #state{last_transaction_id = LastTid}.


% commit:
%     fail if transaction does not match the pid
%     do same as rollback

commit_no_transaction_error_test() ->
    mnesia:start(),
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {commit, Tid, Source, [[], [], []]}, InitState).

commit_wrong_transaction_error_test() ->
    mnesia:start(),
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {commit, Tid, Source, [[], [], []]}, InitState).

commit_cleanup_test() ->
    mnesia:start(),
    Source = self(),
    InitState = #state{},
    {State = #state{last_transaction_id = LastTid}, [{monitor, process, Source}], {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, [{demonitor, Source}], _} =
        ramnesia_machine:apply(none, {commit, Tid, Source, [[], [], []]}, State),
    Expected = #state{last_transaction_id = LastTid},
    Expected = State1.

commit_cleanup_locks_test() ->
    mnesia:start(),
    Source = self(),
    Tid = 1,
    Source1 = spawn(fun() -> ok end),
    Tid1 = 2,
    Source2 = spawn(fun() -> ok end),
    Tid2 = 3,
    LastTid = 3,
    InitState = #state{transactions = #{Source => Tid,
                                        Source1 => Tid1,
                                        Source2 => Tid2},
                       last_transaction_id = LastTid,
                       write_locks = #{writelock => Tid,
                                       writelock_1 => Tid1},
                       read_locks = #{readlock => [Tid],
                                      readlock_1 => [Tid, Tid1],
                                      readlock_2 => [Tid2]}},
    {State1, [{demonitor, Source}], _} =
        ramnesia_machine:apply(none, {commit, Tid, Source, [[], [], []]}, InitState),
    Expected = #state{last_transaction_id = LastTid,
                      transactions = #{Source1 => Tid1, Source2 => Tid2},
                      write_locks = #{writelock_1 => Tid1},
                      read_locks = #{readlock_1 => [Tid1], readlock_2 => [Tid2]}},
    Expected = State1,
    {State2, [{demonitor, Source1}], _} =
        ramnesia_machine:apply(none, {commit, Tid1, Source1, [[], [], []]}, State1),
    {State3, [{demonitor, Source2}], _} =
        ramnesia_machine:apply(none, {commit, Tid2, Source2, [[], [], []]}, State2),
    State3 = #state{last_transaction_id = LastTid}.

commit_write_test() ->
    mnesia:start(),
    mnesia:create_table(foo, []),
    %% To delete
    mnesia:dirty_write({foo, to_delete, val}),
    %% To delete during delete_object
    mnesia:dirty_write({foo, to_delete_object, val}),
    %% To skip during delete_object
    mnesia:dirty_write({foo, to_not_delete_object, not_val}),
    %% To reqrite during write
    mnesia:dirty_write({foo, to_rewrite, val}),
    Source = self(),
    InitState = #state{},
    {State1, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% Writes are of {Table, Record, LockKind} format
    Writes = [{foo, {foo, to_rewrite, other_val}, write},
              {foo, {foo, to_write, val}, write}],
    %% Deletes are of {Table, Key, LockKind} format
    Deletes = [{foo, bar, write},
               {foo, to_delete, write}],
    DeletesObject = [{foo, {foo, to_delete_object, val}, write},
                     {foo, {foo, to_not_delete_object, val}, write}],
    Expected = lists:usort([{foo, to_rewrite, other_val},
                            {foo, to_write, val},
                            {foo, to_not_delete_object, not_val}]),
    {_State2, _, _} =
        ramnesia_machine:apply(none, {commit, Tid, Source, [Writes, Deletes, DeletesObject]}, State1),
    Table = lists:usort(ets:tab2list(foo)),
    Expected = Table.

% lock:
%     fail if no transaction
%     if write-locked:
%         fail
%     if read-locked;
%         if new lock is write:
%             fail
%     add new lock
%     return ok

lock_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{table, foo}, read]}, InitState).

lock_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{table, foo}, read]}, InitState).

lock_aquire_read_test() ->
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {#state{read_locks = RLocks, write_locks = WLocks}, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{table, foo}, read]}, State),
    ExpectedW = #{},
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedW = WLocks,
    ExpectedR = RLocks.

lock_aquire_read_multiple_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2 = #state{read_locks = RLocks, write_locks = WLocks}, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{table, foo}, read]}, State1),
    ExpectedW = #{},
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedW = WLocks,
    ExpectedR = RLocks,
    {#state{read_locks = RLocks1, write_locks = WLocks1}, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, read]}, State2),
    ExpectedW1 = #{},
    ExpectedR1 = #{{table, foo} => [Tid1, Tid]},
    ExpectedW1 = WLocks1,
    ExpectedR1 = RLocks1.

lock_aquire_write_test() ->
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {#state{read_locks = RLocks, write_locks = WLocks}, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{table, foo}, write]}, State),
    WLocks = #{{table, foo} => Tid},
    RLocks = #{}.

lock_read_blocked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{foo, bar}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{foo, bar}, read]}, State2).

lock_read_blocked_by_table_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{foo, bar}, read]}, State2).

lock_write_blocked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{foo, bar}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{foo, bar}, write]}, State2).

lock_write_blocked_by_table_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{foo, bar}, write]}, State2).

lock_write_blocked_by_read_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{foo, bar}, read]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{foo, bar}, write]}, State2).

lock_write_blocked_by_table_read_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, read]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {lock, Tid, Source, [{foo, bar}, write]}, State2).

% read:
%     fail if no transaction
%     attempt lock
%     return read data

read_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {read, Tid, Source, [table, foo, read]}, InitState).

read_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {read, Tid, Source, [table, foo, read]}, InitState).

read_locked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{foo, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {read, Tid, Source, [foo, foo, read]}, State2).

read_with_write_lock_locked_by_read_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{foo, foo}, read]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {read, Tid, Source, [foo, foo, write]}, State2).

read_returns_and_aquires_lock_test() ->
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:dirty_write({foo, foo, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, [{foo, foo, val}]}} =
        ramnesia_machine:apply(none, {read, Tid, Source, [foo, foo, read]}, State),
    ExpectedR = #{{foo, foo} => [Tid]},
    ExpectedR = RLocks.

% index_read/match_object/index_match_object/all_keys:
%     fail if no transaction
%     attempt lock table
%     return read data

%% Index read

index_read_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {index_read, Tid, Source, [foo, foo, 1, read]}, InitState).

index_read_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {index_read, Tid, Source, [foo, foo, 1, read]}, InitState).

index_read_locked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {index_read, Tid, Source, [foo, foo, 1, read]}, State2).

index_read_returns_and_aquires_lock_test() ->
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:add_table_index(foo, val),
    mnesia:dirty_write({foo, foo, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, [{foo, foo, val}]}} =
        ramnesia_machine:apply(none, {index_read, Tid, Source, [foo, val, 3, read]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks.

%% Match object

match_object_no_transaction_error_test() ->
    Pattern = {foo, '_', val},
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {match_object, Tid, Source, [foo, Pattern, read]}, InitState).

match_object_wrong_transaction_error_test() ->
    Pattern = {foo, '_', val},
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {match_object, Tid, Source, [foo, Pattern, read]}, InitState).

match_object_locked_by_write_test() ->
    Pattern = {foo, '_', val},
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {match_object, Tid, Source, [foo, Pattern, read]}, State2).

match_object_returns_and_aquires_lock_test() ->
    Pattern = {foo, '_', val},
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:dirty_write({foo, foo, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, [{foo, foo, val}]}} =
        ramnesia_machine:apply(none, {match_object, Tid, Source, [foo, Pattern, read]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks.

%% Index match object

index_match_object_no_transaction_error_test() ->
    Pattern = {foo, '_', val},
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {index_match_object, Tid, Source, [foo, Pattern, 3, read]}, InitState).

index_match_object_wrong_transaction_error_test() ->
    Pattern = {foo, '_', val},
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {index_match_object, Tid, Source, [foo, Pattern, 3, read]}, InitState).

index_match_object_locked_by_write_test() ->
    Pattern = {foo, '_', val},
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {index_match_object, Tid, Source, [foo, Pattern, 3, read]}, State2).

index_match_object_returns_and_aquires_lock_test() ->
    Pattern = {foo, '_', val},
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:add_table_index(foo, val),
    mnesia:dirty_write({foo, foo, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, [{foo, foo, val}]}} =
        ramnesia_machine:apply(none, {index_match_object, Tid, Source, [foo, Pattern, 3, read]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks.

%% All keys

all_keys_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {all_keys, Tid, Source, [foo, read]}, InitState).

all_keys_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {all_keys, Tid, Source, [foo, read]}, InitState).

all_keys_locked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {all_keys, Tid, Source, [foo, read]}, State2).

all_keys_returns_and_aquires_lock_test() ->
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:dirty_write({foo, foo, val}),
    mnesia:dirty_write({foo, bar, val1}),
    mnesia:dirty_write({foo, baz, val2}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, Keys}} =
        ramnesia_machine:apply(none, {all_keys, Tid, Source, [foo, read]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks,
    Expected = lists:usort([foo, bar, baz]),
    Expected = lists:usort(Keys).

% first/last/next/prev:
%     fail if no transaction
%     attempt lock table with read lock
%     return read data

%% First

first_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {first, Tid, Source, [foo]}, InitState).

first_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {first, Tid, Source, [foo]}, InitState).

first_locked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {first, Tid, Source, [foo]}, State2).

first_returns_and_aquires_lock_test() ->
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:dirty_write({foo, bar, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, bar}} =
        ramnesia_machine:apply(none, {first, Tid, Source, [foo]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks.

%% Last

last_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {last, Tid, Source, [foo]}, InitState).

last_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {last, Tid, Source, [foo]}, InitState).

last_locked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {last, Tid, Source, [foo]}, State2).

last_returns_and_aquires_lock_test() ->
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:dirty_write({foo, bar, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, bar}} =
        ramnesia_machine:apply(none, {last, Tid, Source, [foo]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks.

%% Prev

prev_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {prev, Tid, Source, [foo, bar]}, InitState).

prev_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {prev, Tid, Source, [foo, bar]}, InitState).

prev_locked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {prev, Tid, Source, [foo, bar]}, State2).

prev_returns_and_aquires_lock_test() ->
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:dirty_write({foo, baz, val}),
    mnesia:dirty_write({foo, bar, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, baz}} =
        ramnesia_machine:apply(none, {prev, Tid, Source, [foo, bar]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks.

%% Next

next_no_transaction_error_test() ->
    InitState = #state{},
    Source = self(),
    Tid = 1,
    {InitState, [], {error, no_transaction_for_pid}} =
        ramnesia_machine:apply(none, {next, Tid, Source, [foo, bar]}, InitState).

next_wrong_transaction_error_test() ->
    Source = self(),
    Tid = 2,
    DifferentTid = 1,
    InitState = #state{transactions = #{Source => DifferentTid}, last_transaction_id = DifferentTid},
    {InitState, [], {error, {wrong_transaction_id, DifferentTid}}} =
        ramnesia_machine:apply(none, {next, Tid, Source, [foo, bar]}, InitState).

next_locked_by_write_test() ->
    InitState = #state{},
    Source = self(),
    Source1 = spawn(fun() -> ok end),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, _, {ok, Tid1}} =
        ramnesia_machine:apply(none, {start_transaction, Source1}, State),
    {State2, _, {ok, ok}} =
        ramnesia_machine:apply(none, {lock, Tid1, Source1, [{table, foo}, write]}, State1),
    %% State should not change
    {State2, _, {error, locked}} =
        ramnesia_machine:apply(none, {next, Tid, Source, [foo, bar]}, State2).

next_returns_and_aquires_lock_test() ->
    mnesia:start(),
    mnesia:delete_table(foo),
    mnesia:create_table(foo, []),
    mnesia:dirty_write({foo, baz, val}),
    mnesia:dirty_write({foo, bar, val}),
    InitState = #state{},
    Source = self(),
    {State, _, {ok, Tid}} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    %% State should not change
    {#state{read_locks = RLocks}, _, {ok, baz}} =
        ramnesia_machine:apply(none, {next, Tid, Source, [foo, bar]}, State),
    ExpectedR = #{{table, foo} => [Tid]},
    ExpectedR = RLocks.

% DOWN from a moniror:
%     rollback a transaction for the pid

down_no_transaction_test() ->
    InitState = #state{},
    Source = self(),
    %% The return value is ignored
    {InitState, [], _} =
        ramnesia_machine:apply(none, {down, Source, reason}, InitState).

down_cleanup_test() ->
    Source = self(),
    InitState = #state{},
    {State = #state{last_transaction_id = LastTid}, [{monitor, process, Source}], _} =
        ramnesia_machine:apply(none, {start_transaction, Source}, InitState),
    {State1, [{demonitor, Source}], _} =
        ramnesia_machine:apply(none, {down, Source, reason}, State),
    Expected = #state{last_transaction_id = LastTid},
    Expected = State1.

down_cleanup_locks_test() ->
    Source = self(),
    Tid = 1,
    Source1 = spawn(fun() -> ok end),
    Tid1 = 2,
    Source2 = spawn(fun() -> ok end),
    Tid2 = 3,
    LastTid = 3,
    InitState = #state{transactions = #{Source => Tid,
                                        Source1 => Tid1,
                                        Source2 => Tid2},
                       last_transaction_id = LastTid,
                       write_locks = #{writelock => Tid,
                                       writelock_1 => Tid1},
                       read_locks = #{readlock => [Tid],
                                      readlock_1 => [Tid, Tid1],
                                      readlock_2 => [Tid2]}},
    {State1, [{demonitor, Source}], _} =
        ramnesia_machine:apply(none, {down, Source, reason}, InitState),
    Expected = #state{last_transaction_id = LastTid,
                      transactions = #{Source1 => Tid1, Source2 => Tid2},
                      write_locks = #{writelock_1 => Tid1},
                      read_locks = #{readlock_1 => [Tid1], readlock_2 => [Tid2]}},
    Expected = State1,
    {State2, [{demonitor, Source1}], _} =
        ramnesia_machine:apply(none, {down, Source1, reason}, State1),
    {State3, [{demonitor, Source2}], _} =
        ramnesia_machine:apply(none, {down, Source2, reason}, State2),
    State3 = #state{last_transaction_id = LastTid}.

-endif.
