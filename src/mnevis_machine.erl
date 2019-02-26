-module(mnevis_machine).
-behaviour(ra_machine).

-include_lib("ra/include/ra.hrl").

-export([
         init/1,
         apply/3,
         state_enter/2,
         snapshot_module/0]).

-record(state, {locker_status = down,
                locker = {0, none} :: mnevis_lock_proc:locker() | {0, none}}).

-type state() :: #state{}.

-type config() :: map().

-type reply() :: {ok, term()} | {error, term()}.
-type reply(T, E) :: {ok, T} | {error, E}.
-type apply_result(T, Err) :: {state(), reply(T, Err), ra_machine:effects()}.

-type transaction() :: mnevis_context:transaction().

-type command() :: {commit, transaction(),
                            {[mnevis_context:item()],
                             [mnevis_context:delete_item()],
                             [mnevis_context:item()]}} |
                   {prev, transaction(), {mnevis:table(), term()}} |
                   {next, transaction(), {mnevis:table(), term()}} |
                   {create_table, mnevis:table(), [term()]} |
                   {delete_table, mnevis:table()} |
                   {down, pid(), term()} |
                   {locker_up, mnevis_lock_proc:locker()} |
                   {which_locker, mnevis_lock_proc:locker()}.

-ifdef (TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(committed_transaction, { transaction :: mnevis_context:transaction(),
                                 value }).

%% Ra machine callbacks

-spec init(config()) -> state().
init(_Conf) ->
    %% TODO move committed transaction creation to the create_cluster function
    %% separate start and recovery
    create_committed_transaction_table(),

    mnevis_read:create_versions_table(),
    ok = mnevis_lock_proc:create_locker_cache(),
    #state{}.

-spec state_enter(ra_server:ra_state() | eol, state()) -> ra_machine:effects().
state_enter(leader, State) ->
    start_new_locker_effects(State);
state_enter(State, _) ->
    error_logger:info_msg("mnevis machine enter state ~p~n", [State]),
    [].

-spec start_new_locker_effects(state()) -> ra_machine:effects().
start_new_locker_effects(#state{locker = Locker}) ->
    [{mod_call, mnevis_lock_proc, start_new_locker, [Locker]}].

-spec snapshot_module() -> module().
snapshot_module() ->
    mnevis_snapshot.

-spec apply(map(), command(), state()) ->
    {state(), reply(), ra_machine:effects()}.
apply(Meta, {commit, Transaction, {Writes, Deletes, DeletesObject}}, State)  ->
    with_valid_locker(Transaction, State,
        fun() ->
            Result = commit(Transaction, Writes, Deletes, DeletesObject),
            %% TODO: return committed/skipped
            case Result of
                {ok, committed} ->
                    {State, {ok, ok}, snapshot_effects(Meta, State)};
                {ok, skipped} ->
                    {State, {ok, ok}, []};
                _ ->
                    {State, Result, []}
            end
        end);
apply(_Meta, {prev, Transaction, {Tab, Key}}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    try {ok, mnesia:dirty_prev(Tab, Key)}
                    catch
                        exit:{aborted, {badarg, [Tab, Key]}} ->
                            case mnesia:table_info(Tab, type) of
                                ordered_set ->
                                    {error,
                                        {key_not_found,
                                         closest_prev(Tab, Key)}};
                                _ ->
                                    {error,
                                        {key_not_found,
                                         mnesia:dirty_last(Tab)}}
                            end
                    end
                end)
        end);
apply(_Meta, {next, Transaction, {Tab, Key}}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    try {ok, mnesia:dirty_next(Tab, Key)}
                    catch
                        exit:{aborted, {badarg, [Tab, Key]}} ->
                            case mnesia:table_info(Tab, type) of
                                ordered_set ->
                                    {error,
                                        {key_not_found,
                                         closest_next(Tab, Key)}};
                                _ ->
                                    {error,
                                        {key_not_found,
                                         mnesia:dirty_first(Tab)}}
                            end
                    end
                end)
        end);
%% TODO: return type for create_table
apply(_Meta, {create_table, Tab, Opts}, State) ->
    Result = case mnesia:create_table(Tab, Opts) of
        {atomic, ok} ->
            mnevis_read:init_version(Tab),
            {atomic, ok};
        Res ->
            Res
    end,
    {State, {ok, Result}, []};
apply(_Meta, {delete_table, Tab}, State) ->
    Result = mnesia:delete_table(Tab),
    {State, {ok, Result}, []};
apply(_Meta, {down, Pid, _Reason}, State = #state{locker = {_Term, LockerPid}}) ->
    case Pid of
        LockerPid ->
            {State#state{locker_status = down}, ok, start_new_locker_effects(State)};
        _ ->
            {State, ok, []}
    end;
apply(_Meta, {locker_up, {Term, Pid} = Locker},
              State = #state{locker = {CurrentLockerTerm, _CurrentLockerPid}}) ->
    case Term >= CurrentLockerTerm of
        true ->
            %% TODO: change locker status to something more sensible
            ok = mnevis_lock_proc:update_locker_cache(Locker),
            {State#state{locker_status = up,
                         locker = Locker},
             confirm,
             [{monitor, process, Pid}]};
        false ->
            {State, reject, []}
    end;
apply(_Meta, {which_locker, OldLocker},
              State = #state{locker_status = up,
                             locker = {LockerTerm, _} = CurrentLocker}) ->
    case OldLocker of
        none ->
            {State, {ok, CurrentLocker}, []};
        CurrentLocker ->
            %% TODO: monitor that
            {State, {error, locker_up_to_date}, start_new_locker_effects(State)};
        {OldTerm, _} when OldTerm < LockerTerm ->
            {State, {ok, CurrentLocker}, []};
        _ ->
            %% TODO: what to do?
            {State, {error, {invalid_locker, OldLocker, CurrentLocker}}, []}
    end;
apply(_Meta, {which_locker, _OldLocker}, State = #state{locker_status = down}) ->
    {State, {error, locker_down}, []};
%% TODO: flush_locker_transactions request
%% TODO: cleanup term transactions for previous terms
apply(_Meta, Unknown, State) ->
    error_logger:error_msg("Unknown command ~p~n", [Unknown]),
    {State, {error, {unknown_command, Unknown}}, []}.

%% ==========================

%% Top level helpers

create_committed_transaction_table() ->
    CreateResult = mnesia:create_table(committed_transaction,
                                       [{attributes, record_info(fields, committed_transaction)},
                                        {record_name, committed_transaction},
                                        {type, ordered_set}]),
    case CreateResult of
        {atomic, ok} -> ok;
        {aborted,{already_exists,committed_transaction}} -> ok;
        Other -> error({cannot_create_committed_transaction_table, Other})
    end.

-spec commit(transaction(), [mnevis_context:item()],
                            [mnevis_context:delete_item()],
                            [mnevis_context:item()]) ->
    {ok, committed} | {ok, skipped} | {error, {aborted, term()}}.
commit(Transaction, Writes, Deletes, DeletesObject) ->
    Res = mnesia:transaction(fun() ->
        case mnesia:read(committed_transaction, Transaction) of
            [] ->
                ok = save_committed_transaction(Transaction),
                ok = update_table_versions(Writes, Deletes, DeletesObject),
                _ = apply_deletes(Deletes),
                _ = apply_writes(Writes),
                _ = apply_deletes_object(DeletesObject),
                committed;
            %% Transaction is already committed.
            [{committed_transaction, Transaction, committed}] ->
                skipped
        end
    end),
    case Res of
        {atomic, Result} ->
            {ok, Result};
        {aborted, Reason} ->
            {error, {aborted, Reason}}
    end.

update_table_versions(Writes, Deletes, DeletesObject) ->
    Tabs = lists:usort([Tab || {Tab, _, _} <- Writes ++ Deletes ++ DeletesObject]),
    lists:foreach(
        fun(Tab) ->
            %% This function may crash
            %% Version should be in the database at this point
            ok = mnevis_read:update_version(Tab)
        end,
        Tabs).


-spec snapshot_effects(map(), state()) -> ra_machine:effects().
snapshot_effects(#{index := RaftIdx}, State) ->
    [{release_cursor, RaftIdx, State}].

%% ==========================

%% Mnesia operations

-spec apply_deletes([mnevis_context:delete_item()]) -> [ok].
apply_deletes(Deletes) ->
    [ok = mnesia:delete(Tab, Key, LockKind)
     || {Tab, Key, LockKind} <- Deletes].

-spec apply_deletes_object([mnevis_context:item()]) -> [ok].
apply_deletes_object(DeletesObject) ->
    [ok = mnesia:delete_object(Tab, Rec, LockKind)
     || {Tab, Rec, LockKind} <- DeletesObject].

-spec apply_writes([mnevis_context:item()]) -> [ok].
apply_writes(Writes) ->
    [ok = mnesia:write(Tab, Rec, LockKind)
     || {Tab, Rec, LockKind} <- Writes].

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

%% TODO: optimise transaction numbers
-spec save_committed_transaction(transaction()) -> ok.
save_committed_transaction(Transaction) ->
    ok = mnesia:write({committed_transaction, Transaction, committed}).

%% TODO: store committed transactions in memory
-spec transaction_recorded_as_committed(transaction()) -> boolean().
transaction_recorded_as_committed(Transaction) ->
    Res = mnesia:dirty_read(committed_transaction, Transaction),
    case Res of
        [] ->
            false;
        [{committed_transaction, Transaction, committed}] ->
            true
    end.

%% ==========================

%% Functional helpers to skip ops.

-spec with_valid_locker(transaction(), state(),
                       fun(() -> apply_result(T, E))) -> apply_result(T, E).
with_valid_locker({_Tid, {LockerTerm, _LockerPid}},
                  State = #state{locker = {CurrentLockerTerm, _}}, Fun) ->
    case LockerTerm of
        CurrentLockerTerm -> Fun();
        _ -> {State, {error, {aborted, wrong_locker_term}}, []}
    end.

-spec catch_abort(fun(() -> reply(R, E))) -> reply(R, E | {aborted, term()}).
catch_abort(Fun) ->
    try
        Fun()
    catch exit:{aborted, Reason} ->
        {error, {aborted, Reason}}
    end.

-spec with_transaction(transaction(), state(),
                       fun(() -> reply(T, E))) -> apply_result(T, E).
with_transaction(Transaction, State, Fun) ->
    with_valid_locker(Transaction, State,
        fun() ->
            case transaction_recorded_as_committed(Transaction) of
                %% This is a log replay and the transaction is already committed.
                %% Result will not be received by any client.
                true  -> {State, {error, {transaction_committed, Transaction}}, []};
                false -> {State, Fun(), []}
            end
        end).

%% ==============================

-ifdef(TEST).
-include("mnevis_machine.eunit").
-endif.