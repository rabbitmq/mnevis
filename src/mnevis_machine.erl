-module(mnevis_machine).
-behaviour(ra_machine).

-include_lib("ra/include/ra.hrl").

-export([
         init/1,
         apply/3,
         state_enter/2,
         snapshot_module/0]).

-export([start_new_locker/2]).

-type config() :: map().
-type command() :: term().

-type reply() :: {ok, term()} | {error, term()}.
-type reply(T) :: {ok, T} | {error, term()}.
-type reply(T, E) :: {ok, T} | {error, E}.


-record(state, {locker_status = down,
                locker_pid,
                locker_term = 0}).

-ifdef (TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type state() :: #state{}.
-type transaction_id() :: integer().
-type locker_term() :: integer().
%% TODO: pid/term order: create a shared type
-type transaction() :: {locker_term(), transaction_id()}.

-type table() :: atom().
-type lock_kind() :: read | write.
-type change() :: {table(), term(), lock_kind()}.

-type apply_result(T, Err) :: {state(), reply(T, Err), ra_machine:effects()}.

-record(committed_transaction, { transaction :: {locker_term(), transaction_id()},
                                 value }).

%% Ra machine callbacks

-spec init(config()) -> state().
init(_Conf) ->
    %% TODO move committed transaction creation to the create_cluster function
    %% separate start and recovery
    case create_committed_transaction_table() of
        {atomic, ok} -> ok;
        {aborted,{already_exists,committed_transaction}} -> ok;
        Other -> error({cannot_create_committed_transaction_table, Other})
    end,
    ok = mnevis_lock_proc:create_locker_cache(),
    #state{}.

-spec state_enter(ra_server:ra_state() | eol, state()) -> ra_machine:effects().
state_enter(leader, State) ->
    start_new_locker_effects(State);
state_enter(State, _) ->
    error_logger:info_msg("mnevis machine enter state ~p~n", [State]),
    [].

-spec start_new_locker_effects(state()) -> ra_machine:effects().
start_new_locker_effects(#state{locker_pid = LockerPid, locker_term = LockerTerm}) ->
    [{mod_call, mnevis_machine, start_new_locker, [LockerPid, LockerTerm]}].

-spec snapshot_module() -> module().
snapshot_module() ->
    mnevis_snapshot.

-spec apply(map(), command(), state()) ->
    {state(), reply(), ra_machine:effects()}.
apply(Meta, {commit, Transaction, [Writes, Deletes, DeletesObject]}, State)  ->
    with_valid_locker(Transaction, State,
        fun() ->
            %% TODO: check valid locker
            Result = commit(Transaction, Writes, Deletes, DeletesObject),
            case Result of
                {ok, ok} ->
                    {State, Result, snapshot_effects(Meta, State)};
                _ ->
                    {State, Result, []}
            end
        end);

apply(_Meta, {read, Transaction, [Tab, Key]}, State) ->
    with_transaction(Transaction, State,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_read(Tab, Key)}
                end)
        end);

apply(_Meta, {index_read, Transaction, [Tab, SecondaryKey, Pos]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    %% TODO: catch_abort
                    {ok, mnesia:dirty_index_read(Tab, SecondaryKey, Pos)}
                end)
        end);

apply(_Meta, {match_object, Transaction, [Tab, Pattern]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_match_object(Tab, Pattern)}
                end)
        end);

apply(_Meta, {index_match_object, Transaction, [Tab, Pattern, Pos]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_index_match_object(Tab, Pattern, Pos)}
                end)
        end);

apply(_Meta, {all_keys, Transaction, [Tab]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_all_keys(Tab)}
                end)
        end);

apply(_Meta, {first, Transaction, [Tab]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_first(Tab)}
                end)
        end);

apply(_Meta, {last, Transaction, [Tab]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_last(Tab)}
                end)
        end);

apply(_Meta, {prev, Transaction, [Tab, Key]}, State0) ->
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

apply(_Meta, {next, Transaction, [Tab, Key]}, State0) ->
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
    {State, {ok, mnesia:create_table(Tab, Opts)}, []};

apply(_Meta, {delete_table, Tab}, State) ->
    {State, {ok, mnesia:delete_table(Tab)}, []};

apply(_Meta, {down, Pid, _Reason}, State = #state{locker_status = LockerStatus,
                                                          locker_pid = LockerPid}) ->
    case {Pid, LockerStatus} of
        {LockerPid, up} ->
            {State#state{locker_status = down}, ok, start_new_locker_effects(State)};
        _ -> {State, ok, []}
    end;

apply(_Meta, {locker_up, Pid, Term},
              State = #state{locker_status = _LockerStatus}) ->
error_logger:info_msg("mnevis locker up ~p", [{Pid, Term}]),
    %% TODO: change locker status to something more sensible
    ok = mnevis_lock_proc:update_locker_cache(Pid, Term),
    {State#state{locker_status = up,
                 locker_pid = Pid,
                 locker_term = Term},
     confirm,
     [{monitor, process, Pid}]};
apply(_Meta, {which_locker, OldLocker},
              State = #state{locker_status = up,
                             locker_pid = LockerPid,
                             locker_term = LockerTerm}) when is_pid(LockerPid) ->
    case OldLocker of
        none ->
            {State, {ok, {LockerPid, LockerTerm}}, []};
        {_, OldTerm} when OldTerm < LockerTerm ->
            {State, {ok, {LockerPid, LockerTerm}}, []};
        {LockerPid, LockerTerm} ->
            {State, {error, locker_up_to_date}, start_new_locker_effects(State)};
        _ ->
            %% TODO: what to do?
            {State, {error, {invalid_locker, OldLocker, {LockerPid, LockerTerm}}}, []}
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
    mnesia:create_table(committed_transaction,
        [{attributes, record_info(fields, committed_transaction)},
         {record_name, committed_transaction},
         {type, ordered_set}]).

-spec commit(transaction(), [change()], [change()], [change()]) -> reply(ok).
commit(Transaction, Writes, Deletes, DeletesObject) ->
    Res = mnesia:transaction(fun() ->
        case mnesia:read(committed_transaction, Transaction) of
            [] ->
                ok = save_committed_transaction(Transaction),
                _ = apply_deletes(Deletes),
                _ = apply_writes(Writes),
                _ = apply_deletes_object(DeletesObject),
                ok;
            %% Transaction is already committed.
            [{committed_transaction, Transaction, committed}] ->
                ok
        end
    end),
    case Res of
        {atomic, ok} ->
            {ok, ok};
        {aborted, Reason} ->
            %% TODO: maybe clean transaction here
            {error, {aborted, Reason}}
    end.

-spec snapshot_effects(map(), state()) -> ra_machine:effects().
snapshot_effects(#{index := RaftIdx}, State) ->
    [{release_cursor, RaftIdx, State}].

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
with_valid_locker({LockerTerm, _}, State = #state{locker_term = CurrentLockerTerm}, Fun) ->
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

-ifdef(TEST).
% -include("mnevis_machine.eunit").
-endif.

%% ==============================

-spec start_new_locker(pid(), locker_term()) -> ok.
start_new_locker(OldLockerPid, OldTerm) ->
    case is_pid(OldLockerPid) of
        true  -> mnevis_lock_proc:stop(OldLockerPid);
        false -> ok
    end,
    {ok, _} = mnevis_lock_proc:start(OldTerm + 1),
    ok.
