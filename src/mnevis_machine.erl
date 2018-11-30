-module(mnevis_machine).
-behaviour(ra_machine).

-include_lib("ra/include/ra.hrl").

-export([
         init/1,
         apply/4,
         state_enter/2,
         leader_effects/1,
         eol_effects/1,
         tick/2,
         overview/1,
         snapshot_module/0]).

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

-type table() :: atom().
-type lock_item() :: {table(), term()} | {table, table()} | {global, term(), [node()]}.
-type lock_kind() :: read | write.
-type change() :: {table(), term(), lock_kind()}.

-type apply_result() :: {state(), ra_machine:effects(), reply()}.

-type apply_result(T) :: {state(), ra_machine:effects(), reply(T)}.

-type apply_result(T, Err) :: {state(), ra_machine:effects(), reply(T, Err)}.

-record(committed_transaction, { transaction :: {locker_term(), transaction_id()},
                                 value }).
-define(IS_TID(T), is_integer(T)).

%% Ra machine callbacks

-spec init(config()) -> state().
init(_Conf) ->
    case create_committed_transaction_table() of
        {atomic, ok} -> ok;
        {aborted,{already_exists,committed_transaction}} -> ok;
        Other -> error({cannot_create_committed_transaction_table, Other})
    end,
    ok = create_locker_cache(),
    #state{}.

-spec state_enter(ra_server:ra_state() | eol, state()) -> ra_machine:effects().
state_enter(leader, State) ->
    error_logger:info_msg("mnevis machine leader"),
    start_new_locker_effects(State);
state_enter(State, _) ->
    error_logger:info_msg("mnevis machine enter state ~p~n", [State]),
    [].

-spec start_new_locker_effects(state()) -> ra_machine:effects().
start_new_locker_effects(#state{}) ->
    [{mod_call, mnevis_lock_proc, start, []}].

-spec apply(map(), command(), ra_machine:effects(), state()) ->
    {state(), ra_machine:effects(), reply()}.
apply(Meta, Command, Effects0, State) ->
    with_pre_effects(Effects0, apply_command(Meta, Command, State)).

-spec snapshot_module() -> module().
snapshot_module() ->
    mnevis_snapshot.

apply_command(Meta, {commit, Transaction, [Writes, Deletes, DeletesObject]}, State)  ->
    with_valid_locker(Transaction, State,
        fun() ->
            %% TODO: check valid locker
            Result = commit(Transaction, Writes, Deletes, DeletesObject, State),
            case Result of
                {ok, ok} ->
                    {State, snapshot_effects(Meta, State), Result};
                _ ->
                    {State, [], Result}
            end
        end);

apply_command(_Meta, {read, Transaction, [Tab, Key]}, State) ->
    with_transaction(Transaction, State,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_read(Tab, Key)}
                end)
        end);

apply_command(_Meta, {index_read, Transaction, [Tab, SecondaryKey, Pos, LockKind]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    %% TODO: catch_abort
                    {ok, mnesia:dirty_index_read(Tab, SecondaryKey, Pos)}
                end)
        end);

apply_command(_Meta, {match_object, Transaction, [Tab, Pattern, LockKind]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_match_object(Tab, Pattern)}
                end)
        end);

apply_command(_Meta, {index_match_object, Transaction, [Tab, Pattern, Pos, LockKind]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_index_match_object(Tab, Pattern, Pos)}
                end)
        end);

apply_command(_Meta, {all_keys, Transaction, [Tab, LockKind]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_all_keys(Tab)}
                end)
        end);

apply_command(_Meta, {first, Transaction, [Tab]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_first(Tab)}
                end)
        end);

apply_command(_Meta, {last, Transaction, [Tab]}, State0) ->
    with_transaction(Transaction, State0,
        fun() ->
            catch_abort(
                fun() ->
                    {ok, mnesia:dirty_last(Tab)}
                end)
        end);

apply_command(_Meta, {prev, Transaction, [Tab, Key]}, State0) ->
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

apply_command(_Meta, {next, Transaction, [Tab, Key]}, State0) ->
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
apply_command(_Meta, {create_table, Tab, Opts}, State) ->
    {State, [], {ok, mnesia:create_table(Tab, Opts)}};

apply_command(_Meta, {delete_table, Tab}, State) ->
    {State, [], {ok, mnesia:delete_table(Tab)}};

apply_command(_Meta, {down, Pid, _Reason}, State = #state{locker_status = LockerStatus,
                                                          locker_pid = LockerPid}) ->
    case {Pid, LockerStatus} of
        {LockerPid, up} ->
            {State#state{locker_status = down},
             start_new_locker_effects(State),
             ok};
        _ -> {State, [], ok}
    end;

apply_command(_Meta, {locker_up, Pid, Term},
              State = #state{locker_status = LockerStatus}) ->
error_logger:info_msg("mnevis locker up ~p", [{Pid, Term}]),
    case LockerStatus of
        up   -> {State, [], reject};
        down ->
            ok = update_locker_cache(Pid, Term),
            {State#state{locker_status = up,
                         locker_pid = Pid,
                         locker_term = Term},
             [{monitor, process, Pid}],
             confirm}
    end;
apply_command(_Meta, which_locker,
              State = #state{locker_status = up,
                             locker_pid = LockerPid}) when is_pid(LockerPid) ->
    {State, [], {ok, LockerPid}};
apply_command(_Meta, which_locker, State = #state{locker_status = down}) ->
    {State, [], {error, locker_down}};
%% TODO: flush_locker_transactions request
%% TODO: cleanup term transactions for previous terms
apply_command(_Meta, Unknown, State) ->
    error_logger:error_msg("Unknown command ~p~n", [Unknown]),
    {State, [], {error, unknown_command}}.

%% ==========================

%% Top level helpers

create_committed_transaction_table() ->
    mnesia:create_table(committed_transaction,
        [{attributes, record_info(fields, committed_transaction)},
         {record_name, committed_transaction},
         {type, ordered_set}]).

-spec commit(transaction(), [change()], [change()], [change()], state()) -> reply(ok).
commit(Transaction, Writes, Deletes, DeletesObject, State) ->
    Res = mnesia:transaction(fun() ->
        case mnesia:read(committed_transaction, Tid) of
            [] ->
                ok = save_committed_transaction(Tid),
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
-spec save_committed_transaction(transaction_id()) -> ok.
save_committed_transaction(Tid) ->
    ok = mnesia:write({committed_transaction, Tid, committed}).

-spec get_latest_committed_transaction() -> transaction().
get_latest_committed_transaction() ->
    mnesia:dirty_last(committed_transaction).

%% TODO: store committed transactions in memory
-spec transaction_recorded_as_committed(transaction()) -> boolean().
transaction_recorded_as_committed(Transaction) ->
    Res = mnesia:dirty_read(committed_transaction, Transaction),
    case Res of
        {atomic, []} ->
            false;
        {atomic, [{committed_transaction, Transaction, committed}]} ->
            true;
        {aborted, Err} ->
            error({cannot_read_committed_transaction, Err})
    end.

%% Locker cache

%% ==========================

create_locker_cache() ->
    locker_cache = ets:new(locker_cache, [named_table, public]),
    ok.

-spec update_locker_cache(pid(), integer()) -> ok.
update_locker_cache(Pid, Term) ->
    true = ets:insert(locker_cache, {locker, {Pid, Term}}),
    ok.

%% ==========================

%% Adding effects to the apply_result.
-spec with_pre_effects(ra_machine:effects(), {state(), ra_machine:effects()} | apply_result(T, E)) ->
    {state(), ra_machine:effects()} | apply_result(T, E).
with_pre_effects(Effects0, {State, Effects, Result}) ->
    {State, Effects0 ++ Effects, Result}.

%% Functional helpers to skip ops.

-spec with_transaction(transaction(), state(),
                       fun(() -> apply_result(T, E))) -> apply_result(T, E).
with_valid_locker(Transaction, State, Fun) ->
    case LockerTerm of
        CurrentLockerTerm -> Fun();
        _ -> {State, [], {error, wrong_locker_term}}
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
                true  -> {State, [], {error, transaction_committed}};
                false -> {State, [], Fun()}
            end
        end).

-ifdef(TEST).
-include("mnevis_machine.eunit").
-endif.
