-module(mnevis_machine).
-behaviour(ra_machine).

-include_lib("ra/include/ra.hrl").

-export([
         init/1,
         apply/3,
         state_enter/2,
         snapshot_module/0]).

-export([check_locker/2]).
-export([safe_table_info/3]).
-export([get_item_version/2]).
-export([compare_versions/2]).

-record(state, {
    locker_status = down,
    locker = {0, none} :: mnevis_lock_proc:locker() | {0, none},
    blacklisted = map_sets:new() :: map_sets:set(mnevis_lock:transaction_id())
}).

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

check_locker({LockerTerm, _LockerPid}, #state{locker = {CurrentLockerTerm, _}}) ->
    case LockerTerm of
        CurrentLockerTerm -> ok;
        _                 -> {error, wrong_locker_term}
    end.

-spec compare_versions([mnevis_context:read_version()], term()) ->
    ok | {version_mismatch, [mnevis_context:read_version()]}.
compare_versions(Versions, _State) ->
    mnevis_read:compare_versions(Versions).

-spec safe_table_info(mnevis:table(), term(), state()) ->
    {ok, term()} | {error, {no_exists, mnevis:table()}}.
safe_table_info(Tab, Key, _State) ->
    Tables = mnesia:system_info(tables),
    case lists:member(Tab, Tables) of
        true  -> {ok, mnesia:table_info(Tab, Key)};
        false -> {error, {no_exists, Tab}}
    end.

-spec get_item_version(mnevis_lock:lock_item(), state()) ->
    {ok, mnevis_context:read_version()} |
    {error, no_exists}.
get_item_version({Tab, Item}, _) ->
    VersionKey = case Tab of
        table -> Item;
        _     -> mnevis_lock:item_version_key(Tab, Item)
    end,
    case mnevis_read:get_version(VersionKey) of
        {error, no_exists} ->
            case Tab of
                table -> {error, no_exists};
                _     ->
                    case mnevis_read:get_version(Tab) of
                        {ok, _Version}    -> {ok, {Tab, 0}};
                        {error, _} = Err -> Err
                    end
            end;
        {ok, Version} ->
            {ok, {VersionKey, Version}}
    end.

%% Ra machine callbacks

-spec init(config()) -> state().
init(_Conf) ->
    create_committed_transaction_table(),
    mnevis_read:create_versions_table(),
    #state{}.

-spec state_enter(ra_server:ra_state() | eol, state()) -> ra_machine:effects().
state_enter(leader, State) ->
    start_new_locker_effects(State);
state_enter(recover, _State) ->
    ok = mnevis_lock_proc:create_locker_cache(),
    [];
state_enter(recovered, #state{locker = Locker}) ->
    ok = mnevis_lock_proc:update_locker_cache(Locker),
    [];
state_enter(follower, #state{locker = {_Term, Pid}})
        when is_pid(Pid) andalso node(Pid) == node() ->
    stop_locker_effects(Pid);
state_enter(State, _) ->
    error_logger:info_msg("mnevis machine enter state ~p~n", [State]),
    [].

-spec start_new_locker_effects(state()) -> ra_machine:effects().
start_new_locker_effects(#state{locker = {Term, _Pid}}) ->
    [{mod_call, mnevis_lock_proc, start_new_locker, [Term]}].

-spec stop_locker_effects(pid()) -> ra_machine:effects().
stop_locker_effects(LockerPid) ->
    [{mod_call, mnevis_lock_proc, stop, [LockerPid]}].

-spec snapshot_module() -> module().
snapshot_module() ->
    mnevis_snapshot.

-spec apply(map(), command(), state()) ->
    {state(), reply(), ra_machine:effects()}.
apply(Meta, {commit,
             Transaction,
             {Writes, Deletes, DeletesObject}},
            State)  ->
    with_valid_transaction(Transaction, State,
        fun() ->
            case {Writes, Deletes, DeletesObject} of
                {[], [], []} ->
                    %% TODO: this should not happen
                    {State, {ok, ok}, []};
                _ ->
                    Result = commit(Transaction, Writes, Deletes, DeletesObject),
                    case Result of
                        {ok, {committed, Versions}} ->
                            {State, {ok, {committed, Versions}}, snapshot_effects(Meta, State)};
                        {ok, skipped} ->
                            {State, {ok, skipped}, []};
                        _ ->
                            {State, Result, []}
                    end
            end
        end);

%% Table manipulation
apply(Meta, {create_table, Tab, Opts}, State) ->
    Result = with_ignore_consistent_error(
        fun() ->
            case mnesia:create_table(Tab, Opts) of
                {atomic, ok} ->
                    _ = mnevis_read:init_version(Tab, 0),
                    {atomic, ok};
                {aborted, _} = Res ->
                    Res
            end
        end),
    {State, {ok, Result}, snapshot_effects(Meta, State)};
apply(Meta, {delete_table, Tab}, State) ->
    Result = with_ignore_consistent_error(
        fun() ->
            mnesia:delete_table(Tab)
        end),
    {State, {ok, Result}, snapshot_effects(Meta, State)};
apply(Meta, {add_table_index, Tab, AttrName}, State) ->
    Result = with_ignore_consistent_error(
        fun() ->
            mnesia:add_table_index(Tab, AttrName)
        end),
    {State, {ok, Result}, snapshot_effects(Meta, State)};
apply(Meta, {del_table_index, Tab, AttrName}, State) ->
    Result = with_ignore_consistent_error(
        fun() ->
            mnesia:del_table_index(Tab, AttrName)
        end),
    {State, {ok, Result}, snapshot_effects(Meta, State)};

apply(Meta, {clear_table, Tab}, State) ->
    Result = with_ignore_consistent_error(
        fun() ->
            mnesia:clear_table(Tab)
        end),
    {State, {ok, Result}, snapshot_effects(Meta, State)};

apply(Meta, {transform_table, Tab, {M, F, A}, NewAttributeList, NewRecordName}, State) ->
    Fun = fun(Record) -> erlang:apply(M, F, [Record | A]) end,
    Result = with_ignore_consistent_error(
        fun() ->
            mnesia:transform_table(Tab, Fun, NewAttributeList, NewRecordName)
        end),
    {State, {ok, Result}, snapshot_effects(Meta, State)};
apply(Meta, {transform_table, Tab, {M, F, A}, NewAttributeList}, State) ->
    Fun = fun(Record) -> erlang:apply(M, F, [Record | A]) end,
    Result = with_ignore_consistent_error(
        fun() ->
            mnesia:transform_table(Tab, Fun, NewAttributeList)
        end),
    {State, {ok, Result}, snapshot_effects(Meta, State)};

%% Lock process manipulation
apply(_Meta, {down, Pid, _Reason}, State = #state{locker = {_Term, LockerPid}}) ->
    case Pid of
        LockerPid ->
            %% Locker pid is down. We need to start a new locker
            {State#state{locker_status = down}, ok, start_new_locker_effects(State)};
        _ ->
            {State, ok, []}
    end;
apply(_Meta, {locker_up, {Term, Pid} = Locker},
              State = #state{locker = {CurrentLockerTerm, CurrentLockerPid}}) ->
    case {Term, Pid} of
        %% Duplicate?
        %% TODO: do we need duplicate monitor? is it safe?
        {CurrentLockerTerm, CurrentLockerPid} ->
            {State#state{locker_status = up}, confirm, [{monitor, process, Pid}]};
        {HigherTerm, _} when HigherTerm > CurrentLockerTerm ->
            {State1, Effects} = update_locker(Locker, State),
            {State1, confirm, Effects};
        _ ->
            {State, reject, []}
    end;
apply(_Meta, {which_locker, OldLocker},
              State = #state{locker_status = up,
                             locker = {LockerTerm, _} = CurrentLocker}) ->
    case OldLocker of
        none ->
            {State, {ok, CurrentLocker}, []};
        CurrentLocker ->
            %% Transaction process cannot reach current locker.
            %% It may be down, but is still current.
            %% Or transaction node may be disconnected.
            %% It's hard to check which case is that

            %% We can assume that if transaction process
            %% can reach the ra cluster - it shoudl be able
            %% to reach the locker.

            %% TODO: monitor that.
            {State, {error, waiting_for_new_locker}, start_new_locker_effects(State)};
        {OldTerm, _} when OldTerm < LockerTerm ->
            {State, {ok, CurrentLocker}, []};
        _ ->
            %% TODO: what to do?
            {State, {error, {invalid_locker, OldLocker, CurrentLocker}}, []}
    end;
apply(_Meta, {which_locker, _OldLocker}, State = #state{locker_status = down}) ->
    %% The locker process may be down and we don't have a new one yet.
    {State, {error, waiting_for_new_locker}, []};
apply(_Meta, {blacklist, Locker, TransactionId},
             State = #state{blacklisted = BlackListed}) ->
    %% Locker detected transaction process to be down and wants to clean
    %% this transaction locks. The transaction should not be committed.
    %% See mnevis_lock_proc for more details.
    %% Only for the valid locker.
    with_valid_locker(Locker, State,
        fun() ->
            %% TODO: blacklisted storage. Does it make sense to use mnesia table?
            BlackListed1 = map_sets:add_element(TransactionId, BlackListed),
            {State#state{blacklisted = BlackListed1}, ok, []}
        end);
apply(_Meta, Unknown, State) ->
    error_logger:error_msg("Unknown command ~p~n", [Unknown]),
    {State, {error, {unknown_command, Unknown}}, []}.

%% ==========================

%% Top level helpers

update_locker({_, Pid} = Locker, State = #state{locker = {_, CurrentLockerPid}}) ->
    ok = mnevis_lock_proc:update_locker_cache(Locker),
    MaybeStopEffects = case Pid of
        CurrentLockerPid -> [];
        _                -> stop_locker_effects(CurrentLockerPid)
    end,
    %% TODO: move committed transactions to memory after implementing
    %% term increase in locker.
    cleanup_committed_transactions(),
    {State#state{locker = Locker,
                 locker_status = up,
                 blacklisted = map_sets:new()},
     [{monitor, process, Pid}] ++ MaybeStopEffects}.

cleanup_committed_transactions() ->
    mnesia:clear_table(committed_transaction).

create_committed_transaction_table() ->
    CreateResult = mnesia:create_table(committed_transaction,
                                       [{attributes, record_info(fields, committed_transaction)},
                                        {record_name, committed_transaction},
                                        {type, ordered_set}]),
    case CreateResult of
        {atomic, ok} -> ok;
        %% TODO: Find a way to execute create_committed_transaction_table only once
        {aborted,{already_exists,committed_transaction}} -> ok;
        Other -> error({cannot_create_committed_transaction_table, Other})
    end.

-spec commit(transaction(), [mnevis_context:item()],
                            [mnevis_context:delete_item()],
                            [mnevis_context:item()]) ->
    {ok, {committed, [{mnevis:table(), mnevis_context:version()}]}} |
    {ok, skipped} |
    {error, {aborted, term()}}.
commit(Transaction, Writes, Deletes, DeletesObject) ->
    Res = with_ignore_consistent_error(
        fun() ->
            mnesia:transaction(fun() ->
                case ets:lookup(committed_transaction, Transaction) of
                    [] ->
                        _ = apply_deletes(Deletes),
                        _ = apply_writes(Writes),
                        _ = apply_deletes_object(DeletesObject),
                        _ = mnesia:lock({table, versions}, write),
                        UpdatedVersions = update_table_versions(Writes,
                                                                Deletes,
                                                                DeletesObject),
                        ok = save_committed_transaction(Transaction),
                        {committed, UpdatedVersions};
                    %% Transaction is already committed.
                    [{committed_transaction, Transaction, committed}] ->
                        skipped
                end
            end)
        end),
    case Res of
        {atomic, Result} ->
            {ok, Result};
        {aborted, Reason} ->
            {error, {aborted, Reason}}
    end.

update_table_versions(Writes, Deletes, DeletesObject) ->
    TabKeysForObjects = [mnevis_lock:item_version_key(Tab, mnevis:record_key(Rec))
                         || {Tab, Rec, _} <- Writes ++ DeletesObject],
    TabKeysForDeletes = [mnevis_lock:item_version_key(Tab, Key) || {Tab, Key, _} <- Deletes],
    TabKeys = TabKeysForObjects ++ TabKeysForDeletes,

    TabKeysSorted = lists:usort(TabKeys),
    Tabs = lists:usort(element(1, lists:unzip(TabKeys))),

    TabUpdates = lists:map(
        fun(Tab) ->
            %% This function may crash
            %% Version should be in the database at this point
            mnevis_read:update_version(Tab)
        end,
        Tabs),
    KeyUpdates = lists:map(
        fun(TabKey) ->
            mnevis_read:init_version(TabKey, 1)
        end,
        TabKeysSorted),
    TabUpdates ++ KeyUpdates.


-spec snapshot_effects(map(), state()) -> ra_machine:effects().
snapshot_effects(#{index := RaftIdx}, State) ->
    [{release_cursor, RaftIdx, State}].

%% ==========================

%% Mnesia operations

-spec apply_deletes([mnevis_context:delete_item()]) -> [ok].
apply_deletes(Deletes) ->
    [ok = mnesia:delete(Tab, Key, write)
     || {Tab, Key, _LockKind} <- Deletes].

-spec apply_deletes_object([mnevis_context:item()]) -> [ok].
apply_deletes_object(DeletesObject) ->
    [ok = mnesia:delete_object(Tab, Rec, write)
     || {Tab, Rec, _LockKind} <- DeletesObject].

-spec apply_writes([mnevis_context:item()]) -> [ok].
apply_writes(Writes) ->
    [ok = mnesia:write(Tab, Rec, write)
     || {Tab, Rec, _LockKind} <- Writes].

-spec save_committed_transaction(transaction()) -> ok.
save_committed_transaction(Transaction) ->
    ok = mnesia:write({committed_transaction, Transaction, committed}).

%% TODO: store committed transactions in memory
% TODO LRB
% -spec transaction_recorded_as_committed(transaction()) -> boolean().
% transaction_recorded_as_committed(Transaction) ->
%     Res = mnesia:dirty_read(committed_transaction, Transaction),
%     case Res of
%         [] ->
%             false;
%         [{committed_transaction, Transaction, committed}] ->
%             true
%     end.

-spec is_transaction_id_blacklisted(mnevis_lock:transaction_id(), state()) ->
    boolean().
is_transaction_id_blacklisted(TransactionId, #state{blacklisted = BlackListed}) ->
    map_sets:is_element(TransactionId, BlackListed).

%% ==========================

%% Functional helpers to skip ops.
-spec with_valid_transaction(mnevis_context:transaction(), state(),
                             fun(() -> apply_result(T, E))) -> apply_result(T, E).
with_valid_transaction({TransactionId, Locker}, State, Fun) ->
    case check_locker(Locker, State) of
        ok ->
            case is_transaction_id_blacklisted(TransactionId, State) of
                false -> Fun();
                true  -> {State, {error, {aborted, transaction_seen_as_down_by_locker}}, []}
            end;
        {error, Reason} -> {State, {error, {aborted, Reason}}, []}
    end.

-spec with_valid_locker(mnevis_lock_proc:locker(), state(),
                       fun(() -> apply_result(T, E))) -> apply_result(T, E).
with_valid_locker(Locker, State, Fun) ->
    case check_locker(Locker, State) of
        ok -> Fun();
        {error, Reason} -> {State, {error, {aborted, Reason}}, []}
    end.

-spec with_ignore_consistent_error(fun(() -> {atomic, OK} | {aborted, Err})) ->
    {atomic, OK} | {aborted, Err}.
with_ignore_consistent_error(Fun) ->
    case Fun() of
        {atomic, _} = OK -> OK;
        {aborted, Error} ->
            case consistent_error(Error) of
                true  ->
                    {aborted, Error};
                false ->
                    error({inconsistent_mnevis_error, Error})
            end
        %% Don't expect anything else
    end.

-spec consistent_error(term()) -> boolean().
% consistent_error(_) -> true;
%% Mnesia specific errors, which should be consistent
consistent_error(nested_transaction) -> true;
consistent_error(badarg)             -> true;
consistent_error(no_transaction)     -> true;
consistent_error(combine_error)      -> true;
consistent_error(bad_index)          -> true;
consistent_error(already_exists)     -> true;
consistent_error(index_exists)       -> true;
consistent_error(no_exists)          -> true;
consistent_error(bad_type)           -> true;
consistent_error(illegal)            -> true;
consistent_error(active)             -> true;
consistent_error(not_active)         -> true;
%% Mnesia specific errors which may be inconsistent across nodes:
consistent_error(system_limit)          -> false;
consistent_error(mnesia_down)           -> false;
consistent_error(truncated_binary_file) -> false;

%% All mnevis nodes should only have local mnesia node
consistent_error(not_a_db_node)         -> false;

%% Mnesia should run on a mnevis node
consistent_error(node_not_running)      -> false;

%% Copy from mnesia:error_description/1 code. Expands error tuples.
consistent_error({'EXIT', Reason}) ->
    consistent_error(Reason);
consistent_error({error, Reason}) ->
    consistent_error(Reason);
consistent_error({aborted, Reason}) ->
    consistent_error(Reason);
consistent_error(Reason) when is_tuple(Reason), size(Reason) > 0 ->
    case element(1, Reason) of
        %% Mnesia transform functions return pre-formatted errors
        %% TODO: a better way to check that
        String when is_list(String) ->
            true;
        Other ->
            consistent_error(Other)
    end;
%% Any other error reason is considered inconsistent.
consistent_error(_Reason) -> false.

%% ==============================

-ifdef(TEST).
-include("mnevis_machine.eunit").
-endif.
