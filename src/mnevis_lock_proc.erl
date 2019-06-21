-module(mnevis_lock_proc).

-behaviour(gen_statem).

-export([start/1, stop/1, start_new_locker/1]).

-export([locate/0,
         ensure_lock_proc/1,
         try_lock_call/2,
         cleanup/2]).

-export([create_locker_cache/0,
         update_locker_cache/1]).

-export([init/1,
         handle_event/3,
         terminate/3,
         code_change/4,
         leader/3,
         candidate/3,
         callback_mode/0
         ]).
-export([get_version/1]).

-include_lib("ra/include/ra.hrl").

-record(state, {
    term,
    correlation,
    leader,
    lock_state,
    waiting_blacklist :: #{reference() => {mnevis_lock:transaction_id(), pid()}},
    waiting_blacklist_tids :: map_sets:set(mnevis_lock:transaction_id())}).

-define(LOCKER_TIMEOUT, 5000).

-type election_states() :: leader | candidate.
-type state() :: #state{}.

-type locker_term() :: integer().
-type locker() :: {locker_term(), pid()}.

-export_type([locker_term/0, locker/0]).

%% TODO: share with mnevis.erl
-define(CONSISTENT_QUERY_TIMEOUT, 60000).


%% Locker process for mnevis

%% When the process starts, it will be in a candidate state
%% In this state it tries to register with the ra cluster
%% by sending the `locker_up` command
%% If registered successfully - it changes the state to leader,
%% if rejected - stops.
%% Candidate will retry commands until it gets a result from the ra cluster

%% Each new locker process has a term, which is incremented by
%% the `start_new_locker` function.
%% If a locker with a lower term tries to register - it will be rejected.
%% A locker with the same term, but a different PID will also be rejected.
%% A locker with a higher term will be registered and become a current one.
%% The ra machine should stop an old locker process when registering a new one

%% In the leader state the process may process lock requests.
%% All lock requests are processed by the `mnevis_lock` module functions.

%% This process also monitors all transaction processes
%% (monitor/2 and demonitor/1 are called from `mnevis_lock` module)

%% When the process detects a transaction process to be down, it cannot
%% clean the transaction locks yet, because it may be disconnected and
%% transaction process may proceed with committing.

%% To prevent this, the locker process will inform the ra cluster to
%% blacklist such transaction ID. Commits for this ID will be refused.
%% When ra cluster respons to the blacklist request - it should be safe
%% to cleanup the transaction locks, because it's not going to be committed.
%% The idea is that all transaction locks will stay locked until
%% the transaction is blacklisted.

%% The ra machine should cleanup blacklisted transaction IDs when a new locker
%% process is registered. Because transaction ids exist in a context of
%% a single locker term.

start(Term) ->
    error_logger:info_msg("Start mnevis locker"),
    gen_statem:start(?MODULE, Term, []).

-spec start_new_locker(locker_term()) -> {ok, pid()}.
start_new_locker(Term) ->
    start(Term + 1).

-spec stop(pid()) -> ok.
stop(Pid) ->
    case is_pid(Pid) of
        true  -> gen_statem:cast(Pid, stop);
        false -> ok
    end.

-spec create_locker_cache() -> ok.
create_locker_cache() ->
    case ets:info(locker_cache, name) of
        undefined ->
            locker_cache = ets:new(locker_cache, [named_table, public]),
            ok;
        locker_cache -> ok
    end.

-spec update_locker_cache(locker()) -> ok.
update_locker_cache(Locker) ->
    true = ets:insert(locker_cache, {locker, Locker}),
    ok.

-spec locate() -> {ok, locker()} | {error, term}.
locate() ->
    case ets:lookup(locker_cache, locker) of
        [{locker, Locker}] -> {ok, Locker};
        []                 -> get_current_ra_locker(none)
    end.

-spec ensure_lock_proc(locker()) -> {ok, locker()} | {error, term()}.
ensure_lock_proc(Locker) ->
    case ets:lookup(locker_cache, locker) of
        [{locker, Locker}] ->
            %% Ets cache contains dead or unaccessible reference
            get_current_ra_locker(Locker);
        [{locker, Other}] ->
            %% This one may be running.
            {ok, Other}
    end.

-spec get_current_ra_locker(locker() | none) -> {ok, locker()} | {error, term()}.
get_current_ra_locker(CurrentLocker) ->
    case ra:process_command(mnevis_node:node_id(), {which_locker, CurrentLocker}) of
        {ok, {ok, Locker}, _}    ->
            update_locker_cache(Locker),
            {ok, Locker};
        %% TODO: handle locker errors
        {ok, {error, Reason}, _} -> {error, {command_error, Reason}};
        {error, Reason}          -> {error, Reason};
        {timeout, _}             -> {error, timeout}
    end.

-spec try_lock_call(locker(), mnevis_lock:lock_request()) ->
    mnevis_lock:lock_result() |
    {ok, mnevis_lock:transaction_id(), term()} |
    {error, locker_not_running} |
    {error, locker_timeout}.
try_lock_call({Term, Pid}, LockRequest) ->
    try
        gen_statem:call(Pid, {LockRequest, Term}, ?LOCKER_TIMEOUT)
    catch
        exit:{noproc, {gen_statem, call, [Pid, LockRequest, ?LOCKER_TIMEOUT]}} ->
            {error, locker_not_running};
        exit:{timeout, {gen_statem, call, [Pid, LockRequest, ?LOCKER_TIMEOUT]}} ->
            {error, locker_timeout}
    end.

-spec cleanup(mnevis_lock:transaction_id(), locker()) -> ok.
cleanup(Tid, {_LockerTerm, LockerPid}) ->
    Call = {cleanup, Tid, self()},
    try
        ok = gen_statem:call(LockerPid, Call)
    %% Ignore noproc.
    %% If locker process is unavailable it will cleanup the transaction
    catch
        exit:{noproc,{gen_statem,call,[LockerPid,Call,infinity]}} ->
            ok
    end.


-spec init(locker_term()) -> gen_statem:init_result(election_states()).
init(Term) ->
    error_logger:info_msg("Init mnevis locker"),
    %% Delayed init
    NodeId = mnevis_node:node_id(),
    Correlation = notify_up(Term, NodeId),
    {ok,
     candidate,
     #state{term = Term,
            correlation = Correlation,
            leader = NodeId,
            lock_state = mnevis_lock:init(0),
            waiting_blacklist = #{},
            waiting_blacklist_tids = map_sets:new()},
     [1000]}.

callback_mode() -> state_functions.

-spec candidate(gen_statem:event_type(), term(), state()) ->
    gen_statem:event_handler_result(election_states()).
candidate(info, {ra_event, Leader, Event}, State) ->
    handle_ra_event(Event, State#state{leader = Leader});
candidate(timeout, _, State = #state{term = Term, leader = Leader}) ->
    Correlation = notify_up(Term, Leader),
    {keep_state, State#state{correlation = Correlation}, [1000]};
candidate({call, _From}, {lock, _Tid, _Source, _LockItem, _LockKind}, State) ->
    %% Delay until we're leader
    {keep_state, State, [postpone]};
candidate({call, _From}, {lock_and_version, _Tid, _Source, _LockItem, _LockKind}, State) ->
    %% Delay until we're leader
    {keep_state, State, [postpone]};
candidate(cast, _, State) ->
    {keep_state, State};
candidate(info, _Info, State) ->
    {keep_state, State}.

-spec leader(gen_statem:event_type(), term(), state()) ->
    gen_statem:event_handler_result(election_states()).
leader({call, From},
       {{lock, Tid, Source, LockItem, LockKind}, Term},
       State = #state{lock_state = LockState,
                      term = Term}) ->
    with_non_blacklisted_transaction(Tid, From, State,
        fun() ->
            {LockResult, LockState1} = mnevis_lock:lock(Tid, Source, LockItem, LockKind, LockState),
            {keep_state, State#state{lock_state = LockState1}, [{reply, From, LockResult}]}
        end);
leader({call, From},
       {{lock_and_version, Tid, Source, LockItem, LockKind}, Term},
       State = #state{lock_state = LockState,
                      term = Term}) ->
    with_non_blacklisted_transaction(Tid, From, State,
        fun() ->
            {LockResult, LockState1} = mnevis_lock:lock(Tid, Source, LockItem, LockKind, LockState),
            State1 = State#state{lock_state = LockState1},
            case LockResult of
                {ok, RealTid} ->
                    spawn_link(fun() ->
                        VersionResult = get_version(LockItem),
                        gen_statem:reply(From, {ok, RealTid, VersionResult})
                    end),
                    {keep_state, State1, []};
                {error, _} ->
                    {keep_state, State1, [{reply, From, LockResult}]}
            end
        end);
leader({call, From},
       {{lock, _, _, _, _}, _Term},
       State = #state{term = _CurrentTerm}) ->
    % TODO Term and CurrentTerm are unused. This case is for when they don't
    % match, do we want to log an error?
    {keep_state, State, [{reply, From, {error, locker_term_mismatch}}]};
leader({call, From},
       {{lock_and_version, _, _, _, _}, _Term},
       State = #state{term = _CurrentTerm}) ->
    % TODO Term and CurrentTerm are unused. This case is for when they don't
    % match, do we want to log an error?
    {keep_state, State, [{reply, From, {error, locker_term_mismatch}}]};
leader({call, From}, {cleanup, Tid, Source}, State) ->
    LockState = mnevis_lock:cleanup(Tid, Source, State#state.lock_state),
    {keep_state, State#state{lock_state = LockState}, [{reply, From, ok}]};
leader(cast, stop, _State) ->
    {stop, {normal, stopped}};
leader(cast, _, State) ->
    {keep_state, State};
leader(info, {'DOWN', MRef, process, Pid, Info}, #state{lock_state = LockState0} = State0) ->
    %% TODO: maybe batch failed transaction IDs
    case mnevis_lock:monitor_down(MRef, Pid, Info, LockState0) of
        {none, LockState1} ->
            {keep_state, State0#state{lock_state = LockState1}};
        {Tid, LockState1} ->
            {keep_state,
             send_blacklist_command(Tid, Pid,
                                    State0#state{lock_state = LockState1})}
    end;
leader(info, {ra_event, Leader, {applied, Replies}},
             State0 = #state{waiting_blacklist = WaitingBlacklist,
                             waiting_blacklist_tids = WaitingBlacklistTids}) ->
    State1 = State0#state{leader = Leader},
    {TidWithPids, State2} = lists:foldl(
        %% Successfully blacklisted
        fun({Correlation, Result}, {TidWithPidAcc, State}) ->
            case maps:get(Correlation, WaitingBlacklist, none) of
                %% Transactions are already cleaned up.
                %% This can happen when term updated for example
                %% or when reply is duplicate
                none ->
                    {TidWithPidAcc, State};
                {Tid, _} = TidWithPid ->
                    case Result of
                        ok ->
                            {[TidWithPid | TidWithPidAcc],
                             State#state{
                                waiting_blacklist = maps:remove(Correlation, WaitingBlacklist),
                                waiting_blacklist_tids = map_sets:del_element(Tid, WaitingBlacklistTids)
                             }};
                        {error, {aborted, wrong_locker_term}} ->
                            %% TODO error handling
                            error({error, {aborted, wrong_locker_term}})
                    end
            end
        end,
        {[], State1},
        Replies),
    {keep_state, cleanup_blacklisted(TidWithPids, State2)};
leader(info, {ra_event, Leader, {rejected, {not_leader, _Leader, Correlation}}},
             State = #state{waiting_blacklist = WaitingBlacklist}) ->
    case maps:get(Correlation, WaitingBlacklist, none) of
        none ->
            {keep_state, State};
        {Tid, Pid} ->
            {keep_state, send_blacklist_command(Tid, Pid, State#state{leader = Leader})}
    end;
leader(info, _Info, State) ->
    {keep_state, State}.

handle_event(_Type, _Content, State) ->
    error_logger:info_msg("Unexpected event ~p~n", [{_Type, _Content}]),
    {keep_state, State}.

code_change(_OldVsn, OldState, OldData, _Extra) ->
    {ok, OldState, OldData}.

terminate(_Reason, _State, _Data) ->
    ok.

get_version(LockItem) ->
    VersionKey = case LockItem of
        {table, Table} -> Table;
        {Tab, Item}    -> mnevis_lock:item_version_key(Tab, Item)
    end,
    case ra:consistent_query(mnevis_node:node_id(),
                             {mnevis_machine, get_item_version, [VersionKey]},
                             ?CONSISTENT_QUERY_TIMEOUT) of
        {ok, Result, _} ->
            Result;
        {error, Err} ->
            {error, Err};
        {timeout, TO} ->
            {error, {timeout, TO}}
    end.

%% Registration with the RA cluster
%% ================================

-spec notify_up(locker_term(), ra_server_id()) -> reference().
notify_up(Term, NodeId) ->
    Correlation = make_ref(),
    ok = notify_up(Term, Correlation, NodeId),
    Correlation.

-spec notify_up(locker_term(), reference(), ra_server_id()) -> ok.
notify_up(Term, Correlation, NodeId) ->
    ra:pipeline_command(NodeId, {locker_up, {Term, self()}}, Correlation, normal).

-spec handle_ra_event(ra_server_proc:ra_event_body(), state()) ->
    gen_statem:event_handler_result(election_states()).
handle_ra_event({applied, []}, State) -> {keep_state, State};
handle_ra_event({applied, Replies}, State = #state{correlation = Correlation}) ->
    case proplists:get_value(Correlation, Replies, none) of
        none    -> {keep_state, State};
        reject  -> reject(State);
        confirm -> confirm(State)
    end;
handle_ra_event({rejected, {not_leader, Leader, Correlation}},
                State = #state{correlation = Correlation}) ->
    renotify_up(Leader, State);
handle_ra_event({rejected, {not_leader, _, OldCorrelation}},
                State = #state{correlation = Correlation})
            when OldCorrelation =/= Correlation ->
    {keep_state, State}.

-spec renotify_up(ra_server_id(), state()) ->
    gen_statem:event_handler_result(election_states()).
renotify_up(Leader, State = #state{term = Term, correlation = Correlation}) ->
    ok = notify_up(Term, Correlation, Leader),
    {keep_state, State#state{leader = Leader}}.

reject(State) ->
    {stop, {normal, {rejected, State}}}.

confirm(State) ->
    {next_state, leader, State}.


%% Handling DOWN from transaction process.
%% =======================================

-spec send_blacklist_command(mnevis_lock:transaction_id(), pid(), state()) -> state().
send_blacklist_command(Tid, Pid,
                       State = #state{waiting_blacklist = WaitingBlacklist,
                                      waiting_blacklist_tids = WaitingBlacklistTids,
                                      leader = Leader,
                                      term = Term}) ->
    Correlation = make_ref(),
    ok = ra:pipeline_command(Leader,
                             {blacklist, {Term, self()}, Tid},
                             Correlation,
                             normal),
    State#state{
        waiting_blacklist = maps:put(Correlation, {Tid, Pid}, WaitingBlacklist),
        waiting_blacklist_tids = map_sets:add_element(Tid, WaitingBlacklistTids)
    }.

-spec cleanup_blacklisted([{mnevis_lock:transaction_id(), pid()}], state()) -> state().
cleanup_blacklisted(TidWithPids, State = #state{lock_state = LockState0}) ->
    LockState1 = lists:foldl(
        fun({Tid, Pid}, LockState) ->
            mnevis_lock:cleanup(Tid, Pid, LockState)
        end,
        LockState0,
        TidWithPids),
    State#state{lock_state = LockState1}.


with_non_blacklisted_transaction(Tid, From, State, Fun) ->
    #state{waiting_blacklist_tids = WaitingBlacklistTids} = State,
    case map_sets:is_element(Tid, WaitingBlacklistTids) of
        true ->
            {keep_state, State, [{reply, From, {error, transaction_seen_as_down_by_locker}}]};
        false ->
            Fun()
    end.
