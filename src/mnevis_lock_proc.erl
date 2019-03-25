-module(mnevis_lock_proc).

-behaviour(gen_statem).

-export([start/1, stop/1, start_new_locker/1]).

-export([locate/0,
         ensure_lock_proc/1,
         try_lock_call/2,
         rollback/2]).

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

-include_lib("ra/include/ra.hrl").

-record(state, {term, correlation, leader, lock_state}).

-define(LOCKER_TIMEOUT, 5000).

-type election_states() :: leader | candidate.
-type state() :: #state{}.

-type locker_term() :: integer().
-type locker() :: {locker_term(), pid()}.

-export_type([locker_term/0, locker/0]).

start(Term) ->
    error_logger:info_msg("Start mnevis locker"),
    gen_statem:start(?MODULE, Term, []).

-spec start_new_locker(locker()) -> {ok, pid()}.
start_new_locker({Term, Pid}) ->
    case is_pid(Pid) of
        true  -> gen_statem:cast(Pid, stop);
        false -> ok
    end,
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
        {ok, {error, Reason}, _} -> {error, {command_error, Reason}};
        {error, Reason}          -> {error, Reason};
        {timeout, _}             -> {error, timeout}
    end.

-spec try_lock_call(locker(), mnevis_lock:lock_request()) ->
    mnevis_lock:lock_result() |
    {ok, mnevis_lock:transaction_id(), term()} |
    {error, locker_not_running} |
    {error, is_not_leader}.
try_lock_call({_Term, Pid}, LockRequest) ->
    try
        gen_statem:call(Pid, LockRequest, ?LOCKER_TIMEOUT)
    catch
        exit:{noproc, {gen_statem, call, [Pid, LockRequest, ?LOCKER_TIMEOUT]}} ->
            {error, locker_not_running};
        exit:{timeout, {gen_statem, call, [Pid, LockRequest, ?LOCKER_TIMEOUT]}} ->
            {error, locker_timeout}
    end.

-spec rollback(mnevis_lock:transaction_id(), locker()) -> ok.
rollback(Tid, {_LockerTerm, LockerPid}) ->
    ok = gen_statem:call(LockerPid, {rollback, Tid, self()}).

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
            lock_state = mnevis_lock:init(0)},
     [1000]}.

callback_mode() -> state_functions.

-spec candidate(gen_statem:event_type(), term(), state()) ->
    gen_statem:event_handler_result(election_states()).
candidate(info, {ra_event, Leader, Event}, State) ->
    handle_ra_event(Event, State#state{leader = Leader});
candidate(timeout, _, State = #state{term = Term, leader = Leader}) ->
    Correlation = notify_up(Term, Leader),
    {keep_state, State#state{correlation = Correlation}, [1000]};
candidate({call, From}, {lock, _TransationId, _Source, _LockItem, _LockKind}, State) ->
    {keep_state, State, [{reply, From, {error, is_not_leader}}]};
candidate(cast, _, State) ->
    {keep_state, State};
candidate(info, _Info, State) ->
    {keep_state, State}.

-spec leader(gen_statem:event_type(), term(), state()) ->
    gen_statem:event_handler_result(election_states()).
leader({call, From},
       {lock, TransationId, Source, LockItem, LockKind},
       State = #state{lock_state = LockState}) ->
    {LockResult, LockState1} = mnevis_lock:lock(TransationId, Source, LockItem, LockKind, LockState),
    {keep_state, State#state{lock_state = LockState1}, [{reply, From, LockResult}]};
leader({call, From}, {rollback, TransationId, Source}, State) ->
    LockState = mnevis_lock:cleanup(TransationId, Source, State#state.lock_state),
    {keep_state, State#state{lock_state = LockState}, [{reply, From, ok}]};
leader(cast, stop, _State) ->
    {stop, {normal, stopped}};
leader(cast, _, State) ->
    {keep_state, State};
leader(info, {'DOWN', MRef, process, Pid, Info}, #state{lock_state = LockState0} = State) ->
    LockState1 = mnevis_lock:monitor_down(MRef, Pid, Info, LockState0),
    {keep_state, State#state{lock_state = LockState1}};
leader(info, _Info, State) ->
    {keep_state, State}.

handle_event(_Type, _Content, State) ->
    error_logger:info_msg("Unexpected event ~p~n", [{_Type, _Content}]),
    {keep_state, State}.

code_change(_OldVsn, OldState, OldData, _Extra) ->
    {ok, OldState, OldData}.

terminate(_Reason, _State, _Data) ->
    ok.

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
    error_logger:error_msg("Replies ~p~n", [Replies]),
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
