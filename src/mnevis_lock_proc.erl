-module(mnevis_lock_proc).

-behaviour(gen_statem).

-export([start/1]).

-export([init/1,
         handle_event/3,
         terminate/3,
         code_change/4,
         leader/3,
         candidate/3,
         callback_mode/0
         ]).

-include_lib("ra/include/ra.hrl").

-record(state, {term, correlation, leader}).

-type election_states() :: leader | candidate.
-type leader_term() :: integer().
-type state() :: #state{}.

start(Term) ->
    error_logger:info_msg("Start mnevis locker"),
    gen_statem:start(?MODULE, Term, []).

-spec init(leader_term()) -> gen_statem:init_result(election_states()).
init(Term) ->
    error_logger:info_msg("Init mnevis locker"),
    %% Delayed init
    NodeId = mnevis_node:node_id(),
    Correlation = notify_up(Term, NodeId),
    {ok,
     candidate,
     #state{term = Term,
            correlation = Correlation,
            leader = NodeId},
     [1000]}.

callback_mode() -> state_functions.

-spec candidate(gen_statem:event_type(), term(), state()) ->
    gen_statem:event_handler_result(election_states()).
candidate(info, {ra_event, Leader, Event}, State) ->
    handle_ra_event(Event, State#state{leader = Leader});
candidate(timeout, _, State = #state{term = Term, leader = Leader}) ->
    Correlation = notify_up(Term, Leader),
    {keep_state, State#state{correlation = Correlation}, [1000]};
candidate({call, From}, {lock, _LockItem, _LockKind}, State) ->
    {keep_state, State, [{reply, From, {error, wrong_process}}]};
candidate(cast, _, State) ->
    {keep_state, State};
candidate(info, _Info, State) ->
    {keep_state, State}.

-spec leader(gen_statem:event_type(), term(), state()) ->
    gen_statem:event_handler_result(election_states()).
leader({call, From}, {lock, LockItem, LockKind}, State) ->
    {LockResult, State1} = lock(LockItem, LockKind, State),
    {keep_state, State1, [{reply, From, LockResult}]};
leader(cast, _, State) ->
    {keep_state, State};
leader(info, _Info, State) ->
    {keep_state, State}.

handle_event(_Type, _Content, State) ->
    error_logger:info_msg("Unexpected event ~p~n", [{_Type, _Content}]),
    {keep_state, State}.

code_change(_OldVsn, OldState, OldData, _Extra) ->
    {ok, OldState, OldData}.

terminate(_Reason, _State, _Data) ->
    ok.

-spec notify_up(leader_term(), ra_server_id()) -> reference().
notify_up(Term, NodeId) ->
    Correlation = make_ref(),
    ok = notify_up(Term, Correlation, NodeId),
    Correlation.

-spec notify_up(leader_term(), reference(), ra_server_id()) -> ok.
notify_up(Term, Correlation, NodeId) ->
    ra:pipeline_command(NodeId, {locker_up, self(), Term}, Correlation, normal).

-spec handle_ra_event(ra_server_proc:ra_event(), state()) ->
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

reject(_State) ->
    {stop, rejected}.

confirm(State) ->
    {next_state, leader, State}.


lock(_LockItem, _LockKind, State) ->
    {ok, State}.






