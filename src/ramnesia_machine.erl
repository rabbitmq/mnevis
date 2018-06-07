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
-type state() :: map().
-type reply() :: term().

-spec init(config()) -> {state(), ra_machine:effects()}.
init(_Conf) ->
    {#{}, []}.

-spec apply(ra_index(), command(), state()) ->
    {state(), ra_machine:effects()} | {state(), ra_machine:effects(), reply()}.
apply(_RaftIdx, {lock, Source, Locks}, State) ->
    case can_lock(Locks, State) of
        true ->
            State1 = lock(Source, Locks, State),
            {State1, [{monitor, process, Source}], true};
        false ->
            {State, [], false}
    end;
apply(_RaftIdx, {write, Source, Locks, Writes}, State) ->
    case does_hold_locks(Source, Locks, State) of
        true ->
            %% Actually write the data.
            %% Should be atomic on mnesia level (transaction)
            ok = apply_writes(Writes),
            State1 = clear_locks(Source, Locks, State),
            {State1, [{demonitor, Source}], true};
        false ->
            State1 = clear_locks(Source, Locks, State),
            {State1, [{demonitor, Source}], false}
    end;
apply(_RaftIdx, {down, Source, Reason}, State) ->
    State1 = clear_locks(Source, State),
    {State1, [], ok}.

-spec leader_effects(state()) -> ra_machine:effects().
leader_effects(State) -> remonitor_sources(State).

-spec eol_effects(state()) -> ra_machine:effects().
eol_effects(_State) -> [].

-spec tick(TimeMs :: integer(), state()) -> ra_machine:effects().
tick(_Time, _State) -> [].

-spec overview(state()) -> map().
overview(_State) -> #{}.

remonitor_sources(#{locks => CurrentLocks}) ->
    Sources = lock_sources(CurrentLocks),
    [{monitor, process, Source} || Source <- Sources].

lock_sources(CurrentLocks) ->
    lists:usort([Source || {Source, Lock} <- CurrentLocks]).

can_lock(Locks, #{locks => CurrentLocks}) ->
    lists:all(
        fun({_Source, Lock}) ->
            not lists:member(Lock, Locks)
        end,
        CurrentLocks).

lock(Source, Locks, State = #{locks => CurrentLocks}) ->
    State#{locks => CurrentLocks ++ [{Source, Lock} || Lock <- Locks]}.

clear_locks(Source, Locks, State = #{locks => CurrentLocks}) ->
    CurrentLocks -- [{Source, Lock} || Lock <- Locks].

clear_locks(Source, State = #{locks => CurrentLocks}) ->
    CurrentLocks1 = lists:filter(fun({LSource, _}) when LSource == Source ->
                                        false;
                                    (_) ->
                                        true
                                 end,
                                 CurrentLocks),
    Source#{locks = CurrentLocks1}.

apply_writes(Writes) ->
    {atomic, ok} = mnesia:transaction(
        fun() ->
            [ok = mnesia:write(Write) || Write <- Writes],
            ok
        end).

%% Check that all locks are held by the source.
can_write(Source, Locks, #{locks => CurrentLocks}) ->
    lists:all(
        fun(Lock) ->
            lists:member({Source, Lock}, CurrentLocks))
        end).
