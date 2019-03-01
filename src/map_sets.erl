%% s/sets/map_sets/g
%% Why? Because spead (This module piggybacks on `maps' module's NIFs)
%% This code is based on the public domain code from https://github.com/k32/map_sets
%% Original code was distributed under the public domain license.


-module(map_sets).

-export([new/0,is_set/1,size/1,to_list/1,from_list/1]).
-export([is_element/2,add_element/2,del_element/2]).

-export([union/2,union/1,intersection/2,intersection/1]).
-export([is_disjoint/2]).
-export([subtract/2,is_subset/2]).
-export([fold/3,filter/2]).

-export([is_empty/1]).

-export_type([set/1, set/0]).

-type set(Key) :: #{Key => term()}.
-type set() :: set(term()).

-define(UNUSED, unused).

-spec new() -> set().
new() ->
    #{}.

-spec is_set(term()) -> boolean().
is_set(A) ->
    is_map(A).

-spec size(set()) -> non_neg_integer().
size(A) ->
    maps:size(A).

-spec is_empty(set()) -> boolean().
is_empty(A) -> maps:size(A) == 0.

-spec fold(Function, Acc, Set) -> Acc when
      Function :: fun((Element, Acc) -> Acc),
      Set :: set(Element),
      Acc :: term().
fold(Fun, A, B) ->
    maps:fold( fun(K, _, Acc) -> Fun(K, Acc) end
             , A
             , B).

-spec filter(Predicate, Set) -> Set when
      Predicate :: fun((Element) -> boolean()),
      Set :: set(Element).
filter(P, A) ->
    maps:filter( fun(K, _) -> P(K) end
               , A).

-spec to_list(set(Elem)) -> [Elem].
to_list(A) ->
    maps:keys(A).

-spec from_list([Elem]) -> set(Elem).
from_list(L) ->
    maps:from_list([{I, ?UNUSED} || I <- L]).

-spec is_element(Elem, set(Elem)) -> boolean().
is_element(Elem, Set) ->
    maps:is_key(Elem, Set).

-spec add_element(Elem, set(Elem)) -> set(Elem).
add_element(Elem, Set) ->
    Set#{Elem => ?UNUSED}.

-spec del_element(Elem, set(Elem)) -> set(Elem).
del_element(Elem, Set) ->
    maps:remove(Elem, Set).

-spec is_subset(set(Elem), set(Elem)) -> boolean().
is_subset(S1, S2) ->
    lists:all(fun(El) -> is_element(El, S2) end, to_list(S1)).

-spec subtract(set(Elem), set(Elem)) -> set(Elem).
subtract(S1, S2) ->
    maps:without(maps:keys(S2), S1).

-spec union(set(Elem), set(Elem)) -> set(Elem).
union(S1, S2) ->
    maps:merge(S1, S2).

-spec union([set(Elem)]) -> set(Elem).
union(L) ->
    lists:foldl(fun maps:merge/2, #{}, L).

-spec intersection(set(Elem), set(Elem)) -> set(Elem).
intersection(S1, S2) ->
    case maps:size(S1) > maps:size(S2) of
        true ->
            intersection_(S1, S2);
        false ->
            intersection_(S2, S1)
    end.
intersection_(Large, Small) ->
    maps:filter(fun(E, _) -> is_element(E, Large) end, Small).

-spec intersection(nonempty_list(set(Elem))) -> set(Elem).
intersection([H|T]) ->
    lists:foldl(fun intersection/2, H, T).

-spec is_disjoint(set(Elem), set(Elem)) -> boolean().
is_disjoint(S1, S2) ->
    case maps:size(S1) > maps:size(S2) of
        true ->
            is_disjoint_(S1, S2);
        false ->
            is_disjoint_(S2, S1)
    end.
is_disjoint_(Large, Small) ->
    not lists:any(fun(El) -> is_element(El, Large) end, to_list(Small)).
