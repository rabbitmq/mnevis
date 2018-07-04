-module(ramnesia_prop_SUITE).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").


all() ->
    [
    mnesia_transaction_yield_same_result_as_ramnesia
    ].

init_per_suite(Config) ->
    ramnesia:start(),
    Config.

end_per_suite(Config) -> Config.

mnesia_transaction_yield_same_result_as_ramnesia(_Config) ->
    ct:timetrap(18000000),
    true = proper:quickcheck(mnesia_transaction_yield_same_result_as_ramnesia_prop(), 1000).

mnesia_transaction_yield_same_result_as_ramnesia_prop() ->
    ?FORALL(
        Actions, non_empty(list(oneof([action_gen(), action_list_gen()]))),
        transaction_equal(Actions)).

ops() -> [read, write, delete, delete_object, match_object].

run_op(T, {Op, K, V}) -> run_op(Op, T, K, V).

run_op(read, T, K, _V) -> mnesia:read(T, K);
run_op(write, T, K, V) -> mnesia:write({T, K, V});
run_op(delete, T, K, _V) -> mnesia:delete({T, K});
run_op(delete_object, T, K, V) -> mnesia:delete_object({T, K, V});
run_op(match_object, T, K, V) -> mnesia:match_object({T, K, V}).

action_gen() ->
    ?LET(Op, oneof(ops()),
        ?LET(K, key_gen(),
            ?LET(V, not_underscore(),
                {Op, K, V}))).

action_list_gen() ->
    ?LET(Key, key_gen(),
        ?LET(Val, not_underscore(),
            non_empty(list(action_gen(Key, Val))))).

action_gen(Key, Val) ->
    ?LET(Op, oneof(ops()),
        ?LET(K, frequency([{10, Key}, {1, key_gen()}]),
            ?LET(V, frequency([{10, Val}, {1, not_underscore()}]),
                {Op, K, V}))).

key_gen() ->
    oneof([mnesia_atom_gen(), binary(), loose_tuple(oneof([mnesia_atom_gen(), binary()]))]).

mnesia_atom_gen() ->
    ?SUCHTHAT(A, atom(), A =/= '_' andalso A =/= '').

not_underscore() ->
    ?SUCHTHAT(A, any(), not contains_underscore(A)).

contains_underscore('_') -> true;
contains_underscore(List) when is_list(List) ->
    lists:member('_', List) orelse lists:any(fun contains_underscore/1, List);
contains_underscore(Tuple) when is_tuple(Tuple) ->
    contains_underscore(tuple_to_list(Tuple));
contains_underscore(_) -> false.

transaction_equal(Actions0) ->
    Actions = lists:flatten(Actions0),
    mnesia:delete_table(mnesia_table),
    mnesia:create_table(mnesia_table, []),
    % mnesia:create_table(ramnesia_table, []),

    MnesiaRes = mnesia:transaction(fun() ->
        [run_op(mnesia_table, Action) || Action <- Actions]
    end),

    MnesiaDB = lists:usort(ets:tab2list(mnesia_table)),

    mnesia:delete_table(mnesia_table),
    mnesia:create_table(mnesia_table, []),

    RamnesiaRes = ramnesia:transaction(fun() ->
        [run_op(mnesia_table, Action) || Action <- Actions]
    end),

    RamnesiaDB = lists:usort(ets:tab2list(mnesia_table)),

    ct:pal("Res ~p ~n ~p ~n", [MnesiaRes, RamnesiaRes]),

    ct:pal("DB ~p ~n ~p ~n", [MnesiaDB, RamnesiaDB]),

    ct:pal("Ops ~p ~n", [Actions]),

    MnesiaRes == RamnesiaRes andalso RamnesiaDB == MnesiaDB.