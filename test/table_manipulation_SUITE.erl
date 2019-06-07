-module(table_manipulation_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [{group, three_nodes}].

groups() ->
    [{three_nodes, [], all_tests()}].

all_tests() ->
    [
        create_table_leader,
        create_table_follower,

        delete_table_leader,
        delete_table_follower,

        add_index_leader,
        add_index_follower,

        del_index_leader,
        del_index_follower,

        clear_table_leader,
        clear_table_follower,

        transform_table_leader,
        transform_table_follower
    ].

init_per_group(three_disc_nodes=_Group, Config) ->
    [{disc_copies, true}|Config];
init_per_group(_Group, Config) ->
    [{disc_copies, false}|Config].

end_per_group(_, Config) ->
    Config.

init_per_testcase(_, Config0) ->
    {ok, Nodes} = mnevis_test_utils:create_initial_nodes(),
    {ok, Config1} = mnevis_test_utils:start_cluster(Nodes, Config0),
    [{nodes, Nodes} | Config1].

end_per_testcase(_, Config) ->
    mnevis_test_utils:stop_all(Config).

create_table_leader(_Config) ->
    on_leader(table_manipulation_SUITE, create_table, []).

create_table_follower(_Config) ->
    on_follower(table_manipulation_SUITE, create_table, []).

create_table() ->
    {atomic, ok} = mnevis:create_table(foo, []),
    0 = mnesia:table_info(foo, size),
    ok.

delete_table_leader(_Config) ->
    on_leader(table_manipulation_SUITE, delete_table, []).

delete_table_follower(_Config) ->
    on_follower(table_manipulation_SUITE, delete_table, []).

delete_table() ->
    {atomic, ok} = mnevis:create_table(foo, []),
    0 = mnesia:table_info(foo, size),
    {atomic, ok} = mnevis:delete_table(foo),
    false = lists:member(foo, mnesia:system_info(tables)),
    ok.

add_index_leader(_Config) ->
    on_leader(table_manipulation_SUITE, add_index, []).

add_index_follower(_Config) ->
    on_follower(table_manipulation_SUITE, add_index, []).

add_index() ->
    {atomic, ok} = mnevis:create_table(foo, [{attributes, [key, val]}]),
    0 = mnesia:table_info(foo, size),
    {atomic, ok} = mnevis:add_table_index(foo, val),
    [3] = mnesia:table_info(foo, index),
    {aborted, {no_exists, unknown_table}} = mnevis:add_table_index(unknown_table, val),
    {aborted, {bad_type, unknown_key}} = mnevis:add_table_index(foo, unknown_key),
    [3] = mnesia:table_info(foo, index),
    ok.

del_index_leader(_Config) ->
    on_leader(table_manipulation_SUITE, del_index, []).

del_index_follower(_Config) ->
    on_follower(table_manipulation_SUITE, del_index, []).

del_index() ->
    {atomic, ok} = mnevis:create_table(foo, [{attributes, [key, val]}]),
    0 = mnesia:table_info(foo, size),
    {atomic, ok} = mnevis:add_table_index(foo, val),
    [3] = mnesia:table_info(foo, index),
    {atomic, ok} = mnevis:del_table_index(foo, val),
    [] = mnesia:table_info(foo, index),
    {aborted, {no_exists, foo, 3}} = mnevis:del_table_index(foo, val),
    ok.

clear_table_leader(_Config) ->
    on_leader(table_manipulation_SUITE, clear_table, []).

clear_table_follower(_Config) ->
    on_follower(table_manipulation_SUITE, clear_table, []).

clear_table() ->
    {atomic, ok} = mnevis:create_table(foo, [{attributes, [key, val]}]),
    0 = mnesia:table_info(foo, size),
    {atomic, ok} = mnevis:transaction(fun() ->
        mnesia:write({foo, 1, 1}),
        mnesia:write({foo, 2, 2}),
        mnesia:write({foo, 3, 3}),
        mnesia:write({foo, 4, 4})
    end),
    4 = mnesia:table_info(foo, size),
    {atomic, ok} = mnevis:clear_table(foo),
    0 = mnesia:table_info(foo, size),
    {aborted, {no_exists, foo1}} = mnevis:clear_table(foo1),
    ok.


transform_table_leader(_Config) ->
    on_leader(table_manipulation_SUITE, transform_table, []).

transform_table_follower(_Config) ->
    on_follower(table_manipulation_SUITE, transform_table, []).

transform_table() ->
    {atomic, ok} = mnevis:create_table(foo, [{attributes, [key, val]}]),
    0 = mnesia:table_info(foo, size),
    {atomic, ok} = mnevis:transaction(fun() ->
        mnesia:write({foo, 1, 1}),
        mnesia:write({foo, 2, 2})
    end),
    2 = mnesia:table_info(foo, size),

    [key, val] = mnesia:table_info(foo, attributes),
    foo = mnesia:table_info(foo, record_name),

    Transfrom = {table_manipulation_SUITE, transform, [<<"foo">>]},
    {atomic, ok} = mnevis:transform_table(foo, Transfrom, [key, val, blah], foo_new),

    [key, val, blah] = mnesia:table_info(foo, attributes),
    foo_new = mnesia:table_info(foo, record_name),

    [{foo_new, 1, 1, <<"foo">>}] = mnesia:dirty_read(foo, 1),
    2 = mnesia:table_info(foo, size),

    %% The transform function will not match on the new records.
    {aborted, _} = mnevis:transform_table(foo, Transfrom, [key, val, blah], foo_new),
    ok.

transform({foo, K, V}, Val) ->
    {foo_new, K, V, Val}.

on_leader(M, F, A) ->
    {ok, _RaNodes, Leader} = ra:members(mnevis_node:node_id()),
    {_, LeaderNode} = Leader,
    ok = rpc:call(LeaderNode, M, F, A).

on_follower(M, F, A) ->
    {ok, RaNodes, Leader} = ra:members(mnevis_node:node_id()),
    [Follower | _] = RaNodes -- [Leader],
    {_, FollowerNode} = Follower,
    rpc:call(FollowerNode, M, F, A).
