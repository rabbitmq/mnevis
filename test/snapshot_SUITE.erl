-module(snapshot_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [{group, tests}].

groups() ->
    [
     {tests, [], [create_snapshot]}].

init_per_suite(Config0) ->
    {ok, Nodes} = mnevis_test_utils:create_initial_nodes(?MODULE),
    {ok, Config1} = mnevis_test_utils:start_cluster(Nodes, Config0),
    [{nodes, Nodes} | Config1].

end_per_suite(Config) ->
    mnevis_test_utils:stop_all(Config).

init_per_testcase(_Test, Config) ->
    mnevis:transaction(fun() -> ok end),
    create_sample_table(),
    Config.

end_per_testcase(_Test, Config) ->
    delete_sample_table(),
    Config.

create_sample_table() ->
    mnevis:create_table(sample, []),
    ok.

delete_sample_table() ->
    mnevis:delete_table(sample),
    ok.

create_snapshot(Config) ->
    Nodes = ?config(nodes, Config),
    Node2 = lists:last(Nodes),
    ok = slave:stop(Node2),

    {_Time, _} = timer:tc(fun() ->
        [mnevis:transaction(fun() ->
            mnesia:write({sample, N, N})
        end)  || N <- lists:seq(1, 3000)]
    end),

    3000 = mnesia:table_info(sample, size),

    {ok, Node2} = mnevis_test_utils:restart_slave(Node2, Config),

    ct:sleep(1000),

    {ok, {{LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(_) -> ok end, 100000),
    wait_for_index(Node2, LocalIndex),

    3000 = ct_rpc:call(Node2, mnesia, table_info, [sample, size]).

wait_for_index(Node, Index) ->
    wait_for_index(Node, Index, 0).

wait_for_index(Node, Index, LastIndex) ->
    {Name, _} = mnevis_node:node_id(),
    {ok, {{NodeIndex, _}, _}, _} = ra:local_query({Name, Node}, fun(_) -> ok end, 100000),
    case NodeIndex >= Index of
        true ->
            ok;
        false ->
            %% If index doesn't change - stop waiting
            case NodeIndex == LastIndex of
                true ->
                    ok;
                false ->
                    ct:sleep(500),
                    wait_for_index(Node, Index)
            end
    end.
