-module(snapshot_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [{group, tests}].

groups() ->
    [
     {tests, [], [create_snapshot]}].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    filelib:ensure_dir(PrivDir),
    Nodes = mnevis_test_utils:create_initial_nodes(),
    mnevis_test_utils:start_cluster(Nodes, PrivDir),
    [ {nodes, Nodes} | Config].

end_per_suite(Config) ->
    Nodes = ?config(nodes, Config),
    [slave:stop(Node) || Node <- Nodes, Node =/= node()],
    ra:stop_server(mnevis_node:node_id()),
    application:stop(mnevis),
    application:stop(mnesia),
    application:stop(ra),
    Config.


init_per_testcase(_Test, Config) ->
    mnevis:transaction(fun() -> ok end),
    create_sample_table(),
    Config.

end_per_testcase(_Test, Config) ->
    delete_sample_table(),
    Config.

create_sample_table() ->
    % delete_sample_table(),
    mnevis:create_table(sample, []),
    ok.

delete_sample_table() ->
    mnevis:delete_table(sample),
    ok.

create_snapshot(Config) ->
    Nodes = ?config(nodes, Config),
    Node2P = lists:last(mnevis_test_utils:extra_nodes()),
    Node2 = lists:last(Nodes),
    slave:stop(Node2),

    {Time, _} = timer:tc(fun() ->
        [mnevis:transaction(fun() ->
            mnesia:write({sample, N, N})
        end)  || N <- lists:seq(1, 3000)]
    end),
    3000 = mnesia:table_info(sample, size),


    PrivDir = ?config(priv_dir, Config),
    Node2 = mnevis_test_utils:start_erlang_node(Node2P),
    mnevis_test_utils:start_node(Node2, Nodes, PrivDir),
    mnevis_test_utils:start_server(Node2, Nodes),

    ct:sleep(1000),

    % Node1 = lists:nth(2, Nodes),

    % slave:stop(Node1),

    % ra:members(mnevis_node:node_id()),

    {ok, {{LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(S) -> ok end, 100000),
    wait_for_index(Node2, LocalIndex),

    3000 = rpc:call(Node2, mnesia, table_info, [sample, size]).

wait_for_index(Node, Index) ->
    wait_for_index(Node, Index, 0).

wait_for_index(Node, Index, LastIndex) ->
    {Name, _} = mnevis_node:node_id(),
    {ok, {{NodeIndex, _}, _}, _} = ra:local_query({Name, Node}, fun(S) -> ok end, 100000),
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