-module(mnevis_snapshot_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(NODE1, mnevis_snapshot_SUITE1).
-define(NODE2, mnevis_snapshot_SUITE2).

all() -> [{group, tests}].

groups() ->
    [
     {tests, [], [
        create_snapshot
        % ,
        % nesia_transaction
        % ,
        % multiple_processes
        ]}].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    filelib:ensure_dir(PrivDir),
    Nodes = create_initial_nodes(),
    start_cluster(Nodes, PrivDir),

    [ {nodes, Nodes} | Config].

create_initial_nodes() ->
    [node() | [start_erlang_node(NodeP) || NodeP <- [?NODE1, ?NODE2]]].

start_erlang_node(NodePrefix) ->
    {ok, Host} = inet:gethostname(),
    LocalPath = code:get_path(),
    {ok, Node} = slave:start(Host, NodePrefix),
    LocalPath = code:get_path(),
    add_paths(Node, LocalPath),
    Node.

add_paths(Node, LocalPath) ->
ct:pal("Rpc call code path ~p~n", [Node]),
    RemotePath = rpc:call(Node, code, get_path, []),
    AddPath = LocalPath -- RemotePath,
    ok = rpc:call(Node, code, add_pathsa, [AddPath]).

start_cluster(Nodes, PrivDir) ->
    [start_node(Node, Nodes, PrivDir) || Node <- Nodes],
    {Name, _} = mnevis_node:node_id(),
    Servers = [{Name, Node} || Node <- Nodes],
    {ok, _, _} = ra:start_cluster(Name, {module, mnevis_machine, #{}}, Servers).

start_server(Node, Nodes) ->
    {Name, _} = mnevis_node:node_id(),
    Servers = [{Name, Node} || Node <- Nodes],
    ra:start_server(Name, {Name, Node}, {module, mnevis_machine, #{}}, Servers).

start_node(Node, Nodes, PrivDir) ->
    Node = rpc:call(Node, erlang, node, []),
    rpc:call(Node, application, load, [ra]),
    ok = rpc:call(Node, application, set_env, [ra, data_dir, filename:join(PrivDir, Node)]),
    {ok, _} = rpc:call(Node, application, ensure_all_started, [mnevis]),
    ok.

node_dir(Node, Dir) ->
    filename:join(Dir, Node).

end_per_suite(Config) ->
    ra:stop_server(mnevis_node:node_id()),
    application:stop(mnevis),
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
    Node2 = lists:last(Nodes),
    slave:stop(Node2),

    {Time, _} = timer:tc(fun() ->
        [mnevis:transaction(fun() ->
            mnesia:write({sample, N, N})
        end)  || N <- lists:seq(1, 3000)]
    end),
    3000 = mnesia:table_info(sample, size),


    PrivDir = ?config(priv_dir, Config),
    Node2 = start_erlang_node(?NODE2),
    start_node(Node2, Nodes, PrivDir),
    start_server(Node2, Nodes),

    ct:sleep(1000),

    % Node1 = lists:nth(2, Nodes),

    % slave:stop(Node1),

    % ra:members(mnevis_node:node_id()),

    {Name, _} = mnevis_node:node_id(),

    {ok, {{LocalIndex, _}, _}, _} = ra:local_query({Name, node()}, fun(S) -> ok end),
    wait_for_index(Node2, LocalIndex),

    3000 = rpc:call(Node2, mnesia, table_info, [sample, size]).

wait_for_index(Node, Index) ->
    wait_for_index(Node, Index, 0).

wait_for_index(Node, Index, LastIndex) ->
    {Name, _} = mnevis_node:node_id(),
    {ok, {{NodeIndex, _}, _}, _} = ra:local_query({Name, Node}, fun(S) -> ok end),
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

multiple_processes(_Config) ->
    Self = self(),
    Pids = [spawn_link(fun() ->
        {Time, _} = timer:tc(fun() ->
            [mnevis:transaction(fun() ->
                [mnesia:write({sample, WN*N*PN, N}) || WN <- lists:seq(1, 10)]
             end)  || N <- lists:seq(1, 3)
            ]
        end),
        ct:pal("Time to process 10 transactions ~p~n", [Time]),
        Self ! {stop, self()}
    end) || PN <- lists:seq(1, 100)],
    receive_results(Pids).

receive_results(Pids) ->
    [ receive {stop, Pid} -> ok end || Pid <- Pids ].

mnesia_transaction(_Config) ->
    [
    mnesia:sync_transaction(fun() ->
        mnesia:write({sample, N, N})
    end)  || N <- lists:seq(1, 3000)
    ],
    3000 = mnesia:table_info(sample, size).
