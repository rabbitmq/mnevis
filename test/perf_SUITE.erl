-module(perf_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(NODE1, mnevis_snapshot_SUITE1).
-define(NODE2, mnevis_snapshot_SUITE2).

all() -> [{group, single_node}, {group, three_nodes}].

groups() ->
    [
     {single_node, [], [{group, single_node_seq}, {group, single_node_parallel}]},
     {single_node_seq, [], [
        mnevis_seq,
        mnesia_seq,
        mnevis_seq_4,
        mnesia_seq_4
    ]},
     {single_node_parallel, [], [
        mnevis_parallel_100,
        mnesia_parallel_100,
        mnevis_parallel_500,
        mnesia_parallel_500,
        mnevis_parallel_1000,
        mnesia_parallel_1000,
        mnevis_parallel_5000,
        mnesia_parallel_5000,
        mnevis_parallel_10000,
        mnesia_parallel_10000
        ]}
        ,
     {three_nodes, [], [{group, three_nodes_mnevis}, {group, three_nodes_mnesia}]},
     {three_nodes_mnevis, [], [
        mnevis_seq,
        mnevis_parallel_500,
        mnevis_parallel_1000,
        mnevis_parallel_5000
     ]},
     {three_nodes_mnesia, [], [
        mnesia_seq,
        mnesia_parallel_500,
        mnesia_parallel_1000,
        mnesia_parallel_5000
        ]}
     ].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    ok = filelib:ensure_dir(PrivDir),
    application:load(mnesia),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(SingleNode, Config) when SingleNode == single_node_parallel; SingleNode == single_node_seq ->
    PrivDir = ?config(priv_dir, Config),
    mnesia:create_schema([node()]),
    mnevis:start(PrivDir),
    % mnevis_node:trigger_election(),
    Config;
init_per_group(single_node, Config) ->
    Config;
init_per_group(three_nodes, Config) ->
    Config;
init_per_group(three_nodes_mnevis, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Nodes = create_initial_nodes("mnevis"),
    start_mnevis_cluster(Nodes, PrivDir),
    [ {nodes, Nodes} | Config];
init_per_group(three_nodes_mnesia, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Nodes = create_initial_nodes("mnesia"),
    cluster_mnesia(Nodes),
    [ {nodes, Nodes}, {mnesia_nodes, Nodes} | Config].

create_initial_nodes(Name) ->
    [node() | [start_erlang_node(NodeP) || NodeP <- [list_to_atom(Name ++ "1"),
                                                     list_to_atom(Name ++ "2")]]].

start_erlang_node(NodePrefix) ->
    {ok, Host} = inet:gethostname(),
    LocalPath = code:get_path(),
    {ok, Node} = slave:start(Host, NodePrefix),
    LocalPath = code:get_path(),
    add_paths(Node, LocalPath),
    Node.

add_paths(Node, LocalPath) ->
    RemotePath = rpc:call(Node, code, get_path, []),
    AddPath = LocalPath -- RemotePath,
    ok = rpc:call(Node, code, add_pathsa, [AddPath]).

start_mnevis_cluster(Nodes, PrivDir) ->
    [start_node(Node, Nodes, PrivDir) || Node <- Nodes],
    {Name, _} = mnevis_node:node_id(),
    Servers = [{Name, Node} || Node <- Nodes],
    {ok, _, _} = ra:start_cluster(Name, {module, mnevis_machine, #{}}, Servers).

start_node(Node, Nodes, PrivDir) ->
    Node = rpc:call(Node, erlang, node, []),
    rpc:call(Node, application, load, [ra]),
    rpc:call(Node, application, load, [mnesia]),
    rpc:call(Node, mnesia, create_schema, [[Node]]),
    ok = rpc:call(Node, application, set_env, [ra, data_dir, filename:join(PrivDir, Node)]),
    {ok, _} = rpc:call(Node, application, ensure_all_started, [mnevis]),
    ok.

cluster_mnesia(Nodes) ->
    [rpc:call(Node, mnesia, delete_schema, [[Node]]) || Node <- Nodes],

    mnesia:create_schema(Nodes),
    [rpc:call(Node, mnesia, start, []) || Node <- Nodes].

end_per_group(SingleNode, Config) when SingleNode == single_node_parallel; SingleNode == single_node_seq  ->
    ra:stop_server(mnevis_node:node_id()),
    application:stop(mnevis),
    application:stop(mnesia),
    mnesia:delete_schema([node()]),
    application:stop(ra),
    Config;
end_per_group(single_node, Config) ->
    Config;
end_per_group(three_nodes, Config) ->
    Config;
end_per_group(three_nodes_mnevis, Config) ->
    Nodes = ?config(nodes, Config),
    [slave:stop(Node) || Node <- Nodes, Node =/= node()],
    ra:stop_server(mnevis_node:node_id()),
    application:stop(mnevis),
    application:stop(mnesia),
    mnesia:delete_schema([node()]),
    application:stop(ra),
    Config;
end_per_group(three_nodes_mnesia, Config) ->
    Nodes = ?config(nodes, Config),
    [slave:stop(Node) || Node <- Nodes, Node =/= node()],
    application:stop(mnesia),
    mnesia:delete_schema(Nodes),
    Config.

init_per_testcase(Test, Config) ->
    mnevis:transaction(fun() -> ok end),
    case ?config(mnesia_nodes, Config) of
        undefined ->
            create_sample_table();
        Nodes ->
            ct:pal("Nodes ~p~n", [Nodes]),
            create_sample_table_mnesia(Nodes)
    end,

    Config.

end_per_testcase(_Test, Config) ->
    case ?config(mnesia_nodes, Config) of
        undefined ->
            delete_sample_table();
        Nodes ->
            delete_sample_table_mnesia()
    end,
    Config.

create_sample_table_mnesia(Nodes) ->
    mnesia:create_table(sample, [{disc_copies, Nodes}]),
    mnesia:create_table(sample1, [{disc_copies, Nodes}]),
    mnesia:create_table(sample2, [{disc_copies, Nodes}]),
    mnesia:create_table(sample3, [{disc_copies, Nodes}]),
    ok.

create_sample_table() ->
    {atomic, ok} = mnevis:create_table(sample, [{disc_copies, [node()]}]),
    {atomic, ok} = mnevis:create_table(sample1, [{disc_copies, [node()]}]),
    {atomic, ok} = mnevis:create_table(sample2, [{disc_copies, [node()]}]),
    {atomic, ok} = mnevis:create_table(sample3, [{disc_copies, [node()]}]),
    ok.

delete_sample_table() ->
    mnevis:delete_table(sample),
    mnevis:delete_table(sample1),
    mnevis:delete_table(sample2),
    mnevis:delete_table(sample3),
    ok.

delete_sample_table_mnesia() ->
    mnesia:delete_table(sample),
    mnesia:delete_table(sample1),
    mnesia:delete_table(sample2),
    mnesia:delete_table(sample3),
    ok.


mnevis_seq(Config) ->
    mnevis_seq_N(1, Config).

mnevis_seq_4(Config) ->
    mnevis_seq_N(4, Config).

mnesia_seq(Config) ->
    mnesia_seq_N(1, Config).

mnesia_seq_4(Config) ->
    mnesia_seq_N(4, Config).

mnevis_seq_N(N, _Config) ->
    TF = case N of
        4 ->
            fun(Sample) ->
                mnesia:write({sample, Sample, Sample}),
                mnesia:write({sample1, Sample, Sample}),
                mnesia:write({sample2, Sample, Sample}),
                mnesia:write({sample3, Sample, Sample})
            end;
        1 ->
            fun(Sample) ->
                mnesia:write({sample, Sample, Sample})
            end
    end,
    Times = [
    begin
        {Time, {atomic, InnerTime}} = timer:tc(mnevis, transaction, [fun() ->
            {IT, _} = timer:tc(fun() -> TF(N) end),
            IT
        end]),
        {Time, InnerTime}
    end
    || N <- lists:seq(1, 1000)
    ],
    {OutTimes, InTimes} = lists:unzip(Times),
    ct:pal("Times for transaction ~p~n", [lists:sum(OutTimes)]),
    ct:pal("Times for write ~p~n", [lists:sum(InTimes)]),
    {ok, {{LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(S) -> ok end),
    1000 = mnesia:table_info(sample, size).

mnesia_seq_N(N, Config) ->
    Nodes = case ?config(nodes, Config) of
        undefined -> [node()];
        Ns -> Ns
    end,
    [rpc:call(Node, mnesia_sync, start_link, []) || Node <- Nodes],
    % mnesia_sync:start_link(),
    mnesia_sync:sync(),
    TF = case N of
        4 ->
            fun(Sample) ->
                mnesia:write({sample, Sample, Sample}),
                mnesia:write({sample1, Sample, Sample}),
                mnesia:write({sample2, Sample, Sample}),
                mnesia:write({sample3, Sample, Sample})
            end;
        1 ->
            fun(Sample) ->
                mnesia:write({sample, Sample, Sample})
            end
    end,
    Times = [
    begin
    {Time, InnerTime1} = timer:tc(fun() ->
        {atomic, InnerTime} = mnesia:sync_transaction(fun() ->
            {IT, _} = timer:tc(fun() -> TF(N) end),
            IT
        end),
        % timer:sleep(10),
        [rpc:call(Node, mnesia_sync, sync, []) || Node <- Nodes],
        % mnesia_sync:sync(),
        InnerTime
    end),
    {Time, InnerTime1}
    end || N <- lists:seq(1, 1000)
    ],
    SyncTime = mnesia_sync:get_time(),
    {OutTimes, InTimes} = lists:unzip(Times),
    ct:pal("Times for transaction ~p~n", [lists:sum(OutTimes)]),
    ct:pal("Times for write ~p~n", [lists:sum(InTimes)]),
    ct:pal("Times for sync ~p~n", [SyncTime]),
    1000 = mnesia:table_info(sample, size).


mnevis_parallel_100(Config) ->
    mnevis_parallel(Config, 100).

mnevis_parallel_1000(Config) ->
    mnevis_parallel(Config, 1000).

mnevis_parallel_10000(Config) ->
    mnevis_parallel(Config, 10000).

mnevis_parallel_5000(Config) ->
    mnevis_parallel(Config, 5000).

mnevis_parallel_500(Config) ->
    mnevis_parallel(Config, 500).

mnesia_parallel_100(Config) ->
    mnesia_parallel(Config, 100).

mnesia_parallel_1000(Config) ->
    mnesia_parallel(Config, 1000).

mnesia_parallel_10000(Config) ->
    mnesia_parallel(Config, 10000).

mnesia_parallel_5000(Config) ->
    mnesia_parallel(Config, 5000).

mnesia_parallel_500(Config) ->
    mnesia_parallel(Config, 500).


mnevis_parallel(_Config, N) ->
    Self = self(),
    Pids = [spawn_link(fun() ->
        mnevis:transaction(fun() ->
            mnesia:write({sample, N, N}),
            mnesia:write({sample1, N, N}),
            mnesia:write({sample2, N, N}),
            mnesia:write({sample3, N, N})
        end),
        Self ! {stop, self()}
    end) || N <- lists:seq(1, N)],

    receive_results(Pids),
    {ok, {{LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(S) -> ok end),
    % ct:pal("Metrics ~p~n", [lists:ukeysort(1, ets:tab2list(ra_log_wal_metrics))]),
    % ct:pal("Executed commands ~p~n", [LocalIndex]),
    ok.

mnesia_parallel(_Config, N) ->
    Self = self(),
    mnesia_sync:start_link(),
    mnesia_sync:sync(),
    Pids = [spawn_link(fun() ->
        mnesia:sync_transaction(fun() ->
            mnesia:write({sample, N, N}),
            mnesia:write({sample1, N, N}),
            mnesia:write({sample2, N, N}),
            mnesia:write({sample3, N, N})
        end),
        mnesia_sync:sync(),
        Self ! {stop, self()}
    end) || N <- lists:seq(1, N)],
    receive_results(Pids).

receive_results(Pids) ->
    [ receive {stop, Pid} -> ok end || Pid <- Pids ].