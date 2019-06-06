-module(perf_SUITE).

-compile(nowarn_export_all).
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
    Config;
init_per_group(single_node, Config) ->
    Config;
init_per_group(three_nodes, Config) ->
    Config;
init_per_group(three_nodes_mnevis, Config0) ->
    {ok, Nodes} = mnevis_test_utils:create_initial_nodes(),
    % NB: necessary since tests use disc_copies
    ok = mnesia:create_schema(Nodes),
    {ok, Config1} = mnevis_test_utils:start_cluster(Nodes, Config0),
    [{nodes, Nodes} | Config1];
init_per_group(three_nodes_mnesia, Config) ->
    {ok, Nodes} = mnevis_test_utils:create_initial_nodes(),
    cluster_mnesia(Nodes),
    [{nodes, Nodes}, {mnesia_nodes, Nodes} | Config].

cluster_mnesia(Nodes) ->
    ok = mnesia:delete_schema(Nodes),
    ok = mnesia:create_schema(Nodes),
    [rpc:call(Node, mnesia, start, []) || Node <- Nodes].

end_per_group(_, Config) ->
    mnevis_test_utils:stop_all(Config).

init_per_testcase(_Test, Config) ->
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
        _ ->
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

mnevis_seq_N(N0, _Config) ->
    TF = case N0 of
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
                {IT, _} = timer:tc(fun() -> TF(N1) end),
                IT
            end]),
            {Time, InnerTime}
        end || N1 <- lists:seq(1, 1000)],
    {OutTimes, InTimes} = lists:unzip(Times),
    ct:pal("Times for transaction ~p~n", [lists:sum(OutTimes)]),
    ct:pal("Times for write ~p~n", [lists:sum(InTimes)]),
    {ok, {{_LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(_) -> ok end),
    1000 = mnesia:table_info(sample, size).

mnesia_seq_N(N0, Config) ->
    Nodes = case ?config(nodes, Config) of
        undefined -> [node()];
        Ns -> Ns
    end,
    [rpc:call(Node, mnesia_sync, start_link, []) || Node <- Nodes],
    % mnesia_sync:start_link(),
    mnesia_sync:sync(),
    TF = case N0 of
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
            {IT, _} = timer:tc(fun() -> TF(N1) end),
            IT
        end),
        % timer:sleep(10),
        [rpc:call(Node, mnesia_sync, sync, []) || Node <- Nodes],
        % mnesia_sync:sync(),
        InnerTime
    end),
    {Time, InnerTime1}
    end || N1 <- lists:seq(1, 1000)
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


mnevis_parallel(_Config, N0) ->
    Self = self(),
    Pids = [spawn_link(fun() ->
        mnevis:transaction(fun() ->
            mnesia:write({sample, N1, N1}),
            mnesia:write({sample1, N1, N1}),
            mnesia:write({sample2, N1, N1}),
            mnesia:write({sample3, N1, N1})
        end),
        Self ! {stop, self()}
    end) || N1 <- lists:seq(1, N0)],

    receive_results(Pids),
    {ok, {{_LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(_) -> ok end),
    % ct:pal("Metrics ~p~n", [lists:ukeysort(1, ets:tab2list(ra_log_wal_metrics))]),
    % ct:pal("Executed commands ~p~n", [LocalIndex]),
    ok.

mnesia_parallel(_Config, N0) ->
    Self = self(),
    mnesia_sync:start_link(),
    mnesia_sync:sync(),
    Pids = [spawn_link(fun() ->
        mnesia:sync_transaction(fun() ->
            mnesia:write({sample, N1, N1}),
            mnesia:write({sample1, N1, N1}),
            mnesia:write({sample2, N1, N1}),
            mnesia:write({sample3, N1, N1})
        end),
        mnesia_sync:sync(),
        Self ! {stop, self()}
    end) || N1 <- lists:seq(1, N0)],
    receive_results(Pids).

receive_results(Pids) ->
    [ receive {stop, Pid} -> ok end || Pid <- Pids ].
