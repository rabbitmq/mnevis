-module(perf_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").


all() -> [{group, tests}].

groups() ->
    [
     {tests, [], [
        mnevis_seq
        ,
        mnesia_seq
        ,
        mnevis_parallel
        ,
        mnesia_parallel
        ]}].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    ok = filelib:ensure_dir(PrivDir),
    application:load(mnesia),
    mnesia:create_schema([node()]),
    mnevis:start(PrivDir),
    mnevis_node:trigger_election(),
    Config.

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
    mnevis:create_table(sample, []),
    ok.

delete_sample_table() ->
    mnevis:delete_table(sample),
    ok.

mnevis_seq(_Config) ->
    [
    mnevis:transaction(fun() ->
        mnesia:write({sample, N, N})
    end)  || N <- lists:seq(1, 3000)
    ],
    {ok, {{LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(S) -> ok end),
    3000 = mnesia:table_info(sample, size).

mnesia_seq(_Config) ->
    mnesia_sync:start_link(),
    mnesia_sync:sync(),
    [
    begin
    mnesia:sync_transaction(fun() ->
        mnesia:write({sample, N, N})
    end) ,
    mnesia_sync:sync()
    end || N <- lists:seq(1, 3000)
    ],
    3000 = mnesia:table_info(sample, size).

mnevis_parallel(_Config) ->
    Self = self(),
    Pids = [spawn_link(fun() ->
        mnevis:transaction(fun() ->
            mnesia:write({sample, N, N})
        end),
        Self ! {stop, self()}
    end) || N <- lists:seq(1, 3000)],

    receive_results(Pids),
    {ok, {{LocalIndex, _}, _}, _} = ra:local_query(mnevis_node:node_id(), fun(S) -> ok end),
    ct:pal("Metrics ~p~n", [lists:ukeysort(1, ets:tab2list(ra_log_wal_metrics))]),
    ct:pal("Executed commands ~p~n", [LocalIndex]),
    ok.

mnesia_parallel(_Config) ->
    Self = self(),
    mnesia_sync:start_link(),
    mnesia_sync:sync(),
    Pids = [spawn_link(fun() ->
        mnesia:sync_transaction(fun() ->
            mnesia:write({sample, N, N})
        end),
        mnesia_sync:sync(),
        Self ! {stop, self()}
    end) || N <- lists:seq(1, 3000)],
    receive_results(Pids).

receive_results(Pids) ->
    [ receive {stop, Pid} -> ok end || Pid <- Pids ].