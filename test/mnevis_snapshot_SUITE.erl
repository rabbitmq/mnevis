-module(mnevis_snapshot_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [{group, tests}].

groups() ->
    [
     {tests, [], [
        create_snapshot,
        mnesia_transaction,
        multiple_processes
        ]}].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    filelib:ensure_dir(PrivDir),
    mnevis:start(PrivDir),
    mnevis_node:trigger_election(),
    Config.

end_per_suite(Config) ->
    ra:stop_server(mnevis_node:node_id()),
    application:stop(mnevis),
    application:stop(ra),
    Config.


init_per_testcase(_Test, Config) ->
    create_sample_table(),
    Config.

end_per_testcase(_Test, Config) ->
    delete_sample_table(),
    Config.

create_sample_table() ->
    delete_sample_table(),
    mnesia:create_table(sample, []),
    ok.

delete_sample_table() ->
    mnesia:delete_table(sample),
    ok.

create_snapshot(_Config) ->
    {Time, _} = timer:tc(fun() ->
        [mnevis:transaction(fun() ->
            mnesia:write({sample, N, N})
        end)  || N <- lists:seq(1, 3000)]
    end),
    ct:pal("Time to process 3000 msgs ~p~n", [Time]).

multiple_processes(_Config) ->
    Self = self(),
    Pids = [spawn_link(fun() ->
        {Time, _} = timer:tc(fun() ->
            [mnevis:transaction(fun() ->
                mnesia:write({sample, N*PN, N})
             end)  || N <- lists:seq(1, 30)
            ]
        end),
        ct:pal("Time to process 30 msgs ~p~n", [Time]),
        Self ! {stop, self()}
    end) || PN <- lists:seq(1, 100)],
    receive_results(Pids).

receive_results(Pids) ->
    [ receive {stop, Pid} -> ok end || Pid <- Pids ].

mnesia_transaction(_Config) ->
    [
    mnesia:transaction(fun() ->
        mnesia:write({sample, N, N})
    end)  || N <- lists:seq(1, 3000)
    ].
