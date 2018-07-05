-module(ramnesia_isolation_SUITE).

-compile(export_all).

all() -> [{group, tests}].

groups() ->
    [
     {tests, [], [
        write_invisible_outside_transaction,
        delete_invisible_outside_transaction,
        consistent_counter
        %,
        % conststent_register
        ]}].

init_per_suite(Config) ->
    ramnesia:start(),
    Config.

end_per_suite(Config) -> Config.


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

add_sample(Key, Val) ->
    {atomic, ok} = ramnesia:transaction(fun() ->
        mnesia:write({sample, Key, Val})
    end),
    {atomic, [{sample, Key, Val}]} = ramnesia:transaction(fun() ->
        mnesia:read(sample, Key)
    end),
    ok.

write_invisible_outside_transaction(_Config) ->
    add_sample(foo, bar),
    BackgroundTransaction = spawn_link(fun() ->
        ramnesia:transaction(fun() ->
            %% Update foo key
            mnesia:write({sample, foo, baz}),
            %% Add a new key
            mnesia:write({sample, bar, baz}),
            receive stop -> ok
            end
        end)
    end),
    ramnesia:transaction(fun() ->
        [{sample, foo, bar}] = mnesia:read(sample, foo),
        [] = mnesia:read(sample, bar)
    end),
    BackgroundTransaction ! stop.

delete_invisible_outside_transaction(_Config) ->
    add_sample(foo, bar),
    add_sample(bar, baz),
    BackgroundTransaction = spawn_link(fun() ->
        ramnesia:transaction(fun() ->
            mnesia:delete({sample, foo}),
            mnesia:delete_object({sample, bar, baz}),
            receive stop -> ok
            end
        end)
    end),
    ramnesia:transaction(fun() ->
        [{sample, foo, bar}] = mnesia:read(sample, foo),
        [{sample, bar, baz}] = mnesia:read(sample, bar)
    end),
    BackgroundTransaction ! stop.

consistent_counter(_Config) ->
    add_sample(foo, 0),
    UpdateCounter = fun() ->
        timer:sleep(100),
        [{sample, foo, N}] = mnesia:read(sample, foo),
        timer:sleep(100),
        ok = mnesia:write({sample, foo, N+1}),
        % timer:sleep(100),
        ok
    end,
    % Spawn 100 updates
    Updates = [ spawn_link(fun() ->
                    {atomic, ok} = ramnesia:transaction(UpdateCounter)
                end)
                || _ <- lists:seq(1, 10) ],
    timer:sleep(600),
    wait_for_finish(Updates),
    {atomic, ok} = ramnesia:transaction(fun() ->
        [{sample, foo, 100}] = mnesia:read(sample, foo),
        ok
    end).

wait_for_finish([Pid | Pids]) ->
    case process_info(Pid) of
        undefined -> wait_for_finish(Pids);
        _ -> timer:sleep(100), wait_for_finish([Pid | Pids])
    end.


