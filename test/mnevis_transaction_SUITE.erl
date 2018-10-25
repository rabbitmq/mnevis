

-module(mnevis_transaction_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).


all() ->
    [{group, tests}].

groups() ->
    [
     {tests, [], [
        empty_transaction,
        empty_transaction_abort,
        empty_transaction_error,
        nested_empty_transaction,
        nested_empty_transaction_abort,
        nested_empty_transaction_error,

        transaction_abort,

        writes_aborted,
        deletes_aborted,
        delete_object_aborted,

        lock_aborts_if_no_retries,
        lock_succedes_eventually_on_retries,
        operations_succeed_while_different_key_locked,

        read_cached_state,
        index_read_cached_state,
        match_object_cached_state,
        index_match_object_cached_state,
        all_keys_cached_state,
        first_cached_state,
        last_cached_state,
        prev_cached_state,
        next_cached_state,

        foldl_cached_state,

        write_delete_converge,
        write_delete_object_converge,
        write_bag_delete_converge,
        write_bag_delete_object_converge
     ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    ok = filelib:ensure_dir(PrivDir),
    ct:pal("~nPriv dir ~p~n", [PrivDir]),
    mnevis:start(PrivDir),
    mnevis_node:trigger_election(),
    Config.

end_per_suite(Config) ->
    ra:stop_server(mnevis_node:node_id()),
    application:stop(mnevis),
    application:stop(ra),
    % mnesia:delete_table(committed_transaction),
    Config.

%% -------------------------------------------------------------------
%% Groups.
%% -------------------------------------------------------------------

init_per_group(_, Config) -> Config.

end_per_group(_, Config) -> Config.

init_per_testcase(deletes_aborted, Config) ->
    create_sample_tables(),
    add_sample({sample, foo, bar}),
    Config;
init_per_testcase(delete_object_aborted, Config) ->
    create_sample_tables(),
    add_sample({sample, foo, bar}),
    Config;
init_per_testcase(read_cached_state, Config) ->
    create_sample_tables(),
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    Config;
init_per_testcase(index_read_cached_state, Config) ->
    create_sample_tables(),
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    Config;
init_per_testcase(match_object_cached_state, Config) ->
    create_sample_tables(),
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    Config;
init_per_testcase(index_match_object_cached_state, Config) ->
    create_sample_tables(),
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    Config;
init_per_testcase(all_keys_cached_state, Config) ->
    create_sample_tables(),
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    Config;
init_per_testcase(_Test, Config) ->
    create_sample_tables(),
    Config.

end_per_testcase(_Test, Config) ->
    delete_sample_tables(),
    Config.

create_sample_tables() ->
    {atomic, ok} = mnesia:create_table(sample, [{type, set}, {index, [3]}]),
    {atomic, ok} = mnesia:create_table(sample_bag, [{type, bag}, {index, [3]}]),
    {atomic, ok} = mnesia:create_table(sample_ordered_set, [{type, ordered_set}, {index, [3]}]),
    ok.

delete_sample_tables() ->
    {atomic, ok} = mnesia:delete_table(sample),
    {atomic, ok} = mnesia:delete_table(sample_bag),
    {atomic, ok} = mnesia:delete_table(sample_ordered_set),
    ok.

add_sample({Tab, Key, Val}) ->
    {atomic, ok} = mnevis:transaction(fun() ->
        mnesia:write({Tab, Key, Val})
    end),
    {atomic, Read} = mnevis:transaction(fun() ->
        mnesia:read(Tab, Key)
    end),
    true = lists:member({Tab, Key, Val}, Read).

delete_sample(Tab, Key) ->
    {atomic, ok} = mnevis:transaction(fun() ->
        mnesia:delete({Tab, Key}),
        [] = mnesia:read(Tab, Key),
        ok
    end).

empty_transaction(_Config) ->
    {atomic, ok} = mnevis:transaction(fun() -> ok end).
empty_transaction_abort(_Config) ->
    {aborted, error} = mnevis:transaction(fun() -> mnesia:abort(error) end).
empty_transaction_error(_Config) ->
    {aborted, {reason, _ST}} = mnevis:transaction(fun() -> error(reason) end).


nested_empty_transaction(_Config) ->
    {atomic, ok} = mnevis:transaction(fun() ->
        {atomic, ok} = mnevis:transaction(fun() -> ok end),
        ok
    end).
nested_empty_transaction_abort(_Config) ->
    {aborted, error} = mnevis:transaction(fun() ->
        mnevis:transaction(fun() -> mnesia:abort(error) end)
    end).
nested_empty_transaction_error(_Config) ->
    {aborted, {reason, _ST}} = mnevis:transaction(fun() ->
        mnevis:transaction(fun() -> error(reason) end)
    end).

transaction_abort(_Config) ->
    {aborted, {no_exists, [foo,foo]}} = mnevis:transaction(fun() ->
        mnesia:read(foo, foo)
    end).

writes_aborted(Config) ->
    writes_aborted(Config, sample),
    writes_aborted(Config, sample_bag),
    writes_aborted(Config, sample_ordered_set).

writes_aborted(_Config, Tab) ->
    {aborted, {read, ReadAborted}} = mnevis:transaction(fun() ->
        mnesia:write({Tab, foo, bar}),
        Read = mnesia:read(Tab, foo),
        mnesia:abort({read, Read})
    end),
    %% Data was readable in transaction
    [{Tab, foo, bar}] = ReadAborted,
    %% Data is not in mnesia
    [] = mnesia:dirty_read(Tab, foo),
    %% Data is not in transaction
    {atomic, []} = mnesia:transaction(fun() -> mnesia:read(Tab, foo) end).

deletes_aborted(_Config) ->
    {aborted, abort_delete} = mnevis:transaction(fun() ->
        mnesia:delete({sample, foo}),
        [] = mnesia:read(sample, foo),
        mnesia:abort(abort_delete)
    end),

    %% Value is still there
    {atomic, [{sample, foo, bar}]} = mnevis:transaction(fun() ->
        mnesia:read(sample, foo)
    end).

delete_object_aborted(_Config) ->
    {aborted, abort_delete_object} = mnevis:transaction(fun() ->
        mnesia:delete_object({sample, foo, bar}),
        [] = mnesia:read(sample, foo),
        mnesia:abort(abort_delete_object)
    end),

    %% Value is still there
    {atomic, [{sample, foo, bar}]} = mnevis:transaction(fun() ->
        mnesia:read(sample, foo)
    end).

lock_aborts_if_no_retries(_Config) ->
    lock_for(2000, {sample, foo}, write),
    receive locked ->
        {aborted, locked} = mnevis:transaction(fun() ->
            mnesia:lock({sample, foo}, write)
        end, [], 10)
    after 2000 ->
        error(not_locked)
    end.
lock_succedes_eventually_on_retries(_Config) ->
    lock_for(2000, {sample, foo}, write),
    {atomic, ok} = mnevis:transaction(fun() ->
        mnesia:lock({sample, foo}, write)
    end).

lock_for(Time, LockItem, LockKind) ->
    Pid = self(),
    spawn(fun() ->
        mnevis:transaction(fun() ->
            mnesia:lock(LockItem, LockKind),
            Pid ! locked,
            timer:sleep(Time)
        end)
    end).

operations_succeed_while_different_key_locked(_Config) ->
    lock_for(2000, {sample, foo}, write),
    {atomic, [{sample, bar, val}]} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample, bar, val}),
        [{sample, bar, val}] = mnesia:read(sample, bar)
    end, [], 1).

read_cached_state(_Config) ->
    {aborted, foo} = mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:read(sample, foo),
        [{sample_bag, foo, baj},
         {sample_bag, foo, bar},
         {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),
        [{sample_ordered_set, foo, bar}] = mnesia:read(sample_ordered_set, foo),
        %% Do some changes
        %% Overwrite existing
        ok = mnesia:write({sample, foo, baz}),
        ok = mnesia:write({sample_ordered_set, foo, baz}),
        %% Add new
        ok = mnesia:write({sample, bar, baz}),
        ok = mnesia:write({sample_ordered_set, bar, baz}),
        [{sample, foo, baz}] = mnesia:read(sample, foo),
        [{sample, bar, baz}] = mnesia:read(sample, bar),

        [{sample_ordered_set, foo, baz}] = mnesia:read(sample_ordered_set, foo),
        [{sample_ordered_set, bar, baz}] = mnesia:read(sample_ordered_set, bar),

        %% Add new to bag
        ok = mnesia:write({sample_bag, foo, baq}),
        [{sample_bag, foo, baj},
         {sample_bag, foo, baq},
         {sample_bag, foo, bar},
         {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),

        %% Delete from set
        ok = mnesia:delete({sample, foo}),
        [] = mnesia:read(sample, foo),
        ok = mnesia:delete({sample_ordered_set, bar}),
        [] = mnesia:read(sample_ordered_set, bar),

        %% Delete object from set
        ok = mnesia:delete_object({sample, bar, bazz}),
        [{sample, bar, baz}] = mnesia:read(sample, bar),
        ok = mnesia:delete_object({sample, bar, baz}),
        [] = mnesia:read(sample, bar),

        ok = mnesia:delete_object({sample_ordered_set, foo, bazz}),
        [{sample_ordered_set, foo, baz}] = mnesia:read(sample_ordered_set, foo),
        ok = mnesia:delete_object({sample_ordered_set, foo, baz}),
        [] = mnesia:read(sample_ordered_set, foo),

        %% Delete object from bag
        ok = mnesia:delete_object({sample_bag, foo, bar}),
        [{sample_bag, foo, baj},
         {sample_bag, foo, baq},
         {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),

        ok = mnesia:delete_object({sample_bag, foo, bazz}),
        [{sample_bag, foo, baj},
         {sample_bag, foo, baq},
         {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),

        ok = mnesia:delete_object({sample_bag, foo, baq}),
        [{sample_bag, foo, baj},
         {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),

        %% Delete from bag
        mnesia:delete({sample_bag, foo}),
        [] = mnesia:read(sample_bag, foo),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:read(sample, foo),
        [{sample_bag, foo, baj},
         {sample_bag, foo, bar},
         {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),
        [{sample_ordered_set, foo, bar}] = mnesia:read(sample_ordered_set, foo)
    end).


index_read_cached_state(_Config) ->
    {aborted, foo} = mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:index_read(sample, bar, 3),
        [{sample_bag, foo, bar}] = mnesia:index_read(sample_bag, bar, 3),
        [{sample_ordered_set, foo, bar}] = mnesia:index_read(sample_ordered_set, bar, 3),
        %% Do some changes
        %% Overwrite existing
        ok = mnesia:write({sample, foo, baz}),
        ok = mnesia:write({sample_ordered_set, foo, baz}),
        %% Add new
        ok = mnesia:write({sample, bar, baz}),
        ok = mnesia:write({sample_ordered_set, bar, baz}),
        [] = mnesia:index_read(sample, bar, 3),
        [{sample, bar, baz}, {sample, foo, baz}] = lists:usort(mnesia:index_read(sample, baz, 3)),

        [] = mnesia:index_read(sample_ordered_set, bar, 3),
        [{sample_ordered_set, bar, baz}, {sample_ordered_set, foo, baz}] =
            lists:usort(mnesia:index_read(sample_ordered_set, baz, 3)),

        % %% Add new to bag
        ok = mnesia:write({sample_bag, foo, baq}),
        [{sample_bag, foo, baq}] = mnesia:index_read(sample_bag, baq, 3),

        % %% Delete from set
        ok = mnesia:delete({sample, foo}),
        [{sample, bar, baz}] = mnesia:index_read(sample, baz, 3),
        ok = mnesia:delete({sample_ordered_set, bar}),
        [{sample_ordered_set, foo, baz}] = mnesia:index_read(sample_ordered_set, baz, 3),

        % %% Delete object from set
        ok = mnesia:delete_object({sample, bar, bazz}),
        [{sample, bar, baz}] = mnesia:index_read(sample, baz, 3),
        ok = mnesia:delete_object({sample, bar, baz}),
        [] = mnesia:index_read(sample, baz, 3),

        ok = mnesia:delete_object({sample_ordered_set, foo, bazz}),
        [{sample_ordered_set, foo, baz}] = mnesia:index_read(sample_ordered_set, baz, 3),
        ok = mnesia:delete_object({sample_ordered_set, foo, baz}),
        [] = mnesia:index_read(sample_ordered_set, baz, 3),

        % %% Delete object from bag
        ok = mnesia:delete_object({sample_bag, foo, bar}),
        [] = mnesia:index_read(sample_bag, bar, 3),

        ok = mnesia:delete_object({sample_bag, foo, baq}),
        [] = mnesia:index_read(sample_bag, baq, 3),

        % %% Delete from bag
        mnesia:delete({sample_bag, foo}),
        [] = mnesia:index_read(sample_bag, baz, 3),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:index_read(sample, bar, 3),
        [{sample_bag, foo, bar}] = mnesia:index_read(sample_bag, bar, 3),
        [{sample_ordered_set, foo, bar}] = mnesia:index_read(sample_ordered_set, bar, 3)
    end).

match_object_cached_state(_Config) ->
    {aborted, foo} = mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:match_object({sample, foo, bar}),
        [{sample_bag, foo, bar}] = mnesia:match_object({sample_bag, foo, bar}),
        [{sample_ordered_set, foo, bar}] = mnesia:match_object({sample_ordered_set, foo, bar}),
        %% Do some changes
        %% Overwrite existing
        ok = mnesia:write({sample, foo, baz}),
        ok = mnesia:write({sample_ordered_set, foo, baz}),
        %% Add new
        ok = mnesia:write({sample, bar, baz}),
        ok = mnesia:write({sample_ordered_set, bar, baz}),
        [] = mnesia:match_object({sample, foo, bar}),
        [{sample, foo, baz}] = mnesia:match_object({sample, foo, baz}),
        [{sample, bar, baz}] = mnesia:match_object({sample, bar, '_'}),
        [{sample, bar, baz}, {sample, foo, baz}] = lists:usort(mnesia:match_object({sample, '_', baz})),

        [] = mnesia:match_object({sample_ordered_set, '_', bar}),
        [{sample_ordered_set, bar, baz}, {sample_ordered_set, foo, baz}] =
            lists:usort(mnesia:match_object({sample_ordered_set, '_', baz})),

        %% Add new to bag
        ok = mnesia:write({sample_bag, foo, baq}),
        [{sample_bag, foo, baq}] = mnesia:match_object({sample_bag, '_', baq}),

        %% Delete from set
        ok = mnesia:delete({sample, foo}),
        [{sample, bar, baz}] = mnesia:match_object({sample, '_', baz}),
        ok = mnesia:delete({sample_ordered_set, bar}),
        [{sample_ordered_set, foo, baz}] = mnesia:match_object({sample_ordered_set, '_', baz}),

        %% Delete object from set
        ok = mnesia:delete_object({sample, bar, bazz}),
        [{sample, bar, baz}] = mnesia:match_object({sample, '_', baz}),
        ok = mnesia:delete_object({sample, bar, baz}),
        [] = mnesia:match_object({sample, '_', baz}),

        ok = mnesia:delete_object({sample_ordered_set, foo, bazz}),
        [{sample_ordered_set, foo, baz}] = mnesia:match_object({sample_ordered_set, '_', baz}),
        ok = mnesia:delete_object({sample_ordered_set, foo, baz}),
        [] = mnesia:match_object({sample_ordered_set, '_', baz}),

        %% Delete object from bag
        ok = mnesia:delete_object({sample_bag, foo, bar}),
        [] = mnesia:match_object({sample_bag, '_', bar}),

        ok = mnesia:delete_object({sample_bag, foo, baq}),
        [] = mnesia:match_object({sample_bag, '_', baq}),

        %% Delete from bag
        mnesia:delete({sample_bag, foo}),
        [] = mnesia:match_object({sample_bag, '_', baz}),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:match_object({sample, foo, bar}),
        [{sample_bag, foo, bar}] = mnesia:match_object({sample_bag, foo, bar}),
        [{sample_ordered_set, foo, bar}] = mnesia:match_object({sample_ordered_set, foo, bar})
    end).

index_match_object_cached_state(_Config) ->
    {aborted, foo} = mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:index_match_object({sample, foo, bar}, 3),
        [{sample_bag, foo, bar}] = mnesia:index_match_object({sample_bag, foo, bar}, 3),
        [{sample_ordered_set, foo, bar}] = mnesia:index_match_object({sample_ordered_set, foo, bar}, 3),
        %% Do some changes
        %% Overwrite existing
        ok = mnesia:write({sample, foo, baz}),
        ok = mnesia:write({sample_ordered_set, foo, baz}),
        %% Add new
        ok = mnesia:write({sample, bar, baz}),
        ok = mnesia:write({sample_ordered_set, bar, baz}),
        [] = mnesia:index_match_object({sample, foo, bar}, 3),
        [{sample, foo, baz}] = mnesia:index_match_object({sample, foo, baz}, 3),
        [{sample, bar, baz}] = mnesia:index_match_object({sample, bar, '_'}, 3),
        [{sample, bar, baz}, {sample, foo, baz}] = lists:usort(mnesia:index_match_object({sample, '_', baz}, 3)),

        [] = mnesia:index_match_object({sample_ordered_set, '_', bar}, 3),
        [{sample_ordered_set, bar, baz}, {sample_ordered_set, foo, baz}] =
            lists:usort(mnesia:index_match_object({sample_ordered_set, '_', baz}, 3)),

        %% Add new to bag
        ok = mnesia:write({sample_bag, foo, baq}),
        [{sample_bag, foo, baq}] = mnesia:index_match_object({sample_bag, '_', baq}, 3),

        %% Delete from set
        ok = mnesia:delete({sample, foo}),
        [{sample, bar, baz}] = mnesia:index_match_object({sample, '_', baz}, 3),
        ok = mnesia:delete({sample_ordered_set, bar}),
        [{sample_ordered_set, foo, baz}] = mnesia:index_match_object({sample_ordered_set, '_', baz}, 3),

        %% Delete object from set
        ok = mnesia:delete_object({sample, bar, bazz}),
        [{sample, bar, baz}] = mnesia:index_match_object({sample, '_', baz}, 3),
        ok = mnesia:delete_object({sample, bar, baz}),
        [] = mnesia:index_match_object({sample, '_', baz}, 3),

        ok = mnesia:write({sample, bar_written, baz}),
        ok = mnesia:delete_object({sample, bar_written, bazz}),
        [{sample, bar_written, baz}] = mnesia:index_match_object({sample, '_', baz}, 3),
        ok = mnesia:delete_object({sample, bar_written, baz}),
        [] = mnesia:index_match_object({sample, '_', baz}, 3),

        ok = mnesia:delete_object({sample_ordered_set, foo, bazz}),
        [{sample_ordered_set, foo, baz}] = mnesia:index_match_object({sample_ordered_set, '_', baz}, 3),
        ok = mnesia:delete_object({sample_ordered_set, foo, baz}),
        [] = mnesia:index_match_object({sample_ordered_set, '_', baz}, 3),

        %% Delete object from bag
        ok = mnesia:delete_object({sample_bag, foo, bar}),
        [] = mnesia:index_match_object({sample_bag, '_', bar}, 3),

        ok = mnesia:delete_object({sample_bag, foo, baq}),
        [] = mnesia:index_match_object({sample_bag, '_', baq}, 3),

        %% Delete from bag
        mnesia:delete({sample_bag, foo}),
        [] = mnesia:index_match_object({sample_bag, '_', baz}, 3),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        %% Initial data
        [{sample, foo, bar}] = mnesia:index_match_object({sample, foo, bar}, 3),
        [{sample_bag, foo, bar}] = mnesia:index_match_object({sample_bag, foo, bar}, 3),
        [{sample_ordered_set, foo, bar}] = mnesia:index_match_object({sample_ordered_set, foo, bar}, 3)
    end).

all_keys_cached_state(_Config) ->
    {aborted, foo} = mnevis:transaction(fun() ->
        [foo] = mnesia:all_keys(sample),
        [foo] = mnesia:all_keys(sample_bag),
        [foo] = mnesia:all_keys(sample_ordered_set),

        %% Write add keys
        ok = mnesia:write({sample, bar, baz}),
        ok = mnesia:write({sample, foo, baz}),

        [bar, foo] = lists:usort(mnesia:all_keys(sample)),

        ok = mnesia:write({sample_ordered_set, bar, baz}),
        ok = mnesia:write({sample_ordered_set, foo, baz}),

        [bar, foo] = lists:usort(mnesia:all_keys(sample_ordered_set)),

        %% Write to bag
        ok = mnesia:write({sample_bag, foo, baq}),
        ok = mnesia:write({sample_bag, bar, baq}),
        [bar, foo] = lists:usort(mnesia:all_keys(sample_bag)),

        %% Delete object from set
        ok = mnesia:delete_object({sample, bar, baz}),
        ok = mnesia:delete_object({sample_ordered_set, foo, baz}),

        [foo] = mnesia:all_keys(sample),
        [bar] = mnesia:all_keys(sample_ordered_set),

        %% Delete from set
        ok = mnesia:delete({sample, foo}),
        ok = mnesia:delete({sample_ordered_set, bar}),

        [] = mnesia:all_keys(sample),
        [] = mnesia:all_keys(sample_ordered_set),

        %% Delete object from bag
        ok = mnesia:delete_object({sample_bag, foo, bar}),
        ok = mnesia:delete_object({sample_bag, foo, baz}),

        %% Still some items in the bag
        [bar, foo] = lists:usort(mnesia:all_keys(sample_bag)),

        ok = mnesia:delete_object({sample_bag, bar, baq}),
        ok = mnesia:delete_object({sample_bag, foo, baq}),
        [foo] = mnesia:all_keys(sample_bag),

        %% Delete from bag
        ok = mnesia:delete({sample_bag, foo}),
        [] = mnesia:all_keys(sample_bag),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        [foo] = mnesia:all_keys(sample),
        [foo] = mnesia:all_keys(sample_bag),
        [foo] = mnesia:all_keys(sample_ordered_set)
    end).

first_cached_state(_Config) ->
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    {aborted, foo} = mnevis:transaction(fun() ->
        foo = mnesia:first(sample),
        foo = mnesia:first(sample_bag),
        foo = mnesia:first(sample_ordered_set),

        % ok = mnesia:write({sample, bar, baz}),
        % ok = mnesia:write({sample_bag, bar, baz}),
        % ok = mnesia:write({sample_ordered_set, bar, baz}),

        % %% First for other tables is not defined
        % bar = mnesia:first(sample_ordered_set),

        % ok = mnesia:delete({sample, foo}),
        % ok = mnesia:delete({sample_bag, foo}),

        % bar = mnesia:first(sample),
        % bar = mnesia:first(sample_bag),

        % ok = mnesia:write({sample_bag, foo, baz}),

        % ok = mnesia:delete_object({sample, bar, baz}),
        % ok = mnesia:delete_object({sample_bag, bar, baz}),
        % ok = mnesia:delete_object({sample_ordered_set, bar, baz}),

        % foo = mnesia:first(sample_bag),
        % '$end_of_table' = mnesia:first(sample),
        % foo = mnesia:first(sample_ordered_set),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        foo = mnesia:first(sample),
        foo = mnesia:first(sample_bag),
        foo = mnesia:first(sample_ordered_set)
    end).

last_cached_state(_Config) ->
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    {aborted, foo} = mnevis:transaction(fun() ->
        foo = mnesia:last(sample),
        foo = mnesia:last(sample_bag),
        foo = mnesia:last(sample_ordered_set),

        ok = mnesia:write({sample, zoo, baz}),
        ok = mnesia:write({sample_bag, zoo, baz}),
        ok = mnesia:write({sample_ordered_set, zoo, baz}),

        %% First for other tables is not defined
        zoo = mnesia:last(sample_ordered_set),

        ok = mnesia:delete({sample, foo}),
        ok = mnesia:delete({sample_bag, foo}),

        zoo = mnesia:last(sample),
        zoo = mnesia:last(sample_bag),

        ok = mnesia:write({sample_bag, foo, baz}),

        ok = mnesia:delete_object({sample, zoo, baz}),
        ok = mnesia:delete_object({sample_bag, zoo, baz}),
        ok = mnesia:delete_object({sample_ordered_set, zoo, baz}),

        foo = mnesia:last(sample_bag),
        '$end_of_table' = mnesia:last(sample),
        foo = mnesia:last(sample_ordered_set),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        foo = mnesia:last(sample),
        foo = mnesia:last(sample_bag),
        foo = mnesia:last(sample_ordered_set)
    end).

prev_cached_state(_Config) ->
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    {aborted, foo} = mnevis:transaction(fun() ->
        '$end_of_table' = mnesia:prev(sample, foo),
        '$end_of_table' = mnesia:prev(sample_bag, foo),
        '$end_of_table' = mnesia:prev(sample_ordered_set, foo),

        ok = mnesia:write({sample, bar, baz}),
        ok = mnesia:write({sample_bag, bar, baz}),
        ok = mnesia:write({sample_ordered_set, bar, baz}),

        bar = mnesia:prev(sample_ordered_set, foo),

        ok = mnesia:write({sample_ordered_set, baz, baz}),

        baz = mnesia:prev(sample_ordered_set, foo),

        ok = mnesia:write({sample_ordered_set, foz, baz}),
        ok = mnesia:write({sample_ordered_set, fox, baz}),

        fox = mnesia:prev(sample_ordered_set, foz),
        foo = mnesia:prev(sample_ordered_set, fox),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        '$end_of_table' = mnesia:prev(sample, foo),
        '$end_of_table' = mnesia:prev(sample_bag, foo),
        '$end_of_table' = mnesia:prev(sample_ordered_set, foo)
    end).
next_cached_state(_Config) ->
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    {aborted, foo} = mnevis:transaction(fun() ->
        '$end_of_table' = mnesia:next(sample, foo),
        '$end_of_table' = mnesia:next(sample_bag, foo),
        '$end_of_table' = mnesia:next(sample_ordered_set, foo),

        ok = mnesia:write({sample, goo, baz}),
        ok = mnesia:write({sample_bag, goo, baz}),
        ok = mnesia:write({sample_ordered_set, goo, baz}),

        goo = mnesia:next(sample_ordered_set, foo),

        ok = mnesia:write({sample_ordered_set, fzz, baz}),

        fzz = mnesia:next(sample_ordered_set, foo),

        % ok = mnesia:write({sample_ordered_set, faa, baz}),
        % ok = mnesia:write({sample_ordered_set, fbb, baz}),

        % fbb = mnesia:next(sample_ordered_set, faa),
        % foo = mnesia:next(sample_ordered_set, fbb),
        mnesia:abort(foo)
    end),
    mnevis:transaction(fun() ->
        '$end_of_table' = mnesia:next(sample, foo),
        '$end_of_table' = mnesia:next(sample_bag, foo),
        '$end_of_table' = mnesia:next(sample_ordered_set, foo)
    end).

foldl_cached_state(_Config) ->
    add_sample({sample, foo, bar}),
    add_sample({sample_bag, foo, bar}),
    add_sample({sample_bag, foo, baz}),
    add_sample({sample_bag, foo, baj}),
    add_sample({sample_ordered_set, foo, bar}),
    {aborted, foo} = mnevis:transaction(fun() ->
        KeysFold = fun(Tab) ->
            mnesia:foldl(fun(R, Acc) ->
                {_, K, _} = R,
                Acc ++ [K]
            end,
            [],
            Tab)
        end,

        KeysFoldR = fun(Tab) ->
            mnesia:foldr(fun(R, Acc) ->
                {_, K, _} = R,
                Acc ++ [K]
            end,
            [],
            Tab)
        end,

        ValsFold = fun(Tab) ->
            mnesia:foldl(fun({_, _K, V}, Acc) ->
                Acc ++ [V]
            end,
            [],
            Tab)
        end,
        [foo] = KeysFold(sample),
        [foo, foo, foo] = KeysFold(sample_bag),
        ok = mnesia:write({sample, bar, baz}),
        [bar, foo] = lists:usort(KeysFold(sample)),
        ok = mnesia:delete_object({sample, foo, bar}),
        [bar] = lists:usort(KeysFold(sample)),
        ok = mnesia:delete({sample, bar}),
        [] = lists:usort(KeysFold(sample)),


        ok = mnesia:write({sample_bag, foo, baq}),
        [baj, baq, bar, baz] = lists:usort(ValsFold(sample_bag)),

        ok = mnesia:delete_object({sample_bag, foo, bar}),
        [baj, baq, baz] = lists:usort(ValsFold(sample_bag)),

        ok = mnesia:delete({sample_bag, foo}),
        [] = lists:usort(ValsFold(sample_bag)),


        [foo] = KeysFold(sample_ordered_set),

        ok = mnesia:write({sample_ordered_set, bar, baz}),
        [bar, foo] = KeysFold(sample_ordered_set),
        [foo, bar] = KeysFoldR(sample_ordered_set),

        ok = mnesia:write({sample_ordered_set, baz, baz}),
        [bar, baz, foo] = KeysFold(sample_ordered_set),
        [foo, baz, bar] = KeysFoldR(sample_ordered_set),

        ok = mnesia:delete_object({sample_ordered_set, baz, bazz}),
        [bar, baz, foo] = KeysFold(sample_ordered_set),
        [foo, baz, bar] = KeysFoldR(sample_ordered_set),

        ok = mnesia:delete_object({sample_ordered_set, foo, bar}),
        [bar, baz] = KeysFold(sample_ordered_set),
        [baz, bar] = KeysFoldR(sample_ordered_set),

        ok = mnesia:delete({sample_ordered_set, bar}),
        [baz] = KeysFold(sample_ordered_set),
        [baz] = KeysFoldR(sample_ordered_set),
        mnesia:abort(foo)
    end).

write_delete_converge(_Config) ->
    %% Delete afte write is deleted.
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample, foo, bar})
    end),
    {atomic, []} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample, foo, baz}),
        ok = mnesia:delete({sample, foo}),
        [] = mnesia:read(sample, foo)
    end),
    [] = mnesia:dirty_read(sample, foo),

    %% Write after delete is written
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample, bar, bar})
    end),
    {atomic, [{sample, bar, baz}]} = mnevis:transaction(fun() ->
        ok = mnesia:delete({sample, bar}),
        ok = mnesia:write({sample, bar, baz}),
        [{sample, bar, baz}] = mnesia:read(sample, bar)
    end),
    [{sample, bar, baz}] = mnesia:dirty_read(sample, bar).

write_delete_object_converge(_Config) ->
    %% Delete pbject after update deletes
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample, foo, bar})
    end),
    [{sample, foo, bar}] = mnesia:dirty_read(sample, foo),
    {atomic, []} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample, foo, baz}),
        ok = mnesia:delete_object({sample, foo, baz}),
        [] = mnesia:read(sample, foo)
    end),
    [] = mnesia:dirty_read(sample, foo),

    %% Delete object after update to different does not delete
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample, bar, bar})
    end),
    [{sample, bar, bar}] = mnesia:dirty_read(sample, bar),
    {atomic, [{sample, bar, baz}]} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample, bar, baz}),
        ok = mnesia:delete_object({sample, bar, bar}),
        [{sample, bar, baz}] = mnesia:read(sample, bar)
    end),
    [{sample, bar, baz}] = mnesia:dirty_read(sample, bar),

    %% Different object is not deleted
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample, baz, bar})
    end),
    [{sample, baz, bar}] = mnesia:dirty_read(sample, baz),
    {atomic, [{sample, baz, baz}]} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample, baz, baz}),
        ok = mnesia:delete_object({sample, baz, baq}),
        [{sample, baz, baz}] = mnesia:read(sample, baz)
    end),
    [{sample, baz, baz}] = mnesia:dirty_read(sample, baz).

write_bag_delete_converge(_Config) ->
    %% Bag write is deleted on delete
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, foo, bar})
    end),
    {atomic, []} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, foo, baz}),
        [{sample_bag, foo, bar}, {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),
        ok = mnesia:delete({sample_bag, foo}),
        [] = mnesia:read(sample_bag, foo)
    end),
    [] = mnesia:dirty_read(sample_bag, foo),

    %% Delete from bag deletes old items
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, bar, bar})
    end),
    {atomic, [{sample_bag, bar, baz}]} = mnevis:transaction(fun() ->
        ok = mnesia:delete({sample_bag, bar}),
        [] = mnesia:read(sample_bag, bar),
        ok = mnesia:write({sample_bag, bar, baz}),
        [{sample_bag, bar, baz}] = mnesia:read(sample_bag, bar)
    end),
    [{sample_bag, bar, baz}] = mnesia:dirty_read(sample_bag, bar).

write_bag_delete_object_converge(_Config) ->
    %% Delete pbject deletes written
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, foo, bar})
    end),
    [{sample_bag, foo, bar}] = mnesia:dirty_read(sample_bag, foo),
    {atomic, [{sample_bag, foo, bar}]} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, foo, baz}),
        [{sample_bag, foo, bar}, {sample_bag, foo, baz}] = mnesia:read(sample_bag, foo),
        ok = mnesia:delete_object({sample_bag, foo, baz}),
        [{sample_bag, foo, bar}] = mnesia:read(sample_bag, foo)
    end),
    [{sample_bag, foo, bar}] = mnesia:dirty_read(sample_bag, foo),

    %% Delete object deletes old
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, bar, bar})
    end),
    [{sample_bag, bar, bar}] = mnesia:dirty_read(sample_bag, bar),
    {atomic, [{sample_bag, bar, baz}]} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, bar, baz}),
        [{sample_bag, bar, bar}, {sample_bag, bar, baz}] = mnesia:read(sample_bag, bar),
        ok = mnesia:delete_object({sample_bag, bar, bar}),
        [{sample_bag, bar, baz}] = mnesia:read(sample_bag, bar)
    end),
    [{sample_bag, bar, baz}] = mnesia:dirty_read(sample_bag, bar),

    %% Different object is not deleted
    mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, baz, bar})
    end),
    [{sample_bag, baz, bar}] = mnesia:dirty_read(sample_bag, baz),
    {atomic, [{sample_bag, baz, bar}, {sample_bag, baz, baz}]} = mnevis:transaction(fun() ->
        ok = mnesia:write({sample_bag, baz, baz}),
        [{sample_bag, baz, bar}, {sample_bag, baz, baz}] = mnesia:read(sample_bag, baz),
        ok = mnesia:delete_object({sample_bag, baz, baq}),
        [{sample_bag, baz, bar}, {sample_bag, baz, baz}] = mnesia:read(sample_bag, baz)
    end),
    [{sample_bag, baz, bar}, {sample_bag, baz, baz}] = mnesia:dirty_read(sample_bag, baz).


