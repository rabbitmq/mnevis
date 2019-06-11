-module(mnevis_read).

-export([create_versions_table/0]).
-export([get_version/1, update_version/1, init_version/2]).
-export([local_read_query/1, local_read_query/2]).
-export([wait_for_versions/1]).

create_versions_table() ->
    CreateResult = mnesia:create_table(versions, []),
    case CreateResult of
        {atomic, ok} -> ok;
        {aborted,{already_exists,versions}} -> ok;
        Other -> error({cannot_create_versions_table, Other})
    end.

-spec get_version({mnevis:table(), term()} | mnevis:table()) ->
    {ok, mnevis_context:version()} | {error, no_exists}.
get_version(Tab) ->
    %% TODO: handle error
    MaybeVersion = ets:lookup(versions, Tab),
    case MaybeVersion of
        [{versions, Tab, Version}] -> {ok, Version};
        [] -> {error, no_exists}
    end.

-spec compare_versions([mnevis_context:read_version()]) ->
    ok | {version_mismatch, [mnevis_context:read_version()]}.
compare_versions(ReadVersions) ->
    Mismatched = lists:filtermap(
        fun({Tab, Version}) ->
            case get_version(Tab) of
                {ok, Version} ->
                    false;
                {ok, OtherVersion} ->
                    {true, {Tab, OtherVersion}};
                {error, no_exists} ->
                    {true, {Tab, -1}}
            end
        end,
        ReadVersions),
    case Mismatched of
        [] -> ok;
        _  -> {version_mismatch, Mismatched}
    end.


-spec local_read_query(mnevis_context:read_spec()) ->
    {ok, {[mnevis_context:record()], mnevis_context:version()}} |
    {error, {no_exists, term()} | {no_version_for_table, mnevis:table()}}.
local_read_query(ReadSpec) ->
    local_read_query(ReadSpec, []).

-spec local_read_query(mnevis_context:read_spec(), Versions :: [mnevis_context:read_version()]) ->
    {ok, {[mnevis_context:record()], mnevis_context:version()}} |
    {error, {no_exists, term()} |
            {no_version_for_table, mnevis:table()} |
            {version_mismatch, [mnevis_context:read_version()]}}.
local_read_query({Op, [Tab | _] = Args}, CurrentVersions) ->
    case get_version(Tab) of
        {ok, Version} ->
            case proplists:get_value(Tab, CurrentVersions, Version) of
                Version ->
                    Result = erlang:apply(mnesia, Op, Args),
                    {ok, {Result, Version}};
                OtherVersion ->
                    {error, {version_mismatch, [{Tab, OtherVersion}]}}
            end;
        {error, no_exists} ->
            case lists:member(Tab, mnesia:system_info(tables)) of
                true ->
                    {error, {no_version_for_table, Tab}};
                false ->
                    {error, {no_exists, Args}}
            end
    end.

-spec update_version(mnevis:table()) -> {mnevis:table(), mnevis_context:version()}.
update_version(Tab) ->
    case get_version(Tab) of
        {ok, Version} ->
            NewVersion = Version + 1,
            VersionRecord = {versions, Tab, NewVersion},
            ok = mnesia:write(versions, VersionRecord, write),
            {Tab, NewVersion};
        {error, no_exists} ->
            error({table_version_missing, Tab})
    end.

-spec init_version(mnevis:table(), integer()) -> {mnevis:table(), mnevis_context:version()}.
init_version(Tab, FirstVersion) ->
    NewVersion = case get_version(Tab) of
        {ok, Version} ->
            Version + 1;
        {error, no_exists} ->
            FirstVersion
    end,
    VersionRecord = {versions, Tab, NewVersion},
    case mnesia:is_transaction() of
        true ->
            ok = mnesia:write(VersionRecord);
        false ->
            ok = mnesia:dirty_write(VersionRecord)
    end,
    {Tab, NewVersion}.

-spec wait_for_versions([mnevis_context:read_version()]) -> ok.
wait_for_versions(TargetVersions) ->
    VersionsToWait = filter_versions_to_wait(TargetVersions),
    case VersionsToWait of
        [] ->
            ok;
        _ ->
            %% NOTE: create a one-off process to wait for mnesia events.
            %% If we subscribe in the main process - events may be delivered
            %% after we unsubscribe and mess up gen servers.
            {WaitingPid, MonRef} = spawn_monitor(fun() ->
                {ok, _} = mnesia:subscribe({table, versions, simple}),
                try
                    wait_for_mnesia_updates(VersionsToWait)
                after
                    _ = mnesia:unsubscribe({table, versions, simple}),
                    flush_table_events()
                end
            end),
            receive {'DOWN', MonRef, process, WaitingPid, Reason} ->
                case Reason of
                    normal -> ok;
                    Error  -> error({mnevis_error_waitnig_for_versions,
                                     Error, TargetVersions})
                end
            end
    end.

-spec wait_for_mnesia_updates([mnevis_context:read_version()]) -> ok.
wait_for_mnesia_updates([]) ->
    ok;
wait_for_mnesia_updates(WaitForVersions) ->
    %% TODO: should we wait forever for a follower to catch up with the cluster?
    receive {mnesia_table_event, {write, {versions, Tab, Version}, _}} ->
        case proplists:get_value(Tab, WaitForVersions) of
            undefined ->
                wait_for_mnesia_updates(WaitForVersions);
            WaitingFor ->
                case WaitingFor =< Version of
                    true ->
                        wait_for_mnesia_updates(lists:keydelete(Tab, 1, WaitForVersions));
                    false ->
                        wait_for_mnesia_updates(WaitForVersions)
                end
        end
    %% TODO: better timeout value?
    after 100 ->
        wait_for_mnesia_updates(filter_versions_to_wait(WaitForVersions))
    end.

filter_versions_to_wait(TargetVersions) ->
    case compare_versions(TargetVersions) of
        ok -> [];
        {version_mismatch, CurrentVersions} ->
            lists:filtermap(
                fun({Tab, CurrentVersion}) ->
                    %% Assertion: The version should be in the matched versions
                    {Tab, TargetVersion} = lists:keyfind(Tab, 1, TargetVersions),
                    case CurrentVersion < TargetVersion of
                        true  -> {true, {Tab, TargetVersion}};
                        false -> false
                    end
                end,
                CurrentVersions)
    end.

flush_table_events() ->
    receive {mnesia_table_event, _} -> flush_table_events()
    after 0 -> ok
    end.
