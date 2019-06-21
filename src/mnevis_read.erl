-module(mnevis_read).

-export([create_versions_table/0]).
-export([all_table_versions/0]).
-export([compare_versions/1]).
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

all_table_versions() ->
    Tables = mnesia:system_info(tables),
    lists:filtermap(fun(Tab) ->
        case get_version(Tab) of
            {ok, Version}      -> {true, {Tab, Version}};
            {error, no_exists} -> false
        end
    end,
    Tables).

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
            wait_for_mnesia_updates(VersionsToWait)
    end.

-spec wait_for_mnesia_updates([mnevis_context:read_version()]) -> ok.
wait_for_mnesia_updates([]) ->
    ok;
wait_for_mnesia_updates(WaitForVersions) ->
    %% TODO: should we wait forever for a follower to catch up with the cluster?
    %% TODO: better timeout value?
    timer:sleep(100),
    wait_for_mnesia_updates(filter_versions_to_wait(WaitForVersions)).

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
