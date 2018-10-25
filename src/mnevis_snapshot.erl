-module(mnevis_snapshot).

-behaviour(ra_snapshot).

-include_lib("ra/include/ra.hrl").

-export([release/2, prepare/2, write/3, save/3, read/1, recover/1, install/2, read_indexterm/1]).

-spec release(Index :: ra_index(), State) -> State.
release(_Index, State) -> State.

-spec prepare(Index :: ra_index(), State) -> Ref :: {ra_index(), State}.
prepare(Index, State) ->
    CheckpointName = Index,
    {Time, {ok, CheckpointName, _}} =
        timer:tc(mnesia, activate_checkpoint,
                  [[{name, CheckpointName},
                    {min, mnesia:system_info(tables)}]]),
    io:format("Activate checkpoint ~p : ~p us ~n", [Index, Time]),
    io:format("Checkpoints ~p ~n", [timer:tc(mnesia, system_info, [checkpoints])]),
    {CheckpointName, State}.

%% Saves snapshot from external state to disk.
%% Runs in a separate process.
%% External storage should be available to read
-spec write(Location :: file:filename(),
                Meta :: ra_snapshot:meta(),
                Ref :: term()) ->
    ok | {error, ra_snapshot:file_err()}.
write(Dir, Meta, {CheckpointName, State}) ->
    %% TODO operation order
    case ra_log_snapshot:write(Dir, Meta, State) of
        ok ->
            MnesiaFile = mnesia_file(Dir),
            io:format("Backup checkpoint ~p~n", [element(1, Meta)]),
            case mnesia:backup_checkpoint(CheckpointName, MnesiaFile) of
                ok               ->
                    io:format("Deactivate checkpoint ~p~n", [CheckpointName]),
                    mnesia:deactivate_checkpoint(CheckpointName);
                %% TODO cleanup
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end.



%% Read the snapshot from disk into serialised structure for transfer.
-spec read(Location :: file:filename()) ->
    {ok, Meta :: ra_snapshot:meta(), Data :: term()} |
    {error, invalid_format | {invalid_version, integer()} | checksum_error | ra_snapshot:file_err()}.
read(Dir) ->
    case ra_log_snapshot:read(Dir) of
        {ok, Meta, State} ->
            case file:read_file(mnesia_file(Dir)) of
                {ok, Backup}     -> {ok, Meta, {State, Backup}};
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end.

%% Dump the snapshot data to disk withtou touching the external state
-spec save(Location :: file:filename(), Meta :: ra_snapshot:meta(), Data :: term()) ->
    ok | {error, ra_snapshot:file_err()}.
save(Dir, Meta, {State, Backup}) ->
    case ra_log_snapshot:save(Dir, Meta, State) of
        ok -> file:write_file(mnesia_file(Dir), Backup);
        {error, _} = Err -> Err
    end.

%% Side-effect function
%% Deserialize the snapshot to external state.
-spec install(Data :: term(), Location :: file:filename()) ->
    {ok, State :: term()} | {error, term()}.
install({State, _Backup}, Dir) ->
    case mnesia:restore(mnesia_file(Dir), []) of
        {atomic, _}       -> {ok, State};
        {aborted, Reason} -> {error, {aborted, Reason}}
    end.

%% Side-effect function
%% Recover machine state from file
-spec recover(Location :: file:filename()) ->
    {ok, Meta :: ra_snapshot:meta(), State :: term()} | {error, term()}.
recover(Dir) ->
    case ra_log_snapshot:read(Dir) of
        {ok, Meta, State} ->
            case mnesia:restore(mnesia_file(Dir), []) of
                {atomic, _}       -> {ok, Meta, State};
                {aborted, Reason} -> {error, {aborted, Reason}}
            end;
        {error, _} = Err -> Err
    end.

%% Only read index and term from snapshot
-spec read_indexterm(Location :: file:filename()) ->
    {ok, ra_idxterm()} |
    {error, invalid_format | {invalid_version, integer()} | checksum_error | ra_snapshot:file_err()}.
read_indexterm(Dir) ->
    ra_log_snapshot:read_indexterm(Dir).

mnesia_file(Dir) ->
    filename:join(Dir, "mnesia.backup").