-module(mnevis_snapshot).

-behaviour(ra_snapshot).

-include_lib("ra/include/ra.hrl").

-export([release/2, prepare/2, write/3, save/3, read/1, recover/1, install/2, read_indexterm/1]).

-record(saved_state, {state, node :: node()}).

-record(transferred_state, {saved_state :: #saved_state{}, data :: binary()}).


-spec release(Index :: ra_index(), State) -> State.
release(_Index, State) -> State.

-spec prepare(Index :: ra_index(), State :: term()) -> Ref :: {ra_index(), #saved_state{}}.
prepare(Index, State) ->
    CheckpointName = Index,
    {Time, {ok, CheckpointName, _}} =
        timer:tc(mnesia, activate_checkpoint,
                  [[{name, CheckpointName},
                    {max, mnesia:system_info(tables)},
                    {ram_overrides_dump, true}]]),
    io:format("Activate checkpoint ~p : ~p us for tables ~p ~n", [Index, Time, mnesia:system_info(tables)]),
    io:format("Checkpoints ~p ~n", [timer:tc(mnesia, system_info, [checkpoints])]),
    {CheckpointName, #saved_state{state = State, node = node()}}.

%% Saves snapshot from external state to disk.
%% Runs in a separate process.
%% External storage should be available to read
-spec write(Location :: file:filename(),
                Meta :: ra_snapshot:meta(),
                Ref :: {ra_index(), #saved_state{}}) ->
    ok | {error, ra_snapshot:file_err()}.
write(Dir, Meta, {CheckpointName, SavedState = #saved_state{}}) ->
    %% TODO operation order
    ok = filelib:ensure_dir(ra_log_snapshot_file(Dir)),
    case ra_log_snapshot:write(ra_log_snapshot_file(Dir), Meta, SavedState) of
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
        {error, _} = Err ->
            io:format("Can't write ra_log_snapshot ~p : ~p~n", [ra_log_snapshot_file(Dir), Err]),
            Err
    end.



%% Read the snapshot from disk into serialised structure for transfer.
-spec read(Location :: file:filename()) ->
    {ok, Meta :: ra_snapshot:meta(), Data :: #transferred_state{}} |
    {error, invalid_format | {invalid_version, integer()} | checksum_error | ra_snapshot:file_err()}.
read(Dir) ->
    case ra_log_snapshot:read(ra_log_snapshot_file(Dir)) of
        {ok, Meta, SavedState = #saved_state{}} ->
            case file:read_file(mnesia_file(Dir)) of
                {ok, Backup}     ->
                    {ok, Meta, #transferred_state{saved_state = SavedState,
                                                  data = Backup}};
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end.

%% Dump the snapshot data to disk withtou touching the external state
-spec save(Location :: file:filename(),
           Meta :: ra_snapshot:meta(),
           Data :: #transferred_state{}) ->
    ok | {error, ra_snapshot:file_err()}.
save(Dir, Meta, #transferred_state{saved_state = SavedState, data = Backup}) ->
    ok = filelib:ensure_dir(ra_log_snapshot_file(Dir)),
    case ra_log_snapshot:save(ra_log_snapshot_file(Dir), Meta, SavedState) of
        ok -> file:write_file(mnesia_file(Dir), Backup);
        {error, _} = Err -> Err
    end.

%% Side-effect function
%% Deserialize the snapshot to external state.
-spec install(Data :: term(), Location :: file:filename()) ->
    {ok, State :: term()} | {error, term()}.
install(#transferred_state{saved_state = #saved_state{state = State,
                                                      node  = SourceNode}},
        Dir) ->
    ConvertedBackupFile = converted_mnesia_file(Dir),
    case filelib:is_file(ConvertedBackupFile) of
        true ->
            restore_mnesia_backup(ConvertedBackupFile, State);
        false ->
            case rename_backup_node(SourceNode, node(),
                                    mnesia_file(Dir), ConvertedBackupFile) of
                {ok, _} -> restore_mnesia_backup(ConvertedBackupFile, State);
                {error, _} = Err -> Err
            end
    end.

%% Side-effect function
%% Recover machine state from file
-spec recover(Location :: file:filename()) ->
    {ok, Meta :: ra_snapshot:meta(), State :: term()} | {error, term()}.
recover(Dir) ->
    case ra_log_snapshot:read(ra_log_snapshot_file(Dir)) of
        {ok, Meta, SavedState} ->
            case install(#transferred_state{saved_state = SavedState}, Dir) of
                {ok, State} -> {ok, Meta, State};
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end.

%% Only read index and term from snapshot
-spec read_indexterm(Location :: file:filename()) ->
    {ok, ra_idxterm()} |
    {error, invalid_format | {invalid_version, integer()} | checksum_error | ra_snapshot:file_err()}.
read_indexterm(Dir) ->
    ra_log_snapshot:read_indexterm(ra_log_snapshot_file(Dir)).

ra_log_snapshot_file(Dir) ->
    filename:join(Dir, "state_snapshot").

mnesia_file(Dir) ->
    filename:join(Dir, "mnesia.backup").

converted_mnesia_file(Dir) ->
    filename:join(Dir, "mnesia.backup.local").

restore_mnesia_backup(BackupFile, State) ->
    case mnesia:restore(BackupFile, [{default_op, recreate_tables}]) of
        {atomic, _}       -> {ok, State};
        {aborted, Reason} -> {error, {aborted, Reason}}
    end.

rename_backup_node(From, To, Source, Target) ->
    Switch =
        fun(Node) when Node == From -> To;
           (Node) when Node == To -> throw({error, already_exists});
           (Node) -> Node
        end,
    mnesia:traverse_backup(Source, Target, fun convert_traverse_node/2, Switch).

convert_traverse_node({schema, db_nodes, Nodes}, Switch) ->
    {[{schema, db_nodes, lists:map(Switch,Nodes)}], Switch};
convert_traverse_node({schema, version, Version}, Switch) ->
    {[{schema, version, Version}], Switch};
convert_traverse_node({schema, cookie, Cookie}, Switch) ->
    {[{schema, cookie, Cookie}], Switch};
convert_traverse_node({schema, Tab, CreateList}, Switch) ->
    Keys = [ram_copies, disc_copies, disc_only_copies],
    OptSwitch =
        fun({Key, Val}) ->
                case lists:member(Key, Keys) of
                    true -> {Key, lists:map(Switch, Val)};
                    false-> {Key, Val}
                end
        end,
    {[{schema, Tab, lists:map(OptSwitch, CreateList)}], Switch};
convert_traverse_node(Other, Switch) ->
    {[Other], Switch}.

