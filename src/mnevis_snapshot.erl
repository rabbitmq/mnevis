-module(mnevis_snapshot).

-behaviour(ra_snapshot).

-include_lib("ra/include/ra.hrl").

-export([prepare/2,
         write/3,
         begin_read/1,
         read_chunk/3,
         begin_accept/2,
         accept_chunk/2,
         complete_accept/2,
         recover/1,
         validate/1,
         read_meta/1]).

-record(saved_state, {state, node :: node()}).

-spec prepare(Index :: ra_index(), State :: term()) -> Ref :: {ra_index(), #saved_state{}}.
prepare(Index, State) ->
    CheckpointName = Index,
    {Time, {ok, CheckpointName, _}} =
        timer:tc(mnesia, activate_checkpoint,
                  [[{name, CheckpointName},
                    {max, mnesia:system_info(tables)},
                    {ram_overrides_dump, true}]]),
    error_logger:info_msg("Activate checkpoint ~p : ~p us for tables ~p ~n", [Index, Time, mnesia:system_info(tables)]),
    error_logger:info_msg("Checkpoints ~p ~n", [timer:tc(mnesia, system_info, [checkpoints])]),
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
    ok = filelib:ensure_dir(filename:join(Dir, "d")),
    case write_internal(Dir, Meta, CheckpointName, SavedState) of
        ok -> ok;
        {error, _} = Err ->
            cleanup_dir(Dir),
            Err
    end.

write_internal(Dir, Meta, CheckpointName, SavedState) ->
    case ra_log_snapshot:write(Dir, Meta, <<>>) of
        ok ->
            StateFile = state_file(Dir),
            SavedStateBinary = term_to_binary(SavedState),
            SavedStateCrc = erlang:crc32(SavedStateBinary),
            case file:write_file(StateFile, <<SavedStateCrc:32/integer, SavedStateBinary/binary>>) of
                ok               -> write_mnesia_backup(Dir, CheckpointName);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            error_logger:error_msg("Can't write ra_log_snapshot ~p : ~p~n", [Dir, Err]),
            Err
    end.

write_mnesia_backup(Dir, CheckpointName) ->
    MnesiaFile = mnesia_file(Dir),
    %% TODO: checkpoint may be deleted when deleting a table.
    case lists:member(CheckpointName, mnesia:system_info(checkpoints)) of
        true ->
            case mnesia:backup_checkpoint(CheckpointName, MnesiaFile) of
                ok               ->
                    error_logger:info_msg("Deactivate checkpoint ~p~n", [CheckpointName]),
                    mnesia:deactivate_checkpoint(CheckpointName);
                %% TODO cleanup
                {error, _} = Err -> Err
            end;
        false ->
            %% TODO: Checkpoint was deleted. Do nothing?
            ok
    end.

cleanup_dir(Dir) ->
    ra_lib:recursive_delete(Dir).

%% Read the snapshot metadata and initialise a read state used in read_chunk/3
%% The read state should contain all the information required to read a chunk
-spec begin_read(Location :: file:filename()) ->
    {ok, Meta :: ra_snapshot:meta(), ReadState :: term()}
    | {error, term()}.
begin_read(Dir) ->
    case read_meta(Dir) of
        {ok, Meta} ->
            {ok, Meta, saved_state_start};
        {error, _} = Error ->
            Error
    end.

%% Read a chunk of data from the snapshot using the read state
%% Returns a binary chunk of data and a continuation state
-spec read_chunk(ReadState,
                 Size :: non_neg_integer(),
                 Location :: file:filename()) ->
    {ok, term(), {next, ReadState} | last} | {error, term()}.
read_chunk(saved_state_start, Size, Dir) ->
    SavedStateFile = state_file(Dir),
    case file:open(SavedStateFile, [read, binary, raw]) of
        {ok, Fd} ->
            {ok, Eof} = file:position(Fd, eof),
            read_chunk({saved_state_chunk, {0, Eof, Fd}}, Size, Dir);
        {error, _} = Err ->
            Err
    end;
read_chunk({saved_state_chunk, FileReadState0}, Size, _Dir) ->
    case read_file_chunk(FileReadState0, Size) of
        {ok, Data, {next, FileReadState1}} ->
            {ok, {saved_state_chunk, Data},
                 {next, {saved_state_chunk, FileReadState1}}};
        {ok, Data, last} ->
            {ok, {saved_state_chunk, Data},
                 {next, mnesia_backup_start}}
    end;
read_chunk(mnesia_backup_start, Size, Dir) ->
    Filename = mnesia_file(Dir),
    case file:open(Filename, [read, binary, raw]) of
        {ok, Fd} ->
            {ok, Eof} = file:position(Fd, eof),
            read_chunk({mnesia_backup_chunk, {0, Eof, Fd}}, Size, Dir);
        {error, _} = Err ->
            Err
    end;
read_chunk({mnesia_backup_chunk, FileReadState0}, Size, _Dir) ->
    case read_file_chunk(FileReadState0, Size) of
        {ok, Data, {next, FileReadState1}} ->
            {ok, {mnesia_backup_chunk, Data},
                 {next, {mnesia_backup_chunk, FileReadState1}}};
        {ok, Data, last} ->
            {ok, {mnesia_backup_chunk, Data}, last}
    end.

read_file_chunk({Pos, Eof, Fd}, Size) ->
    {ok, _} = file:position(Fd, Pos),
    {ok, Data} = file:read(Fd, Size),
    case Pos + Size >= Eof of
        true ->
            _ = file:close(Fd),
            {ok, Data, last};
        false ->
            {ok, Data, {next, {Pos + Size, Eof, Fd}}}
    end.

%% begin a stateful snapshot acceptance process
-spec begin_accept(Dir :: file:filename(), Meta :: ra_snapshot:meta()) ->
    {ok, AcceptState :: term()} | {error, term()}.
begin_accept(Dir, Meta) ->
    %% TODO: crc all the things?
    ok = filelib:ensure_dir(filename:join(Dir, "d")),
    case ra_log_snapshot:write(Dir, Meta, <<>>) of
        ok ->
            SavedStateFile = state_file(Dir),
            {ok, Fd} = file:open(SavedStateFile, [write, binary, raw]),
            {ok, {saved_state, Fd, Dir}};
        {error, _} = Err ->
            Err
    end.

%% TODO: return error on invalid function clause

%% accept a chunk of data
-spec accept_chunk(Chunk :: term(), AcceptState :: term()) ->
    {ok, AcceptState :: term()} | {error, term()}.
accept_chunk({saved_state_chunk, Chunk}, {saved_state, Fd, Dir}) ->
    ok = file:write(Fd, Chunk),
    {ok, {saved_state, Fd, Dir}};
accept_chunk({mnesia_backup_chunk, Chunk}, {saved_state, StateFd, Dir}) ->
    ok = file:sync(StateFd),
    ok = file:close(StateFd),
    MnesiaFile = mnesia_file(Dir),
    {ok, Fd} = file:open(MnesiaFile, [write, binary, raw]),
    accept_chunk({mnesia_backup_chunk, Chunk}, {mnesia_backup, Fd, Dir});
accept_chunk({mnesia_backup_chunk, Chunk}, {mnesia_backup, Fd, Dir}) ->
    ok = file:write(Fd, Chunk),
    {ok, {mnesia_backup, Fd, Dir}}.

%% accept the last chunk of data
-spec complete_accept(Chunk :: term(),
                          AcceptState :: term()) ->
    ok | {error, term()}.
complete_accept({mnesia_backup_chunk, Chunk}, {saved_state, StateFd, Dir}) ->
    ok = file:sync(StateFd),
    ok = file:close(StateFd),
    MnesiaFile = mnesia_file(Dir),
    {ok, Fd} = file:open(MnesiaFile, [write, binary, raw]),
    complete_accept({mnesia_backup_chunk, Chunk}, {mnesia_backup, Fd, Dir});
complete_accept({mnesia_backup_chunk, Chunk}, {mnesia_backup, Fd, _Dir}) ->
    ok = file:write(Fd, Chunk),
    ok = file:sync(Fd),
    ok = file:close(Fd).

%% Side-effect function
%% Recover machine state from file
-spec recover(Location :: file:filename()) ->
    {ok, Meta :: ra_snapshot:meta(), State :: term()} | {error, term()}.
recover(Dir) ->
    case ra_log_snapshot:recover(Dir) of
        {ok, Meta, <<>>} ->
            StateFile = state_file(Dir),
            case file:read_file(StateFile) of
                {ok, <<Crc:32/integer, SavedStateBin/binary>>} ->
                    Crc = erlang:crc32(SavedStateBin),
                    #saved_state{state = State,
                                 node = SourceNode} = binary_to_term(SavedStateBin),
                    case recover_mnesia(Dir, SourceNode) of
                        ok -> {ok, Meta, State};
                        {error, _} = MnesiaErr -> MnesiaErr
                    end;
                {error, _} = SavedStateErr ->
                    SavedStateErr
            end;
        {error, _} = MetaErr ->
            MetaErr
    end.

recover_mnesia(Dir, SourceNode) ->
    MnesiaFile = mnesia_file(Dir),
    ConvertedMnesiaFile = converted_mnesia_file(Dir),
    case filelib:is_file(ConvertedMnesiaFile) of
        true ->
            restore_mnesia_backup(ConvertedMnesiaFile);
        false ->
            case rename_backup_node(SourceNode, node(),
                                    MnesiaFile, ConvertedMnesiaFile) of
                {ok, _} -> restore_mnesia_backup(ConvertedMnesiaFile);
                {error, _} = Err -> Err
            end
    end.

%% validate the integrity of the snapshot
-spec validate(Location :: file:filename()) ->
    ok | {error, term()}.
validate(Dir) ->
    case ra_log_snapshot:recover(Dir) of
        {ok, _Meta, <<>>} -> ok;
        {error, _} = Err -> Err
    end.

%% Only read index and term from snapshot
-spec read_meta(Location :: file:filename()) ->
    {ok, ra_snapshot:meta()} |
    {error, invalid_format | {invalid_version, integer()} | checksum_error | ra_snapshot:file_err()}.
read_meta(Dir) ->
    ra_log_snapshot:read_meta(Dir).

mnesia_file(Dir) ->
    filename:join(Dir, "mnesia.backup").

converted_mnesia_file(Dir) ->
    filename:join(Dir, "mnesia.backup.local").

state_file(Dir) ->
    filename:join(Dir, "saved_state").

restore_mnesia_backup(BackupFile) ->
    case mnesia:restore(BackupFile, [{default_op, recreate_tables}]) of
        {atomic, _}       -> ok;
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

