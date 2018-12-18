-module(mnevis_snapshot).

-behaviour(ra_snapshot).

-include_lib("ra/include/ra.hrl").

-export([prepare/2,
         write/3,
         begin_read/2,
         read_chunk/3,
         begin_accept/3,
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
-spec begin_read(Size :: non_neg_integer(),
                     Location :: file:filename()) ->
    {ok, Crc :: non_neg_integer(), Meta :: ra_snapshot:meta(), ReadState :: term()}
    | {error, term()}.
begin_read(Size, Dir) ->
    case read_meta(Dir) of
        {ok, Meta} ->
            case file:read_file(state_file(Dir)) of
                {ok, <<Crc:32/integer, SavedStateBin/binary>>} ->
                    case byte_size(SavedStateBin) =< Size of
                        true ->
                            {ok, Crc, Meta, {saved_state_full, SavedStateBin}};
                        false ->
                            error_logger:warning_msg("Saved state ~p too big to fit the chunk ~p~n",
                                                     [byte_size(SavedStateBin), Size]),

                            SavedStateChunks = split_to_chunks(SavedStateBin, Size),
                            {ok, Crc, Meta, {saved_state_partial, SavedStateChunks}}
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Error ->
            Error
    end.

split_to_chunks(Binary, Size) ->
    case Binary of
        <<Chunk:Size/binary, Rest/binary>> ->
            [Chunk | split_to_chunks(Rest, Size)];
        <<>> -> [];
        Rest -> [Rest]
    end.

%% Read a chunk of data from the snapshot using the read state
%% Returns a binary chunk of data and a continuation state
-spec read_chunk(ReadState,
                 Size :: non_neg_integer(),
                 Location :: file:filename()) ->
    {ok, term(), {next, ReadState} | last} | {error, term()}.
read_chunk({saved_state_full, SavedStateBin}, _Size, _Dir) ->
    {ok, {saved_state_full, SavedStateBin}, {next, mnesia_backup_start}};
read_chunk({saved_state_partial, [Chunk | Chunks]}, _Size, _Dir) ->
    Next = case Chunks of
        [] -> mnesia_backup_start;
        _  -> {saved_state_partial, Chunks}
    end,
    {ok, {saved_state_partial, Chunk},
         {next, Next}};
read_chunk(mnesia_backup_start, Size, Dir) ->
    Filename = mnesia_file(Dir),
    case file:open(Filename, [read, binary, raw]) of
        {ok, Fd} ->
            {ok, DataStart} = file:position(Fd, cur),
            {ok, Eof} = file:position(Fd, eof),
            read_file_chunk(DataStart, Size, Eof, Fd);
        {error, _} = Err ->
            Err
    end;
read_chunk({mnesia_backup_chunk, {Pos, Eof, Fd}}, Size, _Dir) ->
    read_file_chunk(Pos, Size, Eof, Fd).

read_file_chunk(Pos, Size, Eof, Fd) ->
    {ok, _} = file:position(Fd, Pos),
    {ok, Data} = file:read(Fd, Size),
    case Pos + Size >= Eof of
        true ->
            _ = file:close(Fd),
            {ok, {mnesia_backup_chunk, Data}, last};
        false ->
            {ok, {mnesia_backup_chunk, Data}, {next, {mnesia_backup_chunk, {Pos + Size, Eof, Fd}}}}
    end.

%% begin a stateful snapshot acceptance process
-spec begin_accept(Dir :: file:filename(), Crc :: non_neg_integer(), Meta :: ra_snapshot:meta()) ->
    {ok, AcceptState :: term()} | {error, term()}.
begin_accept(Dir, Crc, Meta) ->
    %% TODO: crc?
    filelib:ensure_dir(filename:join(Dir, "d")),
    case ra_log_snapshot:write(Dir, Meta, <<>>) of
        ok ->
            {ok, {start_saved_state, Crc, Dir}};
        {error, _} = Err ->
            Err
    end.

%% TODO: return error on invalid function clause

%% accept a chunk of data
-spec accept_chunk(Chunk :: term(), AcceptState :: term()) ->
    {ok, AcceptState :: term()} | {error, term()}.
accept_chunk({saved_state_full, SavedStateBin}, {start_saved_state, Crc, Dir}) ->
    case accept_saved_state(SavedStateBin, state_file(Dir), Crc) of
        ok               -> {ok, {start_mnesia_backup, Dir}};
        {error, _} = Err -> Err
    end;
accept_chunk({saved_state_partial, Chunk}, {start_saved_state, Crc, Dir}) ->
    {ok, {saved_state_partial, Chunk, Crc, Dir}};
accept_chunk({saved_state_partial, Chunk}, {saved_state_partial, StatePartial, Crc, Dir}) ->
    {ok, {saved_state_partial, <<StatePartial/binary, Chunk/binary>>, Crc, Dir}};
accept_chunk({mnesia_backup_chunk, Chunk}, {saved_state_partial, StateFull, Crc, Dir}) ->
    case accept_saved_state(StateFull, state_file(Dir), Crc) of
        ok ->
            %% TODO: error handling
            MnesiaFile = mnesia_file(Dir),
            {ok, Fd} = file:open(MnesiaFile, [write, binary, raw]),
            ok = file:write(Fd, Chunk),
            {ok, {mnesia_backup_file, Fd}};
        {error, _} = Err ->
            Err
    end;
accept_chunk({mnesia_backup_chunk, Chunk}, {mnesia_backup_file, Fd}) ->
    ok = file:write(Fd, Chunk),
    {ok, {mnesia_backup_file, Fd}}.

accept_saved_state(Data, File, Crc) ->
    Crc = erlang:crc32(Data),
    file:write_file(File, <<Crc:32/integer, Data/binary>>).

%% accept the last chunk of data
-spec complete_accept(Chunk :: term(),
                          AcceptState :: term()) ->
    ok | {error, term()}.
complete_accept({mnesia_backup_chunk, Chunk}, {saved_state_partial, StateFull, Crc, Dir}) ->
    case accept_saved_state(StateFull, state_file(Dir), Crc) of
        ok ->
            %% TODO: error handling
            MnesiaFile = mnesia_file(Dir),
            file:write_file(MnesiaFile, Chunk);
        {error, _} = Err ->
            Err
    end;
complete_accept({mnesia_backup_chunk, Chunk}, {start_mnesia_backup, Dir}) ->
    file:write_file(mnesia_file(Dir), Chunk);
complete_accept({mnesia_backup_chunk, Chunk}, {mnesia_backup_file, Fd}) ->
    ok = file:write(Fd, Chunk),
    file:close(Fd).

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

