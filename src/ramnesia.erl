-module(ramnesia).

-export([transaction/3]).
% -behaviour(mnesia_access).

%% mnesia_access behaviour
-export([
    lock/4,
    %% Write ops
    write/5,
    delete/5,
    delete_object/5,

    %% Read ops
    read/5,
    match_object/5,
    all_keys/4,
    first/3,
    last/3,
    index_match_object/6,
    index_read/6,
    table_info/4,
    prev/4,
    next/4
    ]).

-record(context, {
    transaction_id,
    delete = #{},
    delete_object = #{},
    write_set = #{},
    write_bag = #{}}).

transaction(Fun, Args, Retries) ->
    transaction(Fun, Args, Retries, none).


transaction(Fun, Args, Retries, _Err) ->
    start_transaction_context(),

    try mnesia:activity(ets, Fun, Args, ramnesia) of
        Res ->

            %% TODO:
            %% Apply writes
            %% Apply deletes
            %% Finish transaction
            Res
    %% TODO: check for abort error
    catch error:Reason ->
        transaction(Fun, Args, Retries - 1, Reason)
    end.

start_transaction_context() ->
    update_transaction_context(#context{}).

update_transaction_context(Context) ->
    put(ramnesia_transaction_context, Context).

get_transaction_context() ->
    get(ramnesia_transaction_context).

transaction_id(#context{transaction_id = TransactionID}) ->
    TransactionID.

%% TODO: writes before/after delete, delte before/after write.
add_write_set(#context{write_set = WriteSet} = Context, Tab, Rec, LockKind) ->
    Key = record_key(Rec),
    Context#context{write_set = maps:put({Tab, Key}, {Tab, Rec, LockKind}, WriteSet)}.

add_write_bag(#context{write_bag = WriteBag} = Context, Tab, Rec, LockKind) ->
    Key = record_key(Rec),
    Item = {Tab, Rec, LockKind},
    OldBag = maps:get(Key, WriteBag, []) -- [Item],
    Context#context{write_bag = maps:put({Tab, Key}, [Item | OldBag], WriteBag)}.

add_delete(#context{delete = Delete,
                    write_set = WriteSet,
                    write_bag = WriteBag} = Context,
            Tab, Key, LockKind) ->
    Context#context{delete = maps:put({Tab, Key}, {Tab, Key, LockKind}, Delete),
                    write_set = maps:remove({Tab, Key}, WriteSet),
                    write_bag = maps:remove({Tab, Key}, WriteBag)}.

add_delete_object(#context{write_set = WriteSet} = Context, Tab, Rec, LockKind) ->
    Key = record_key(Rec),
    Item = {Tab, Rec, LockKind},
    case maps:get({Tab, Key}, WriteSet, not_found) of
        not_found ->
            add_delete_object_1(Context, Tab, Rec, LockKind);
        %% Delete matches the changed object
        {Tab, Rec, _LockKind} ->
            Context1 = add_delete_object_1(Context, Tab, Rec, LockKind),
            Context1#context{write_set = maps:remove({Tab, Key}, WriteSet)};
        %% Record have changed. No delete required.
        {Tab, _NotRec, _LockKind} ->
            Context
    end.

add_delete_object_1(#context{delete_object = DeleteObject,
                             write_bag = WriteBag} = Context,
                    Tab, Rec, LockKind) ->
    WriteBagItems = lists:filter(fun({_Tab, WriteRec, _LockKind}) ->
        case WriteRec of
            Rec -> false;
            _   -> true
        end
    end,
    maps:get({Tab, Key}, WriteBag, [])),
    OldDeleteItems = maps:get({Tab, Key}, DeleteObject, []) -- [Item],
    Context#context{delete_object = maps:put({Tab, Key}, [Item | OldDeleteItems], DeleteObject),
                    write_bag = maps:put({Tab, Key}, WriteBagItems, WriteBag)}.

read_from_context(#context{write_set = WriteSet, delete = Delete}, Tab, Key) ->
    case maps:get({Tab, Key}, Delete, not_found) of
        not_found ->
            case maps:get({Tab, Key}, WriteSet, not_found) of
                not_found -> not_found;
                {Tab, Rec, _LockKind} -> {written, set, Rec}
            end;
        {Tab, Key, _LockKind} ->
            deleted
    end.

lock(_ActivityId, _Opaque, LockItem, LockKind) ->
    execute_command(lock, [LockItem, LockKind]).

write(ActivityId, Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    Context1 = case mnesia:table_info(ActivityId, Opaque, Tab, type) of
        bag ->
            add_write_bag(Context, Tab, Rec, LockKind);
        Set when Set =:= set; Set =:= ordered_set ->
            add_write_set(Context, Tab, Rec, LockKind)
    end,
    update_transaction_context(Context1),
    execute_command(Context1, lock, [{Tab, record_key(Rec)}, LockKind]).

delete(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context = get_transaction_context(),
    Context1 = add_delete(Context, Tab, Key, LockKind),
    update_transaction_context(Context1),
    execute_command(Context1, lock, [{Tab, Key}, LockKind]).

delete_object(_ActivityId, _Opaque, Tab, Rec, LockKind) ->
    Context = get_transaction_context(),
    Context1 = add_delete_object(Context, Tab, Rec, LockKind),
    update_transaction_context(Context1),
    execute_command(Context1, lock, [{Tab, record_key(Rec)}, LockKind]).

read(_ActivityId, _Opaque, Tab, Key, LockKind) ->
    Context = get_transaction_context(),
    case read_from_context(Context, Tab, Key) of
        {written, set, Record} -> [Record];
        deleted -> [];
        not_found ->
            RecList = execute_command(Context, read, [Tab, Key, LockKind]),
            filter_from_context(Context, Tab, Key, RecList)
    end.

match_object(_ActivityId, _Opaque, Tab, Pattern, LockKind) ->
    Context = get_transaction_context(),
    RecList = execute_command(Context, match_object, [Tab, Pattern, LockKind]),
    filter_match_from_context(Context, Tab, Pattern, RecList).

all_keys(_ActivityId, _Opaque, Tab, LockKind) ->
    execute_command(all_keys, [Tab, LockKind]).

%% TODO consider cache of written/deleted
first(_ActivityId, _Opaque, Tab) ->
    execute_command(first, [Tab]).

last(_ActivityId, _Opaque, Tab) ->
    execute_command(last, [Tab]).

prev(_ActivityId, _Opaque, Tab, Key) ->
    execute_command(prev, [Tab, Key]).

next(_ActivityId, _Opaque, Tab, Key) ->
    execute_command(next, [Tab, Key]).

index_match_object(_ActivityId, _Opaque, Tab, Pattern, Pos, LockKind) ->
    Context = get_transaction_context(),
    RecList = execute_command(Context, index_match_object, [Tab, Pattern, Pos, LockKind]),
    filter_match_from_context(Context, Tab, Pattern, RecList).

index_read(_ActivityId, _Opaque, Tab, SecondaryKey, Pos, LockKind) ->
    Context = get_transaction_context(),
    RecList = do_index_read(Context, Tab, SecondaryKey, Pos, LockKind),
    filter_index_from_context(Context, Tab, SecondaryKey, Pos, RecList).

table_info(ActivityId, Opaque, Tab, InfoItem) ->
    mnesia:table_info(ActivityId, Opaque, Tab, InfoItem).

execute_command(Command, Args) ->
    Context = get_transaction_context(),
    execute_command(Context, Command, Args).

execute_command(Context, Command, Args) ->
    NodeId = ramnesia_node:node_id(),
    Command = {Command, transaction_id(Context), self(), Args},
    case ra:send_and_await_consensus(NodeId, Command) of
        {ok, {ok, Result}, _}    -> Result;
        %% TODO: some errors should restart, not abort.
        {ok, {error, Reason}, _} -> mnesia:abort(Reason);
        {error, Reason}          -> mnesia:abort(Reason);
        {timeout, _}             -> mnesia:abort(timeout)
    end.

do_index_read(Context, Tab, SecondaryKey, Pos, LockKind) ->
    execute_command(Context, index_read, [Tab, SecondaryKey, Pos, LockKind]).

record_key(Record) ->
    element(2, Record).


filter_from_context(Context, Tab, Key, RecList) ->
    #context{write_bag = WriteBag,
             delete_object = DeleteObject} = Context,
    Deleted = get_records(maps:get({Tab, Key}, DeleteObject, [])),
    Added = get_records(maps:get({Tab, Key}, WriteBag, [])),
    lists:usort(RecList ++ Added) -- Deleted.

filter_match_from_context(Context, Tab, Pattern, RecList) ->
    #context{write_bag = WriteBag,
             write_set = WriteSet,
             delete_object = DeleteObject,
             delete = Delete} = Context,
    lists:flatmap(fun(Rec) ->
        Key = record_key(Rec),
        CacheKey = {Tab, Key},
        RecsRewritten = case maps:get(CacheKey, WriteSet, not_found) of
            not_found -> [Rec];
            {Tab, NewRec, _LockKind} ->
                match_pattern(Pattern, [NewRec])
        end,
        RecsAdded = RecsRewritten ++ match_pattern(Pattern, get_records(maps:get(CacheKey, WriteBag, []))),
        RecsNotDeleted = case maps:get(CacheKey, Delete, not_found) of
            not_found -> RecsAdded;
            _         -> []
        end,
        RecsNotDeleted -- get_records(maps:get(CacheKey, DeleteObject, []))
    end,
    RecList).


filter_index_from_context(Context, Tab, SecondaryKey, Pos, RecList) ->
    #context{write_bag = WriteBag,
             write_set = WriteSet,
             delete_object = DeleteObject,
             delete = Delete} = Context,
    lists:flatmap(fun(Rec) ->
        Key = record_key(Rec),
        CacheKey = {Tab, Key},
        RecsRewritten = case maps:get(CacheKey, WriteSet, not_found) of
            not_found -> [Rec];
            {Tab, NewRec, _LockKind} ->
                case element(Pos, NewRec) == SecondaryKey of
                    true  -> [NewRec];
                    false -> []
                end
        end,
        RecsAdded = RecsRewritten ++
                    maps:fold(
                        fun({Table, _}, {Table, WrittenRec, _LockKind}, Acc) when Table == Tab ->
                            case element(Pos, WrittenRec) == SecondaryKey of
                                true  -> [WrittenRec | Acc];
                                false -> Acc
                            end;
                            (_, _, Acc) -> Acc
                        end,
                        [],
                        WriteSet) ++
                    maps:fold(
                        fun({Table, _}, Items, Acc) when Table == Tab ->
                            WrittenRecs = get_records(Items),
                            lists:filter(fun(WrittenRec) ->
                                element(Pos, WrittenRec) == SecondaryKey
                            end,
                            WrittenRecs) ++ Acc;
                            (_, _, Acc) -> Acc
                        end,
                        [],
                        WriteBag),
        lists:filter(fun(RecAdded) ->
            Key = record_key(RecAdded),
            case maps:is_key({Tab, Key}, Delete) of
                true  -> false;
                false ->
                    case maps:get({Tab, Key}, DeleteObject, []) of
                        [] -> true;
                        DeletedObjects -> not lists:member(RecAdded, get_records(DeletedObjects))
                    end
            end
        end,
        RecsAdded)

    end,
    RecList).

get_records(Items) ->
    lists:map(fun({_, Rec, _}) -> Rec end, Items).

match_pattern(_Pattern, []) -> [];
match_pattern(Pattern, RecList) ->
    MatchSpec = [{Pattern, [], ['$_']}],
    CompiledMatchSpec = ets:match_spec_compile(MatchSpec),
    ets:match_spec_run(RecList, CompiledMatchSpec).

