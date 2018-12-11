-module(mnevis_context).

-export([init/0, init/3,
         add_write_set/4, add_write_bag/4, add_delete/4, add_delete_object/4,
         read_from_context/3,
         filter_read_from_context/4,
         filter_match_from_context/4,
         filter_index_from_context/5,
         filter_all_keys_from_context/3,
         transaction_id/1,
         locker_term/1,
         locker/1,
         writes/1,
         deletes/1,
         deletes_object/1,
         writes/2,
         deletes/2,
         deletes_object/2,
         key_deleted/3,
         delete_object_for_key/3,
         prev_cached_key/3,
         next_cached_key/3,
         set_retry/1,
         is_retry/1]).

% Keeps track of deletes and writes.

% Delete:
%     Should delete all writes with the same key
%     Clean write_set
%     Clean write_bag
%     Clean delete_object

% Write set:
%     Writes a new record
%     Clean delete for key
%     Clean delete_object key

% Write bag:
%     Need to keep deletes and add to (empty) bag
%     Need to clean delete_objects for this object
%     Use operation ordering!!!!!
%     Deletes should apply before writes

% Delete object:
%     Skip if write_set changed the record
%     Clean same objects from write_bag


% Read from cache:
%     If write_set: return written
%     If deleted and no write_bag: return empty
%     If deleted and write_bag: return cached bag

% Filter read:
%     If write_bag: add bag
%     If delete_object: filter deleted

% Filter match:
%     Filter step for each key:
%         Delete deleted keys
%         Delete delete_object
%         If write_set delete if does not match
%     Add step:
%         Run match on write_set cache
%         Run match on write_bag cache

% Filter index:
%     Filter step for each key:
%         Delete deleted keys
%         Delete delete_object
%         If write_set delete if does not match the key
%     Add step:
%         Run index filter on write_set cache
%         Run index filter on write_bag cache

-record(context, {
    transaction_id = undefined,
    locker_term = undefined,
    locker = undefined,
    delete = #{},
    delete_object = #{},
    write_set = #{},
    write_bag = #{},
    retry = false}).

-type context() :: #context{}.
-type record() :: tuple().
-type lock_kind() :: read | write.
-type key() :: term().
-type table() :: atom().
-type transaction_id() :: integer().
-type locker_term() :: integer().
-type delete_item() :: {table(), key(), lock_kind()}.
-type item() :: {table(), record(), lock_kind()}.

-export_type([context/0]).

-spec init() -> context().
init() -> #context{}.

-spec init(transaction_id(), locker_term(), pid()) -> context().
init(Tid, LockerTerm, Locker) -> #context{transaction_id = Tid,
                                          locker_term = LockerTerm,
                                          locker = Locker}.

-spec transaction_id(context()) -> transaction_id().
transaction_id(#context{transaction_id = Tid}) -> Tid.

-spec locker_term(context()) -> locker_term().
locker_term(#context{locker_term = LockerTerm}) -> LockerTerm.

-spec locker(context()) -> pid().
locker(#context{locker = Locker}) -> Locker.

-spec deletes(context()) -> [delete_item()].
deletes(#context{delete = Delete}) -> maps:values(Delete).

-spec deletes(context(), table()) -> [delete_item()].
deletes(#context{delete = Delete}, Tab) ->
    for_table(Tab, Delete).

-spec deletes_object(context()) -> [item()].
deletes_object(#context{delete_object = DeleteObject}) ->
    lists:append(maps:values(DeleteObject)).

-spec deletes_object(context(), table()) -> [item()].
deletes_object(#context{delete_object = DeleteObject}, Tab) ->
    lists:append(for_table(Tab, DeleteObject)).

-spec writes(context()) -> [item()].
writes(#context{write_set = WriteSet, write_bag = WriteBag}) ->
    maps:values(WriteSet) ++ lists:append(maps:values(WriteBag)).

-spec writes(context(), table()) -> [item()].
writes(#context{write_set = WriteSet, write_bag = WriteBag}, Tab) ->
    for_table(Tab, WriteSet) ++ lists:append(for_table(Tab, WriteBag)).

-spec for_table(table(), #{{table(), key()} => Item}) -> [Item].
for_table(Tab, Map) ->
    maps:fold(fun({Table, _}, V, Acc) when Table == Tab -> [V | Acc];
                 (_, _, Acc) -> Acc
              end,
              [],
              Map).

-spec key_deleted(context(), table(), key()) -> boolean().
key_deleted(#context{delete = Delete}, Tab, Key) ->
    case maps:get({Tab, Key}, Delete, not_found) of
        not_found -> false;
        _Item     -> true
    end.

-spec delete_object_for_key(context(), table(), key()) -> [item()].
delete_object_for_key(#context{delete_object = DeleteObject}, Tab, Key) ->
    maps:get({Tab, Key}, DeleteObject, []).

-spec add_write_set(context(), table(), record(), lock_kind()) -> context().
add_write_set(#context{write_set = WriteSet,
                       delete = Delete,
                       delete_object = DeleteObject} = Context,
              Tab, Rec, LockKind) ->
    Key = mnevis:record_key(Rec),
    Item = {Tab, Rec, LockKind},
    Context#context{write_set = maps:put({Tab, Key}, Item, WriteSet),
                    delete = maps:remove({Tab, Key}, Delete),
                    delete_object = maps:remove({Tab, Key}, DeleteObject)}.

-spec add_write_bag(context(), table(), record(), lock_kind()) -> context().
add_write_bag(#context{write_bag = WriteBag,
                       delete_object = DeleteObject} = Context,
              Tab, Rec, LockKind) ->
    Key = mnevis:record_key(Rec),
    Item = {Tab, Rec, LockKind},
    OldBag = maps:get({Tab, Key}, WriteBag, []) -- [Item],
    DeleteObjectItems = maps:get({Tab, Key}, DeleteObject, []) -- [Item],
    Context#context{write_bag = maps:put({Tab, Key}, [Item | OldBag], WriteBag),
                    delete_object = maps:put({Tab, Key}, DeleteObjectItems, DeleteObject)}.

-spec add_delete(context(), table(), key(), lock_kind()) -> context().
add_delete(#context{delete = Delete,
                    write_set = WriteSet,
                    write_bag = WriteBag,
                    delete_object = DeleteObject} = Context,
            Tab, Key, LockKind) ->
    Context#context{delete = maps:put({Tab, Key}, {Tab, Key, LockKind}, Delete),
                    write_set = maps:remove({Tab, Key}, WriteSet),
                    write_bag = maps:remove({Tab, Key}, WriteBag),
                    delete_object = maps:remove({Tab, Key}, DeleteObject)}.

-spec add_delete_object(context(), table(), record(), lock_kind()) -> context().
add_delete_object(#context{write_set = WriteSet} = Context, Tab, Rec, LockKind) ->
    Key = mnevis:record_key(Rec),
    case maps:get({Tab, Key}, WriteSet, not_found) of
        not_found ->
            add_delete_object_1(Context, Tab, Rec, LockKind);
        %% Delete matches the changed object
        {Tab, Rec, _LockKind} ->
            add_delete_object_1(Context, Tab, Rec, LockKind);
        %% Record have changed. No delete required.
        {Tab, _NotRec, _LockKind} ->
            Context
    end.

add_delete_object_1(#context{delete_object = DeleteObject,
                             write_bag = WriteBag} = Context,
                    Tab, Rec, LockKind) ->
    Key = mnevis:record_key(Rec),
    Item = {Tab, Rec, LockKind},
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

-spec read_from_context(context(), table(), key()) ->
    not_found |
    deleted |
    {written, set, record()} |
    {deleted_and_written, bag, [record()]}.
read_from_context(#context{write_set = WriteSet,
                           delete = Delete,
                           write_bag = WriteBag},
                  Tab, Key) ->
    case maps:get({Tab, Key}, Delete, not_found) of
        not_found ->
            case maps:get({Tab, Key}, WriteSet, not_found) of
                not_found -> not_found;
                {Tab, Rec, _LockKind} -> {written, set, Rec}
            end;
        {Tab, Key, _LockKind} ->
            case maps:get({Tab, Key}, WriteBag, not_found) of
                not_found -> deleted;
                Items     -> {deleted_and_written, bag, get_records(Items)}
            end
    end.

-spec filter_read_from_context(context(), table(), key(), [record()]) -> [record()].
filter_read_from_context(Context, Tab, Key, RecList) ->
    #context{write_bag = WriteBag,
             delete_object = DeleteObject} = Context,
    Deleted = get_records(maps:get({Tab, Key}, DeleteObject, [])),
    Added = get_records(maps:get({Tab, Key}, WriteBag, [])),
    lists:usort(RecList ++ Added) -- Deleted.

-spec filter_from_context(context(), fun((record()) -> boolean()), table(), [record()]) -> [record()].
filter_from_context(Context, Fun, Tab, RecList) ->
    #context{write_set = WriteSet,
             write_bag = WriteBag,
             delete_object = DeleteObject,
             delete = Delete} = Context,
    RecListNotDeleted = lists:filter(fun(Rec) ->
        Key = mnevis:record_key(Rec),
        CacheKey = {Tab, Key},
        case maps:get(CacheKey, Delete, not_found) of
            not_found ->
                case maps:get(CacheKey, DeleteObject, not_found) of
                    not_found ->
                        case maps:get(CacheKey, WriteSet, not_found) of
                            %% No changes for the record
                            not_found      -> true;
                            %% Changed record still matches
                            {_, NewRec, _} -> Fun(NewRec)
                        end;
                    DeleteObjectItems ->
                        %% There is no delete_object for the record
                        not lists:member(Rec, get_records(DeleteObjectItems))
                end;
            _ ->
                %% Record was deleted
                false
        end
    end,
    RecList),
    WriteSetItems = [Item
                     || Item = {Table, Rec, _} <- maps:values(WriteSet),
                     Table == Tab,
                     Fun(Rec),
                     not record_deleted_object(Rec, Table, DeleteObject)],
    WriteBagItems = [Item
                     || Item = {Table, Rec, _} <- lists:append(maps:values(WriteBag)),
                     Table == Tab,
                     Fun(Rec)],
    lists:usort(RecListNotDeleted ++ get_records(WriteSetItems) ++ get_records(WriteBagItems)).

record_deleted_object(Rec, Table, DeleteObject) ->
    lists:member(Rec, get_records(maps:get({Table, mnevis:record_key(Rec)}, DeleteObject, []))).

-spec filter_match_from_context(context(), table(), term(), [record()]) -> [record()].
filter_match_from_context(Context, Tab, Pattern, RecList) ->
    Predicate = fun(Rec) -> match_pattern(Pattern, Rec) end,
    filter_from_context(Context, Predicate, Tab, RecList).

-spec filter_index_from_context(context(), table(), term(), integer(), [record()]) -> [record()].
filter_index_from_context(Context, Tab, SecondaryKey, Pos, RecList) ->
    Predicate = fun(Rec) -> element(Pos, Rec) == SecondaryKey end,
    filter_from_context(Context, Predicate, Tab, RecList).

-spec filter_all_keys_from_context(context(), table(), [key()]) -> [key()].
filter_all_keys_from_context(Context, Tab, Keys) ->
    #context{delete_object = DeleteObject} = Context,
    DeleteKeys = [ Key || {_, Key, _} <- deletes(Context, Tab) ],
    WriteKeys = [ mnevis:record_key(Rec) || {_, Rec, _} <- writes(Context, Tab),
                  not record_deleted_object(Rec, Tab, DeleteObject) ],
    lists:usort(Keys ++ WriteKeys) -- DeleteKeys.

-spec get_records([item()]) -> [record()].
get_records(Items) ->
    lists:map(fun({_, Rec, _}) -> Rec end, Items).

-spec match_pattern(term(), tuple()) -> boolean().
match_pattern(Pattern, Rec) ->
    MatchSpec = [{Pattern, [], ['$_']}],
    CompiledMatchSpec = ets:match_spec_compile(MatchSpec),
    ets:match_spec_run([Rec], CompiledMatchSpec) == [Rec].


-spec prev_cached_key(context(), table(), key()) -> key().
prev_cached_key(Context, Tab, Key) ->
    #context{write_bag = WriteBag,
             write_set = WriteSet,
             delete_object = DeleteObject} = Context,
    BagKeys = [K || {T, K} <- maps:keys(WriteBag), T == Tab, K < Key],
    SetKeys = maps:fold(fun({T, K}, Rec, Acc) ->
        case T of
            Tab ->
                case K < Key andalso not record_deleted_object(Rec, Tab, DeleteObject) of
                    true  -> [K | Acc];
                    false -> Acc
                end;
            _ -> Acc
        end
    end,
    [],
    WriteSet),
    case BagKeys ++ SetKeys of
        []   -> '$end_of_table';
        Keys -> lists:max(Keys)
    end.

-spec next_cached_key(context(), table(), key()) -> key().
next_cached_key(Context, Tab, Key) ->
    #context{write_bag = WriteBag,
             write_set = WriteSet,
             delete_object = DeleteObject} = Context,
    BagKeys = [K || {T, K} <- maps:keys(WriteBag), T == Tab, K > Key],
    SetKeys = maps:fold(fun({T, K}, Rec, Acc) ->
        case T of
            Tab ->
                case K > Key andalso not record_deleted_object(Rec, Tab, DeleteObject) of
                    true  -> [K | Acc];
                    false -> Acc
                end;
            _ -> Acc
        end
    end,
    [],
    WriteSet),
    case BagKeys ++ SetKeys of
        []   -> '$end_of_table';
        Keys -> lists:min(Keys)
    end.

-spec set_retry(context()) -> context().
set_retry(Context) -> Context#context{retry = true}.

-spec is_retry(context()) -> boolean().
is_retry(#context{retry = Retry}) -> Retry == true.






