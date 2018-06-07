-module(ramnesia_context).

-export([init/1,
         add_write_set/4, add_write_bag/4, add_delete/4, add_delete_object/4,
         read_from_context/3,
         filter_read_from_context/4,
         filter_match_from_context/4,
         filter_index_from_context/5,
         transaction_id/1,
         writes/1,
         deletes/1,
         deletes_object/1]).

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
%     Use operation ordering
%     Deletes should apply before writes

% Delete object:
%     Skip if write_set changed the record
%     Clean write_set if the same object
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
    transaction_id,
    delete = #{},
    delete_object = #{},
    write_set = #{},
    write_bag = #{}}).

init(Tid) -> #context{transaction_id = Tid}.

transaction_id(#context{transaction_id = Tid}) -> Tid.

deletes(#context{delete = Delete}) -> Delete.

deletes_object(#context{delete_object = DeleteObject}) -> DeleteObject.

writes(#context{write_set = WriteSet, write_bag = WriteBag}) ->
    maps:values(WriteSet) ++ lists:append(maps:values(WriteBag)).

%% TODO: writes before/after delete, delte before/after write.
add_write_set(#context{write_set = WriteSet,
                       delete = Delete,
                       delete_object = DeleteObject} = Context,
              Tab, Rec, LockKind) ->
    Key = ramnesia:record_key(Rec),
    Item = {Tab, Rec, LockKind},
    Context#context{write_set = maps:put({Tab, Key}, Item, WriteSet),
                    delete = maps:remove({Tab, Key}, Delete),
                    delete_object = maps:remove({Tab, Key}, DeleteObject)}.

add_write_bag(#context{write_bag = WriteBag,
                       delete_object = DeleteObject} = Context,
              Tab, Rec, LockKind) ->
    Key = ramnesia:record_key(Rec),
    Item = {Tab, Rec, LockKind},
    OldBag = maps:get({Tab, Key}, WriteBag, []) -- [Item],
    DeleteObjectItems = maps:get({Tab, Key}, DeleteObject, []) -- [Item],
    Context#context{write_bag = maps:put({Tab, Key}, [Item | OldBag], WriteBag),
                    delete_object = maps:put({Tab, Key}, DeleteObjectItems, DeleteObject)}.

add_delete(#context{delete = Delete,
                    write_set = WriteSet,
                    write_bag = WriteBag,
                    delete_object = DeleteObject} = Context,
            Tab, Key, LockKind) ->
    Context#context{delete = maps:put({Tab, Key}, {Tab, Key, LockKind}, Delete),
                    write_set = maps:remove({Tab, Key}, WriteSet),
                    write_bag = maps:remove({Tab, Key}, WriteBag),
                    delete_object = maps:remove({Tab, Key}, DeleteObject)}.

add_delete_object(#context{write_set = WriteSet} = Context, Tab, Rec, LockKind) ->
    Key = ramnesia:record_key(Rec),
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
    Key = ramnesia:record_key(Rec),
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

filter_read_from_context(Context, Tab, Key, RecList) ->
    #context{write_bag = WriteBag,
             delete_object = DeleteObject} = Context,
    Deleted = get_records(maps:get({Tab, Key}, DeleteObject, [])),
    Added = get_records(maps:get({Tab, Key}, WriteBag, [])),
    lists:usort(RecList ++ Added) -- Deleted.

filter_from_context(Context, Fun, Tab, RecList) ->
    #context{write_set = WriteSet,
             write_bag = WriteBag,
             delete_object = DeleteObject,
             delete = Delete} = Context,
    RecListNotDeleted = lists:filter(fun(Rec) ->
        Key = ramnesia:record_key(Rec),
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
                     Fun(Rec)],
    WriteBagItems = [Item
                     || Item = {Table, Rec, _} <- lists:append(maps:values(WriteBag)),
                     Table == Tab,
                     Fun(Rec)],
    lists:usort(RecListNotDeleted ++ WriteSetItems ++ WriteBagItems).

filter_match_from_context(Context, Tab, Pattern, RecList) ->
    Predicate = fun(Rec) -> match_pattern(Pattern, [Rec]) == [Rec] end,
    filter_from_context(Context, Predicate, Tab, RecList).


filter_index_from_context(Context, Tab, SecondaryKey, Pos, RecList) ->
    Predicate = fun(Rec) -> element(Pos, Rec) == SecondaryKey end,
    filter_from_context(Context, Predicate, Tab, RecList).

get_records(Items) ->
    lists:map(fun({_, Rec, _}) -> Rec end, Items).

match_pattern(_Pattern, []) -> [];
match_pattern(Pattern, RecList) ->
    MatchSpec = [{Pattern, [], ['$_']}],
    CompiledMatchSpec = ets:match_spec_compile(MatchSpec),
    ets:match_spec_run(RecList, CompiledMatchSpec).

