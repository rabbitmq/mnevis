-module(mnevis_context).

-export([init/0, init/1,
         is_empty/1,
         add_write_set/4,
         add_write_bag/4,
         add_delete/4,
         add_delete_object/4,
         add_read/3,
         get_read/2,
         filter_all_keys/3,
         filter_index/5,
         filter_match/4,
         filter_match_spec/4,
         filter_read/4,
         filter_tagged_match_spec/4,
         has_changes_for_table/2,
         is_version_up_to_date/2,
         mark_version_up_to_date/2,
         read/3,
         transaction/1,
         transaction_id/1,
         locker/1,
         locks/1,
         set_lock_acquired/3,

         has_transaction/1,
         assert_transaction/1,
         assert_no_transaction/1,

         set_transaction/2,
         cleanup_changes/1,

         writes/1,
         deletes/1,
         deletes_object/1,
         writes/2,
         deletes/2,
         deletes_object/2,

         key_deleted/3,
         delete_object_for_key/3,
         prev_cached_key/3,
         next_cached_key/3]).

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

-type read_op() :: dirty_all_keys | dirty_first | dirty_last |
                   dirty_index_match_object | dirty_index_read  |
                   dirty_match_object | dirty_read | dirty_prev |
                   dirty_next.

-type read_spec() :: {read_op(), [table() | key()]}.
-type version() :: non_neg_integer().
-type read_version() :: {table(), version()} | {{table(), integer()}, version()}.

-type record() :: tuple().
-type lock_kind() :: read | write.
-type key() :: term().
-type table() :: atom().

-type tabkey() :: {table(), key()}.

-type delete_item() :: {table(), key(), lock_kind()}.
-type item() :: {table(), record(), lock_kind()}.

-type transaction() :: {mnevis_lock:transaction_id(),
                        mnevis_lock_proc:locker()}.

-type locks() :: #{term() => lock_kind()}.

-record(context, {
    %% Set once. Transaction id and locker
    transaction = undefined :: transaction() | undefined,
    %% Add and rewrite map. Lock items and their respective lock kinds
    %% already aquired by the current transaction
    locks = #{} :: locks(),
    %% Read cache. Entries already read from the database
    %% in the current transaction.
    read = #{} :: #{read_spec() => [record()]},
    %% Write/delete cache.
    delete = #{} :: #{tabkey() => delete_item()},
    delete_object = #{} :: #{tabkey() => item()},
    write_set = #{} :: #{tabkey() => item()},
    write_bag = #{} :: #{tabkey() => [item()]},
    %% Mapset of lock items, for which transaction
    %% already waited for the version. No need to wait again.
    versions = map_sets:new() :: map_sets:set()}).

-type context() :: #context{}.

-export_type([context/0,
              item/0,
              delete_item/0,
              transaction/0,
              read_spec/0,
              record/0,
              version/0,
              read_version/0]).

-spec init() -> context().
init() -> #context{}.

-spec init(transaction()) -> context().
init(Transaction) -> #context{transaction = Transaction}.

-spec transaction(context()) -> transaction() | undefined.
transaction(#context{transaction = Transaction}) -> Transaction.

-spec transaction_id(context()) -> mnevis_lock:transaction_id().
transaction_id(#context{transaction = {Tid, _}}) -> Tid.

-spec locker(context()) -> mnevis_lock_proc:locker().
locker(#context{transaction = {_, Locker}}) -> Locker.

-spec locks(context()) -> locks().
locks(#context{locks = Locks}) -> Locks.

-spec set_lock_acquired(term(), lock_kind(), context()) -> context().
set_lock_acquired(LockItem, LockKind, #context{locks = Locks} = Context) ->
    Locks1 = case {LockKind, maps:get(LockItem, Locks, none)} of
        {_, none}     -> maps:put(LockItem, LockKind, Locks);
        {write, read} -> maps:put(LockItem, LockKind, Locks);
        _ -> Locks
    end,
    Context#context{locks = Locks1}.

-spec mark_version_up_to_date(term(), context()) -> context().
mark_version_up_to_date(LockItem, #context{versions = Versions} = Context) ->
    Context#context{versions = map_sets:add_element(LockItem, Versions)}.

-spec is_version_up_to_date(term(), context()) -> boolean().
is_version_up_to_date(LockItem, #context{versions = Versions}) ->
    map_sets:is_element(LockItem, Versions).

-spec has_transaction(context()) -> boolean().
has_transaction(#context{transaction = undefined}) -> false;
has_transaction(#context{transaction = {_, {_, _}}}) -> true.

-spec is_empty(context()) -> boolean().
is_empty(#context{delete = Delete, delete_object = DeleteObject,
                  write_set = WriteSet, write_bag = WriteBag, read = Read}) ->
    0 == maps:size(Delete) andalso
    0 == maps:size(DeleteObject) andalso
    0 == maps:size(WriteSet) andalso
    0 == maps:size(WriteBag) andalso
    0 == maps:size(Read).

-spec assert_transaction(context()) -> ok.
assert_transaction(Context) ->
    case has_transaction(Context) of
        true  -> ok;
        false -> error({mnevis_context, no_transaction_set})
    end.

-spec assert_no_transaction(context()) -> ok.
assert_no_transaction(Context) ->
    case has_transaction(Context) of
        true  -> error({mnevis_context, unexpected_transaction});
        false -> ok
    end.

-spec set_transaction(transaction(), context()) -> context().
set_transaction(Transaction, Context) ->
    assert_no_transaction(Context),
    Context#context{transaction = Transaction, locks = #{}}.

%% Remove everything except transaction.
-spec cleanup_changes(context()) -> context().
cleanup_changes(#context{transaction = Transaction}) ->
    #context{transaction = Transaction}.

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

-spec has_changes_for_table(table(), context()) -> boolean().
has_changes_for_table(Tab,
                      #context{write_set = WriteSet,
                               write_bag = WriteBag,
                               delete = Delete,
                               delete_object = DeleteObject}) ->
    has_for_table(Tab, WriteSet) orelse
    has_for_table(Tab, WriteBag) orelse
    has_for_table(Tab, Delete) orelse
    has_for_table(Tab, DeleteObject).

-spec has_for_table(table(), map()) -> boolean().
has_for_table(Tab, Map) ->
    lists:any(fun({T, _}) when T == Tab -> true;
                 (_) -> false
              end,
              maps:keys(Map)).

-spec key_deleted(context(), table(), key()) -> boolean().
key_deleted(#context{delete = Delete}, Tab, Key) ->
    case maps:get({Tab, Key}, Delete, not_found) of
        not_found -> false;
        _Item     -> true
    end.

-spec delete_object_for_key(context(), table(), key()) -> [item()].
delete_object_for_key(#context{delete_object = DeleteObject}, Tab, Key) ->
    maps:get({Tab, Key}, DeleteObject, []).

-spec add_read(context(), read_spec(), [record()]) -> context().
add_read(#context{read = Read0} = Context, ReadSpec, RecList) ->
    Read1 = maps:put(ReadSpec, RecList, Read0),
    Context#context{read = Read1}.

-spec get_read(context(), read_spec()) -> {ok, record()} | {error, not_found}.
get_read(#context{read = Read}, ReadSpec) ->
    case maps:get(ReadSpec, Read, not_found) of
        not_found ->
            {error, not_found};
        RecList ->
            {ok, RecList}
    end.

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

-spec read(context(), table(), key()) ->
    not_found |
    deleted |
    {written, set, record()} |
    {deleted_and_written, bag, [record()]}.
read(#context{write_set = WriteSet,
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

-spec filter_read(context(), table(), key(), [record()]) -> [record()].
filter_read(Context, Tab, Key, RecList) ->
    #context{write_bag = WriteBag,
             delete_object = DeleteObject} = Context,
    Deleted = get_records(maps:get({Tab, Key}, DeleteObject, [])),
    Added = get_records(maps:get({Tab, Key}, WriteBag, [])),
    lists:usort(RecList ++ Added) -- Deleted.

-spec filter(context(), table(), [Record],
                          fun((Record) -> boolean()),
                          fun((Record) -> term())) -> [Record].
filter(Context, Tab, RecList, Predicate, KeyFun) ->
    #context{write_set = WriteSet,
             write_bag = WriteBag,
             delete_object = DeleteObject,
             delete = Delete} = Context,
    RecListNotDeleted = lists:filter(fun(Rec) ->
        Key = KeyFun(Rec),
        CacheKey = {Tab, Key},
        case maps:get(CacheKey, Delete, not_found) of
            not_found ->
                case maps:get(CacheKey, DeleteObject, not_found) of
                    not_found ->
                        case maps:get(CacheKey, WriteSet, not_found) of
                            %% No changes for the record
                            not_found      -> true;
                            %% Changed record still matches
                            {_, NewRec, _} -> Predicate(NewRec)
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
                     Predicate(Rec),
                     not record_deleted_object(Rec, Table, DeleteObject)],
    WriteBagItems = [Item
                     || Item = {Table, Rec, _} <- lists:append(maps:values(WriteBag)),
                     Table == Tab,
                     Predicate(Rec)],
    lists:usort(RecListNotDeleted ++ get_records(WriteSetItems) ++ get_records(WriteBagItems)).

record_deleted_object(Rec, Table, DeleteObject) ->
    lists:member(Rec, get_records(maps:get({Table, mnevis:record_key(Rec)}, DeleteObject, []))).

-spec filter_match(context(), table(), term(), [record()]) -> [record()].
filter_match(Context, Tab, Pattern, RecList) ->
    MatchSpec = [{Pattern, [], ['$_']}],
    filter_match_spec(Context, Tab, MatchSpec, RecList).

-spec filter_match_spec(context(), table(), ets:match_spec(), [record()]) ->
    [record()].
filter_match_spec(Context, Tab, MatchSpec, RecList) ->
    %% TODO: we can filer a whole list with a match spec, not one-by-one.
    %% this could be more efficient, but I'm not sure
    Compiled = ets:match_spec_compile(MatchSpec),
    Predicate = fun(Rec) -> match_compiled(Compiled, Rec) end,
    filter(Context, Tab, RecList, Predicate, fun mnevis:record_key/1).

-spec filter_tagged_match_spec(context(), table(), ets:match_spec(), [{integer(), record()}]) ->
    [{integer(), record()}].
filter_tagged_match_spec(Context, Tab, MatchSpec, TaggedRecList) ->
    Compiled = ets:match_spec_compile(MatchSpec),
    Predicate = fun(Rec) -> match_compiled(Compiled, Rec) end,
    KeyFun = fun({_Teg, Rec}) -> mnevis:record_key(Rec) end,
    filter(Context, Tab, TaggedRecList, Predicate, KeyFun).

-spec filter_index(context(), table(), term(), integer(), [record()]) -> [record()].
filter_index(Context, Tab, SecondaryKey, Pos, RecList) ->
    Predicate = fun(Rec) -> element(Pos, Rec) == SecondaryKey end,
    filter(Context, Tab, RecList, Predicate, fun mnevis:record_key/1).

-spec filter_all_keys(context(), table(), [key()]) -> [key()].
filter_all_keys(Context, Tab, Keys) ->
    #context{delete_object = DeleteObject} = Context,
    DeleteKeys = [ Key || {_, Key, _} <- deletes(Context, Tab) ],
    WriteKeys = [ mnevis:record_key(Rec) || {_, Rec, _} <- writes(Context, Tab),
                  not record_deleted_object(Rec, Tab, DeleteObject) ],
    lists:usort(Keys ++ WriteKeys) -- DeleteKeys.

-spec get_records([item()]) -> [record()].
get_records(Items) ->
    lists:map(fun({_, Rec, _}) -> Rec end, Items).

-spec match_compiled(term(), tuple()) -> boolean().
match_compiled(Compiled, Rec) ->
    ets:match_spec_run([Rec], Compiled) == [Rec].

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






