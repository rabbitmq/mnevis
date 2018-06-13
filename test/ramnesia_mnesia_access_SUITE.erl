-module(ramnesia_mnesia_access_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).


all() ->
    [{group, tests}].


groups() ->
    [
     {tests, [], [
        lock,

        write,
        delete,
        delete_object,

        read,
        match_object,
        all_keys,
        first,
        last,
        index_match_object,
        index_read,
        table_info,
        prev,
        next
     ]}
    ].


lock(_Config) -> ok.

write(_Config) -> ok.
delete(_Config) -> ok.
delete_object(_Config) -> ok.

read(_Config) -> ok.
match_object(_Config) -> ok.
all_keys(_Config) -> ok.
first(_Config) -> ok.
last(_Config) -> ok.
index_match_object(_Config) -> ok.
index_read(_Config) -> ok.
table_info(_Config) -> ok.
prev(_Config) -> ok.
next(_Config) -> ok.