-module(ramnesia_app).
-behaviour(application).

-export([start/0]).
-export([start/2]).
-export([stop/1]).

start() ->
    ok = application:load(ra),
    application:set_env(ra, data_dir, "/tmp/ramnesia"),
    application:ensure_all_started(ramnesia).

start(_Type, _Args) ->
    ramnesia_node:start(),
	ramnesia_sup:start_link().

stop(_State) ->
	ok.
