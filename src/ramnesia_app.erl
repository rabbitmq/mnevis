-module(ramnesia_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    application:set_env(ra, data_dir, "/tmp/ramnesia"),
    ramnesia_node:start(),
	ramnesia_sup:start_link().

stop(_State) ->
	ok.
