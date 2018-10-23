PROJECT = mnevis
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = ra
LOCAL_DEPS = mnesia
TEST_DEPS = proper

dep_ra = git https://github.com/rabbitmq/ra custom-snapshot

include erlang.mk

repl:
	erl -pa ebin -pa deps/*/ebin -sname foo
