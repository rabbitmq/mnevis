PROJECT = mnevis
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = ra map_sts
LOCAL_DEPS = mnesia
TEST_DEPS = proper

dep_map_sets = git https://github.com/k32/map_sets.git master
dep_ra = git https://github.com/rabbitmq/ra master

include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)

repl: all
	erl -pa ebin -pa deps/*/ebin -sname foo
