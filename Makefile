PROJECT = ramnesia
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = ra
LOCAL_DEPS = mnesia

dep_ra = git https://github.com/rabbitmq/ra master

include erlang.mk
