PROJECT = ramnesia
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0
PROJECT_MOD = ramnesia

DEPS = ra
LOCAL_DEPS = mnesia
TEST_DEPS = proper

NODE_NAME ?= rmns
RELX_REPLACE_OS_VARS = true
INITIAL_NODES ?= $(NODE_NAME)

export NODE_NAME
export RELX_REPLACE_OS_VARS
export INITIAL_NODES

dep_ra = git https://github.com/rabbitmq/ra master

include erlang.mk

clean:: distclean-relx-rel

repl:
	erl -pa ebin -pa deps/*/ebin -sname foo

run-cluster:
	$(verbose) NODE_NAME=rmns1 INITIAL_NODES=rmns1,rmns2,rmns3 $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/bin/$(RELX_REL_NAME)$(RELX_REL_EXT) start
	$(verbose) NODE_NAME=rmns2 INITIAL_NODES=rmns1,rmns2,rmns3 $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/bin/$(RELX_REL_NAME)$(RELX_REL_EXT) start
	$(verbose) NODE_NAME=rmns3 INITIAL_NODES=rmns1,rmns2,rmns3 $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/bin/$(RELX_REL_NAME)$(RELX_REL_EXT) start
	$(verbose) NODE_NAME=rmns2 $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/bin/$(RELX_REL_NAME)$(RELX_REL_EXT) attach
