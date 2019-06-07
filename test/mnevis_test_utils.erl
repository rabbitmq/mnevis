-module(mnevis_test_utils).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

create_initial_nodes(NodePrefix) ->
    NodePrefixStr = atom_to_list(NodePrefix),
    NP1 = NodePrefixStr ++ "_slave_01",
    NP2 = NodePrefixStr ++ "_slave_02",
    Nodes = [node() | [start_erlang_node(NodeP) || NodeP <- [NP1, NP2]]],
    {ok, Nodes}.

start_erlang_node(NodePrefix) ->
    {ok, Host} = inet:gethostname(),
    {ok, Node} = slave:start(Host, NodePrefix),
    add_paths(Node),
    Node.

add_paths(Node) ->
    LocalPath = code:get_path(),
    ok = rpc:call(Node, code, add_pathsa, [LocalPath]).

start_cluster(Nodes, Config0) ->
    PrivDir = ?config(priv_dir, Config0),
    filelib:ensure_dir(PrivDir),
    [start_node(Node, PrivDir) || Node <- Nodes],
    {Name, _} = mnevis_node:node_id(),
    Servers = [{Name, Node} || Node <- Nodes],
    {ok, _, _} = ra:start_cluster(Name, {module, mnevis_machine, #{}}, Servers),
    Config1 = [{ra, enabled} | Config0],
    {ok, Config1}.

start_server(Node, Nodes) ->
    {Name, _} = mnevis_node:node_id(),
    Servers = [{Name, N} || N <- Nodes],
    ra:start_server(Name, {Name, Node}, {module, mnevis_machine, #{}}, Servers).

start_node(Node, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Node = rpc:call(Node, erlang, node, []),
    rpc:call(Node, application, load, [ra]),
    ok = rpc:call(Node, application, set_env, [ra, data_dir, filename:join(PrivDir, Node)]),
    {ok, _} = rpc:call(Node, application, ensure_all_started, [mnevis]),
    ok.

stop_all(Config) ->
    case ?config(nodes, Config) of
        undefined ->
            ok;
        Nodes ->
            [slave:stop(Node) || Node <- Nodes, Node =/= node()]
    end,
    application:stop(mnevis),
    application:stop(mnesia),
    case ?config(ra, Config) of
        undefined ->
            ok;
        enabled ->
            ra:stop_server(mnevis_node:node_id())
    end,
    application:stop(ra),
    % NB: slaves are temporary, no need to delete schema
    mnesia:delete_schema([node()]),
    Config.

ensure_not_leader() ->
    {ok, _, Leader} = ra:members(mnevis_node:node_id()),
    Node = node(),
    case Leader of
        {_, Node} ->
            ra:stop_server(Leader),
            ra:restart_server(Leader),
            ensure_not_leader();
        _ ->
            ok
    end.
