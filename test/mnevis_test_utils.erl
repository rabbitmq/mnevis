-module(mnevis_test_utils).

-compile(export_all).

extra_nodes() ->
    [mnevis_snapshot_SUITE1, mnevis_snapshot_SUITE2].

create_initial_nodes() ->
    [node() | [start_erlang_node(NodeP) || NodeP <- extra_nodes()]].

start_erlang_node(NodePrefix) ->
    {ok, Host} = inet:gethostname(),
    LocalPath = code:get_path(),
    {ok, Node} = slave:start(Host, NodePrefix),
    LocalPath = code:get_path(),
    add_paths(Node, LocalPath),
    Node.

add_paths(Node, LocalPath) ->
    RemotePath = rpc:call(Node, code, get_path, []),
    AddPath = LocalPath -- RemotePath,
    ok = rpc:call(Node, code, add_pathsa, [AddPath]).

start_cluster(Nodes, PrivDir) ->
    [start_node(Node, Nodes, PrivDir) || Node <- Nodes],
    {Name, _} = mnevis_node:node_id(),
    Servers = [{Name, Node} || Node <- Nodes],
    {ok, _, _} = ra:start_cluster(Name, {module, mnevis_machine, #{}}, Servers).

start_server(Node, Nodes) ->
    {Name, _} = mnevis_node:node_id(),
    Servers = [{Name, Node} || Node <- Nodes],
    ra:start_server(Name, {Name, Node}, {module, mnevis_machine, #{}}, Servers).

start_node(Node, Nodes, PrivDir) ->
    Node = rpc:call(Node, erlang, node, []),
    rpc:call(Node, application, load, [ra]),
    ok = rpc:call(Node, application, set_env, [ra, data_dir, filename:join(PrivDir, Node)]),
    {ok, _} = rpc:call(Node, application, ensure_all_started, [mnevis]),
    ok.

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