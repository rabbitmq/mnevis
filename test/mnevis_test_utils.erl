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
    ok = ct_rpc:call(Node, code, add_pathsa, [LocalPath]).

start_cluster(Nodes, Config0) ->
    [start_node(Node, Config0) || Node <- Nodes],
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
    ok = filelib:ensure_dir(PrivDir),
    Node = ct_rpc:call(Node, erlang, node, []),
    ok = ct_rpc:call(Node, ?MODULE, node_setup, [PrivDir]),
    ok = ct_rpc:call(Node, application, set_env, [ra, data_dir, PrivDir]),
    ok = ct_rpc:call(Node, application, set_env, [mnesia, dir, PrivDir]),
    {ok, _} = ct_rpc:call(Node, application, ensure_all_started, [mnevis]),
    ok.

restart_slave(Node, Config) when is_atom(Node) ->
    Name = upto($@, atom_to_list(Node)),
    {ok, Host} = inet:gethostname(),
    {ok, Node} = slave:start(Host, Name),
    ok = add_paths(Node),
    ok = start_node(Node, Config),
    Nodes = ?config(nodes, Config),
    ok = mnevis_test_utils:start_server(Node, Nodes),
    {ok, Node}.

stop_all(Config) ->
    case ?config(nodes, Config) of
        undefined ->
            ok;
        Nodes ->
            ok = stop_nodes(Nodes, Config)
    end,
    application:stop(mnevis),
    application:stop(mnesia),
    case ?config(ra, Config) of
        undefined ->
            ok;
        enabled ->
            NodeId = mnevis_node:node_id(),
            ra:stop_server(NodeId),
            ra:force_delete_server(NodeId)
    end,
    application:stop(ra),
    % NB: slaves are temporary, no need to delete schema
    Node = node(),
    mnesia:delete_schema([Node]),
    ok = remove_node_data(Node, Config),
    Config.

stop_nodes([], _Config) ->
    ok;
stop_nodes([Node|T], Config) when node() =:= Node ->
    stop_nodes(T, Config);
stop_nodes([Node|T], Config) ->
    ok = stop_slave_node(Node, Config),
    stop_nodes(T, Config).

stop_slave_node(Node, Config) ->
    ok = slave:stop(Node),
    remove_node_data(Node, Config).

remove_node_data(Node, Config) ->
    NodeStr = atom_to_list(Node),
    PrivDir = ?config(priv_dir, Config),
    NodeDir = filename:join(PrivDir, NodeStr),
    ra_lib:recursive_delete(NodeDir).

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

node_setup(DataDir) ->
    Node = atom_to_list(node()),
    LogFile = filename:join([DataDir, Node, "mnevis.log"]),
    SaslFile = filename:join([DataDir, Node, "mnevis_sasl.log"]),
    logger:set_primary_config(level, debug),
    Config = #{config => #{type => {file, LogFile}}, level => debug},
    logger:add_handler(ra_handler, logger_std_h, Config),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:start(sasl),
    filelib:ensure_dir(LogFile),
    _ = error_logger:logfile({open, LogFile}),
    _ = error_logger:tty(false),
    ok.

upto(_, []) -> [];
upto(Char, [Char|_]) -> [];
upto(Char, [H|T]) -> [H|upto(Char, T)].
