-module(mnevis_node).

-export([start/0, node_id/0]).
-export([make_initial_nodes/1]).

node_id() ->
    node_id(node()).

node_id(Node) ->
    {mnevis_node, Node}.

start() ->
    Name = mnevis_node,
    NodeId = node_id(),
    InitialNodes = case application:get_env(mnevis, initial_nodes) of
        undefined   -> [NodeId];
        {ok, []}    -> [NodeId];
        {ok, Nodes} -> make_initial_nodes(Nodes)
    end,
    lists:foreach(fun({_, N}) ->
        io:format("PING ~p~n", [N]),
        net_adm:ping(N)
    end,
    InitialNodes),
    {ok, _, _} = ra:start_cluster(Name, {module, mnevis_machine, #{}}, InitialNodes).


make_initial_nodes(Nodes) ->
    [make_initial_node(Node) || Node <- Nodes].

make_initial_node(Node) ->
    NodeBin = atom_to_binary(Node, utf8),
    case string:split(NodeBin, "@", trailing) of
        [_N, _H] -> node_id(Node);
        [N] ->
            H = inet_db:gethostname(),
            node_id(binary_to_atom(iolist_to_binary([N, "@", H]), utf8))
    end.
