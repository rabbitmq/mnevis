-module(ramnesia_node).

-export([start/0, node_id/0]).
-export([make_initial_nodes/1]).

node_id() ->
    {ramnesia_node, node()}.

start() ->
    Name = ramnesia_node,
    NodeId = node_id(),
    InitialNodes = case application:get_env(ramnesia, initial_nodes) of
        undefined   -> [NodeId];
        {ok, Nodes} -> make_initial_nodes(Nodes)
    end,
    lists:foreach(fun(N) ->
        io:format("PING ~n"),
        net_adm:ping(N)
    end,
    InitialNodes),
    ok = ra:start_node(Name, NodeId, {module, ramnesia_machine, #{}}, InitialNodes)
    % ,
    % ok = ra:trigger_election(NodeId)
    .

make_initial_nodes(Nodes) ->
    [make_initial_node(Node) || Node <- Nodes].

make_initial_node(Node) ->
    NodeBin = atom_to_binary(Node, utf8),
    case string:split(NodeBin, "@", trailing) of
        [_N, _H] -> Node;
        [N] ->
            H = inet_db:gethostname(),
            binary_to_atom(iolist_to_binary([N, "@", H]), utf8)
    end.
