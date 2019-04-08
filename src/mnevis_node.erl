-module(mnevis_node).

-export([start/0, node_id/0]).
-export([add_node/1, remove_node/1]).
-export([make_initial_nodes/1]).

-define(START_TIMEOUT, 100000).

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
    Res = case ra_directory:uid_of(Name) of
        undefined ->
            ra:start_server(Name, NodeId, {module, mnevis_machine, #{}}, InitialNodes);
        _ ->
            ra:restart_server(NodeId)
    end,
    case Res of
        ok ->
            case ra:members(NodeId, ?START_TIMEOUT) of
                {ok, _, _} ->
                    ok;
                {timeout, _} = Err ->
                    %% TODO: improve error reporting
                    error_logger:error_msg("Timeout waiting for mnevis cluster"),
                    Err;
                Err ->
                    Err
            end;
        _ ->
            Res
    end.

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

add_node(Node) ->
    ra:add_member(node_id(), node_id(Node)).

remove_node(Node) ->
    ra:remove_member(node_id(), node_id(Node)).
