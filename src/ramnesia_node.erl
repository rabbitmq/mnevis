-module(ramnesia_node).

-export([start/0, node_id/0]).


node_id() ->
    {ramnesia_node, node()}.

start() ->
    Uid = <<"ramnesia_node">>,
    Name = ramnesia_node,
    NodeId = node_id(),
    Config = #{id => NodeId,
               uid => Uid,
               cluster_id => Name,
               initial_nodes => [NodeId],
               log_init_args => #{uid => Uid},
               machine => {module, ramnesia_machine, #{}}},
    ok = ra:start_node(Config),
    ok = ra:trigger_election(NodeId).