-module(simple_dgraph).

%% Minimal graph data structure.
%% Only suitable for use in mnevis_machine.

-export([
    new/0,
    add_vertex/2,
    add_vertex/3,

    ensure_vertex/2,
    del_vertex/2,

    vertex_label/2,

    in_neighbours/2,
    out_neighbours/2,

    in_edges/2,
    out_edges/2,

    add_edge/3,
    del_edge/2,
    del_edges/2
]).

-type vertex_name() :: term().
-type edge() :: {vertex_name(), vertex_name()}.

-record(graph, {
    labels = #{} :: #{vertex_name() => term()},
    out_edges = #{} :: #{vertex_name() => sets:set(vertex_name())},
    in_edges = #{} :: #{vertex_name() => sets:set(vertex_name())}
}).

-type graph() :: #graph{}.

-export_type([graph/0]).

new() ->
    #graph{}.

-spec add_vertex(#graph{}, vertex_name()) -> #graph{}.
add_vertex(Graph, VertexName) ->
    add_vertex(Graph, VertexName, []).

-spec ensure_vertex(#graph{}, vertex_name()) -> #graph{}.
ensure_vertex(Graph, VertexName) ->
    case vertex_label(Graph, VertexName) of
        error -> add_vertex(Graph, VertexName, []);
        _     -> Graph
    end.

-spec add_vertex(#graph{}, vertex_name(), term()) -> #graph{}.
add_vertex(Graph, VertexName, Label) ->
    Graph#graph{labels = maps:put(VertexName, Label, Graph#graph.labels)}.

-spec vertex_label(#graph{}, vertex_name()) -> {ok, term()} | error.
vertex_label(Graph, VertexName) ->
    maps:find(VertexName, Graph#graph.labels).

-spec add_edge(#graph{}, vertex_name(), vertex_name()) -> {ok, #graph{}} | {error, {bad_vertex, vertex_name()}}.
add_edge(Graph, From, To) ->
    case validate_vertices(Graph, From, To) of
        ok ->
            OldOut = maps:get(From, Graph#graph.out_edges, sets:new()),
            OldIn = maps:get(To, Graph#graph.in_edges, sets:new()),

            {ok, Graph#graph{
                     out_edges = maps:put(From,
                                          sets:add_element(To, OldOut),
                                          Graph#graph.out_edges),
                     in_edges = maps:put(To,
                                         sets:add_element(From, OldIn),
                                         Graph#graph.in_edges)
                 }};
        {error, {bad_vertex, _}} = Err ->
            Err
    end.

validate_vertices(Graph, From, To) ->
    case vertex_label(Graph, From) of
        error -> {error, {bad_vertex, From}};
        _ ->
            case vertex_label(Graph, To) of
                error -> {error, {bad_vertex, To}};
                _ -> ok
            end
    end.

-spec del_edges(#graph{}, [edge()]) -> #graph{}.
del_edges(Graph, Edges) ->
    lists:foldl(fun(Edge, G) ->
        del_edge(G, Edge)
    end,
    Graph,
    Edges).

-spec del_edge(#graph{}, edge()) -> #graph{}.
del_edge(Graph, {From, To}) ->
    OldOut = maps:get(From, Graph#graph.out_edges, sets:new()),
    OldIn = maps:get(To, Graph#graph.in_edges, sets:new()),

    NewOut = sets:del_element(To, OldOut),
    NewIn = sets:del_element(From, OldIn),

    Graph#graph{
        out_edges = case sets:size(NewOut) == 0 of
            true  -> maps:remove(From, Graph#graph.out_edges);
            false -> maps:put(From, NewOut, Graph#graph.out_edges)
        end,
        in_edges = case sets:size(NewIn) == 0 of
            true  -> maps:remove(To, Graph#graph.in_edges);
            false -> maps:put(To, NewIn, Graph#graph.in_edges)
        end
    }.

-spec in_edges(#graph{}, vertex_name()) -> [edge()].
in_edges(Graph, Vertex) ->
    [ {From, Vertex} || From <- in_neighbours(Graph, Vertex) ].

-spec out_edges(#graph{}, vertex_name()) -> [edge()].
out_edges(Graph, Vertex) ->
    [ {Vertex, To} || To <- out_neighbours(Graph, Vertex) ].

-spec out_neighbours(#graph{}, vertex_name()) -> [vertex_name()].
out_neighbours(Graph, Vertex) ->
    Out = maps:get(Vertex, Graph#graph.out_edges, sets:new()),
    sets:to_list(Out).

-spec in_neighbours(#graph{}, vertex_name()) -> [vertex_name()].
in_neighbours(Graph, Vertex) ->
    Out = maps:get(Vertex, Graph#graph.in_edges, sets:new()),
    sets:to_list(Out).

-spec del_vertex(#graph{}, vertex_name()) -> #graph{}.
del_vertex(Graph, Vertex) ->
    OutEdges = out_edges(Graph, Vertex),
    InEdges = in_edges(Graph, Vertex),
    Graph1 = del_edges(Graph, OutEdges ++ InEdges),
    Graph1#graph{labels = maps:remove(Vertex, Graph1#graph.labels)}.



