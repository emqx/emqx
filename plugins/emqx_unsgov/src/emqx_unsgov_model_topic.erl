%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_model_topic).

-moduledoc """
Traverses compiled topic trees and validates publish topics.
""".

-export([validate_topic/2, walk_to_node/2]).

validate_topic(Compiled, Topic) when is_map(Compiled), is_binary(Topic) ->
    case walk_to_node(Compiled, Topic) of
        {ok, Node} ->
            case maps:get(kind, Node) of
                endpoint ->
                    allow;
                _ ->
                    {deny, not_endpoint}
            end;
        {error, Reason} ->
            {deny, Reason}
    end.

walk_to_node(Compiled, Topic) ->
    Segments = emqx_topic:tokens(Topic),
    walk(maps:get(root, Compiled), Segments).

walk(Node, []) ->
    LiteralChildren = maps:get(literal_children, Node, #{}),
    case maps:find(<<"#">>, LiteralChildren) of
        {ok, HashNode} -> {ok, HashNode};
        error -> {ok, Node}
    end;
walk(#{kind := endpoint}, [_ | _]) ->
    {error, topic_invalid};
walk(Node, [Segment | Rest]) ->
    LiteralChildren = maps:get(literal_children, Node, #{}),
    case maps:find(Segment, LiteralChildren) of
        {ok, Next} ->
            walk(Next, Rest);
        error ->
            VariableChildren = maps:get(variable_children, Node, []),
            case match_variable(VariableChildren, Segment) of
                {ok, Next2} ->
                    walk(Next2, Rest);
                error ->
                    case maps:find(<<"#">>, LiteralChildren) of
                        {ok, HashNode} -> {ok, HashNode};
                        error -> {error, topic_invalid}
                    end
            end
    end.

match_variable([], _Segment) ->
    error;
match_variable([Node | Rest], Segment) ->
    case emqx_unsgov_model_schema:match_segment(maps:get(matcher, Node, any), Segment) of
        true -> {ok, Node};
        false -> match_variable(Rest, Segment)
    end.
