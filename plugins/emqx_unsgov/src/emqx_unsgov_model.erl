%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_model).

-moduledoc """
Compiles UNS model JSON into runtime structures and topic filters.
""".

-export([
    compile/1,
    validate_topic/2,
    validate_message/4,
    screen_topic/2,
    summary/1
]).

-define(NAMESPACE, namespace).
-define(VARIABLE, variable).
-define(ENDPOINT, endpoint).

compile(RawModel0) when is_map(RawModel0) ->
    RawModel = normalize_map_keys(RawModel0),
    Id = read_bin(RawModel, [id], undefined),
    Name = read_bin(RawModel, [name], Id),
    Tree = read_map(RawModel, [tree], #{}),
    VarTypes = compile_var_types(read_map(RawModel, [variable_types], #{})),
    maybe
        ok ?= validate_model_id(Id),
        {ok, PayloadTypes} ?= compile_payload_types(read_map(RawModel, [payload_types], #{})),
        {ok, {LiteralChildren, VariableChildren}} ?= compile_children(Tree, VarTypes, PayloadTypes),
        Root = #{
            kind => ?NAMESPACE,
            key => <<"$root">>,
            literal_children => LiteralChildren,
            variable_children => VariableChildren
        },
        {ok, #{
            id => Id,
            name => Name,
            root => Root,
            topic_filters => collect_topic_filters(Root),
            payload_types => PayloadTypes,
            raw => RawModel0
        }}
    end;
compile(_) ->
    {error, #{cause => invalid_model}}.

validate_model_id(undefined) ->
    {error, #{cause => missing_model_id}};
validate_model_id(<<>>) ->
    {error, #{cause => invalid_model_id, value => <<>>}};
validate_model_id(Id) when is_binary(Id) ->
    case re:run(Id, <<"^[A-Za-z0-9_-]+$">>) of
        {match, _} -> ok;
        nomatch -> {error, #{cause => invalid_model_id, value => Id}}
    end;
validate_model_id(Id) ->
    {error, #{cause => invalid_model_id, value => Id}}.

validate_topic(Compiled, Topic) ->
    emqx_unsgov_model_topic:validate_topic(Compiled, Topic).

validate_message(Compiled, Topic, Payload, ValidatePayload) ->
    emqx_unsgov_model_message:validate_message(
        Compiled, Topic, Payload, ValidatePayload
    ).

screen_topic(Compiled, Topic) when is_map(Compiled), is_binary(Topic) ->
    TopicSegments = topic_segments(Topic),
    TopicFilters = maps:get(topic_filters, Compiled, []),
    TopicSegments =/= [] andalso
        lists:any(
            fun(FilterSegments) ->
                match_filter_candidate(FilterSegments, TopicSegments)
            end,
            TopicFilters
        ).

summary(#{id := Id, name := Name, root := Root}) ->
    #{
        id => Id,
        name => Name,
        endpoint_count => count_endpoints(Root),
        node_count => count_nodes(Root)
    }.

compile_var_types(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V0, Acc) ->
            VK = to_bin(K),
            V = normalize_nested_map_keys(V0),
            Acc#{VK => emqx_unsgov_model_schema:compile_var_matcher(V)}
        end,
        #{},
        Map
    );
compile_var_types(_) ->
    #{}.

compile_payload_types(Map) when is_map(Map) ->
    maps:fold(fun fold_compile_payload_type/3, {ok, #{}}, Map);
compile_payload_types(_) ->
    {ok, #{}}.

fold_compile_payload_type(_K, _V, {error, _} = Error) ->
    Error;
fold_compile_payload_type(K, V0, {ok, Acc}) ->
    PayloadType = to_bin(K),
    case PayloadType of
        <<"any">> ->
            {error, #{cause => reserved_payload_type_name, value => PayloadType}};
        _ ->
            fold_compile_payload_type_schema(PayloadType, V0, Acc)
    end.

fold_compile_payload_type_schema(PayloadType, V0, Acc) ->
    case emqx_unsgov_model_schema:normalize_payload_schema(V0) of
        {ok, Schema} ->
            {ok, Acc#{PayloadType => Schema}};
        {error, Reason} ->
            {error, #{
                cause => invalid_payload_schema,
                payload_type => PayloadType,
                reason => Reason
            }}
    end.

compile_children(Children, VarTypes, PayloadTypes) when is_map(Children) ->
    FoldFn = fun(KV, Acc) -> fold_compile_child(KV, Acc, VarTypes, PayloadTypes) end,
    lists:foldl(FoldFn, {ok, {#{}, []}}, maps:to_list(Children));
compile_children(_, _, _) ->
    {error, #{cause => invalid_tree}}.

fold_compile_child(_KV, {error, _} = Error, _VarTypes, _PayloadTypes) ->
    Error;
fold_compile_child({RawKey, RawNode0}, {ok, {LiteralAcc, VarAcc}}, VarTypes, PayloadTypes) ->
    Key = to_bin(RawKey),
    RawNode = normalize_nested_map_keys(RawNode0),
    case compile_node(Key, RawNode, VarTypes, PayloadTypes) of
        {ok, Node = #{kind := ?VARIABLE}} ->
            {ok, {LiteralAcc, [Node | VarAcc]}};
        {ok, Node} ->
            {ok, {LiteralAcc#{Key => Node}, VarAcc}};
        {error, _} = Error ->
            Error
    end.

compile_node(Key, Node, VarTypes, PayloadTypes) ->
    Type = node_type(Key, Node),
    case Type of
        ?NAMESPACE ->
            compile_namespace(Key, Node, VarTypes, PayloadTypes);
        ?VARIABLE ->
            compile_variable(Key, Node, VarTypes, PayloadTypes);
        ?ENDPOINT ->
            compile_endpoint(Key, Node, PayloadTypes);
        _ ->
            {error, #{cause => invalid_node_type, key => Key, value => Type}}
    end.

compile_namespace(Key, Node, VarTypes, PayloadTypes) ->
    maybe
        {ok, Children} ?= require_children(Key, Node),
        {ok, {LiteralChildren, VariableChildren}} ?=
            compile_children(Children, VarTypes, PayloadTypes),
        {ok, #{
            kind => ?NAMESPACE,
            key => Key,
            literal_children => LiteralChildren,
            variable_children => VariableChildren
        }}
    end.

compile_variable(Key, Node, VarTypes, PayloadTypes) ->
    maybe
        {ok, VarName} ?= require_variable_name(Key),
        {ok, {VarTypeName, Matcher}} ?= variable_matcher(Key, VarName, Node, VarTypes),
        {ok, Children} ?= require_children(Key, Node),
        {ok, {LiteralChildren, VariableChildren}} ?=
            compile_children(Children, VarTypes, PayloadTypes),
        {ok, #{
            kind => ?VARIABLE,
            key => Key,
            var_name => VarName,
            var_type => VarTypeName,
            matcher => Matcher,
            literal_children => LiteralChildren,
            variable_children => VariableChildren
        }}
    end.

require_variable_name(<<"+">>) ->
    {ok, <<"+">>};
require_variable_name(Key) ->
    case parse_braced_var(Key) of
        error -> {error, #{cause => invalid_variable_key, key => Key}};
        VarName -> {ok, VarName}
    end.

require_children(Key, Node) ->
    case read_map(Node, [children], undefined) of
        undefined -> {error, #{cause => missing_children, key => Key}};
        Children -> {ok, Children}
    end.

variable_matcher(<<"+">>, _VarName, _Node, _VarTypes) ->
    {ok, {<<"+">>, any}};
variable_matcher(_Key, VarName, Node, VarTypes) ->
    VarTypeName = read_bin(Node, [var_type], VarName),
    case maps:find(VarTypeName, VarTypes) of
        error ->
            {error, #{cause => unknown_variable_type, var_type => VarTypeName}};
        {ok, Matcher} ->
            {ok, {VarTypeName, Matcher}}
    end.

compile_endpoint(Key, Node, PayloadTypes) ->
    case read_map(Node, [children], undefined) of
        Children when is_map(Children), map_size(Children) =:= 0 ->
            compile_endpoint_payload(Key, Node, PayloadTypes);
        undefined ->
            compile_endpoint_payload(Key, Node, PayloadTypes);
        _ ->
            {error, #{cause => endpoint_with_children, key => Key}}
    end.

compile_endpoint_payload(Key, Node, PayloadTypes) ->
    PayloadName = read_bin(Node, [payload], <<"any">>),
    case PayloadName of
        <<"any">> ->
            {ok, #{
                kind => ?ENDPOINT, key => Key, payload => <<"any">>, payload_schema => #{}
            }};
        _ ->
            case maps:find(PayloadName, PayloadTypes) of
                {ok, PayloadSchema} ->
                    {ok, #{
                        kind => ?ENDPOINT,
                        key => Key,
                        payload => PayloadName,
                        payload_schema => PayloadSchema
                    }};
                error ->
                    {error, #{cause => unknown_payload_type, payload_type => PayloadName}}
            end
    end.

node_type(Key, Node) ->
    case read_any(Node, [type], undefined) of
        <<"namespace">> -> ?NAMESPACE;
        <<"variable">> -> ?VARIABLE;
        <<"terminal">> -> ?ENDPOINT;
        <<"endpoint">> -> ?ENDPOINT;
        _ -> infer_node_type(Key, Node)
    end.

infer_node_type(Key, Node) ->
    case read_map(Node, [children], undefined) of
        Children when is_map(Children) ->
            infer_node_type_by_key(Key);
        _ ->
            ?ENDPOINT
    end.

infer_node_type_by_key(<<"+">>) ->
    ?VARIABLE;
infer_node_type_by_key(Key) ->
    case parse_braced_var(Key) of
        error -> ?NAMESPACE;
        _ -> ?VARIABLE
    end.

normalize_map_keys(Map) when is_map(Map) ->
    maps:fold(
        fun
            (<<"id">>, V, Acc) -> Acc#{id => V};
            (<<"name">>, V, Acc) -> Acc#{name => V};
            (<<"tree">>, V, Acc) -> Acc#{tree => V};
            (<<"variable_types">>, V, Acc) -> Acc#{variable_types => V};
            (<<"payload_types">>, V, Acc) -> Acc#{payload_types => V};
            (K, V, Acc) -> Acc#{K => V}
        end,
        #{},
        Map
    );
normalize_map_keys(V) ->
    V.

normalize_nested_map_keys(Map) when is_map(Map) ->
    maps:fold(
        fun
            (<<"_type">>, V, Acc) -> Acc#{type => V};
            (<<"_var_type">>, V, Acc) -> Acc#{var_type => V};
            (<<"_payload">>, V, Acc) -> Acc#{payload => V};
            (<<"children">>, V, Acc) -> Acc#{children => V};
            (<<"type">>, V, Acc) -> Acc#{type => V};
            (<<"pattern">>, V, Acc) -> Acc#{pattern => V};
            (<<"values">>, V, Acc) -> Acc#{values => V};
            (<<"required">>, V, Acc) -> Acc#{required => V};
            (<<"properties">>, V, Acc) -> Acc#{properties => V};
            (<<"additionalProperties">>, V, Acc) -> Acc#{additional_properties => V};
            (<<"enum">>, V, Acc) -> Acc#{enum => V};
            (K, V, Acc) when is_binary(K) -> Acc#{K => V}
        end,
        #{},
        Map
    );
normalize_nested_map_keys(V) ->
    V.

parse_braced_var(<<"{", Rest/binary>>) ->
    case binary:split(Rest, <<"}">>, [global]) of
        [VarName, <<>>] when VarName =/= <<>> -> VarName;
        _ -> error
    end;
parse_braced_var(_) ->
    error.

count_endpoints(#{kind := ?ENDPOINT}) ->
    1;
count_endpoints(Node) ->
    Literal = maps:values(maps:get(literal_children, Node, #{})),
    Vars = maps:get(variable_children, Node, []),
    lists:sum([count_endpoints(N) || N <- Literal ++ Vars]).

count_nodes(Node) ->
    Literal = maps:values(maps:get(literal_children, Node, #{})),
    Vars = maps:get(variable_children, Node, []),
    1 + lists:sum([count_nodes(N) || N <- Literal ++ Vars]).

collect_topic_filters(Root) ->
    Paths = collect_filters_from_children(Root, []),
    ordsets:to_list(ordsets:from_list(Paths)).

collect_filters_from_children(Node, Prefix) ->
    Literal = maps:values(maps:get(literal_children, Node, #{})),
    Vars = maps:get(variable_children, Node, []),
    ChildNodes = Literal ++ Vars,
    lists:append([collect_filters_for_node(Child, Prefix) || Child <- ChildNodes]).

collect_filters_for_node(Node = #{kind := Kind}, Prefix) ->
    Segment =
        case Kind of
            ?VARIABLE -> <<"+">>;
            _ -> maps:get(key, Node)
        end,
    Path = Prefix ++ [Segment],
    case has_compiled_children(Node) of
        true ->
            collect_filters_from_children(Node, Path);
        false ->
            [Path]
    end.

has_compiled_children(Node) ->
    LiteralChildren = maps:get(literal_children, Node, #{}),
    VariableChildren = maps:get(variable_children, Node, []),
    (map_size(LiteralChildren) > 0) orelse (length(VariableChildren) > 0).

topic_segments(Topic) ->
    [S || S <- binary:split(Topic, <<"/">>, [global]), S =/= <<>>].

%% Pre-check candidate match:
%% - exact filter/topic match passes
%% - topic being a valid prefix of a filter also passes
%% - '#' filter segment matches remaining topic
match_filter_candidate([<<"#">> | _], _Topic) ->
    true;
match_filter_candidate([], []) ->
    true;
match_filter_candidate([], [_ | _]) ->
    false;
match_filter_candidate([_ | _], []) ->
    true;
match_filter_candidate([FilterSeg | FilterRest], [TopicSeg | TopicRest]) ->
    case FilterSeg of
        <<"+">> ->
            match_filter_candidate(FilterRest, TopicRest);
        _ when FilterSeg =:= TopicSeg ->
            match_filter_candidate(FilterRest, TopicRest);
        _ ->
            false
    end.

read_map(Map, [K | Ks], Default) ->
    case maps:find(K, Map) of
        {ok, V} when is_map(V) -> V;
        {ok, _} -> Default;
        error -> read_map(Map, Ks, Default)
    end;
read_map(_Map, [], Default) ->
    Default.

read_any(Map, [K | Ks], Default) ->
    case maps:find(K, Map) of
        {ok, V} -> V;
        error -> read_any(Map, Ks, Default)
    end;
read_any(_Map, [], Default) ->
    Default.

read_bin(Map, Keys, Default) ->
    case read_any(Map, Keys, Default) of
        V when is_binary(V) -> V;
        _ -> Default
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(V) -> iolist_to_binary(io_lib:format("~p", [V])).
