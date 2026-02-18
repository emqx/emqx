%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate_model).

-export([
    compile/1,
    validate_topic/3,
    validate_message/5,
    summary/1
]).

-define(NAMESPACE, namespace).
-define(VARIABLE, variable).
-define(ENDPOINT, endpoint).

compile(RawModel0) when is_map(RawModel0) ->
    RawModel = normalize_map_keys(RawModel0),
    Id = read_bin(RawModel, [id], default_model_id()),
    Name = read_bin(RawModel, [name], Id),
    Tree = read_map(RawModel, [tree], #{}),
    VarTypes = compile_var_types(read_map(RawModel, [variable_types], #{})),
    PayloadTypes = compile_payload_types(read_map(RawModel, [payload_types], #{})),
    case compile_children(Tree, VarTypes, PayloadTypes) of
        {ok, {LiteralChildren, VariableChildren}} ->
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
                payload_types => PayloadTypes,
                raw => RawModel0
            }};
        {error, _} = Error ->
            Error
    end;
compile(_) ->
    {error, invalid_model}.

validate_topic(Compiled, Topic, AllowIntermediate) when is_map(Compiled), is_binary(Topic) ->
    case walk_to_node(Compiled, Topic) of
        {ok, Node} ->
            case maps:get(kind, Node) of
                ?ENDPOINT ->
                    allow;
                _ when AllowIntermediate =:= true ->
                    allow;
                _ ->
                    {deny, not_endpoint}
            end;
        {error, Reason} ->
            {deny, Reason}
    end.

validate_message(Compiled, Topic, Payload, AllowIntermediate, ValidatePayload) ->
    case walk_to_node(Compiled, Topic) of
        {ok, Node} ->
            case maps:get(kind, Node) of
                ?ENDPOINT ->
                    case maybe_validate_payload(Node, Payload, ValidatePayload) of
                        ok -> allow;
                        {error, Reason} -> {deny, Reason}
                    end;
                _ when AllowIntermediate =:= true ->
                    allow;
                _ ->
                    {deny, not_endpoint}
            end;
        {error, Reason} ->
            {deny, Reason}
    end.

summary(#{id := Id, name := Name, root := Root}) ->
    #{
        id => Id,
        name => Name,
        endpoint_count => count_endpoints(Root),
        node_count => count_nodes(Root)
    }.

walk_to_node(Compiled, Topic) ->
    Segments = [S || S <- binary:split(Topic, <<"/">>, [global]), S =/= <<>>],
    case Segments of
        [] ->
            {error, topic_invalid};
        _ ->
            walk(maps:get(root, Compiled), Segments)
    end.

maybe_validate_payload(_Node, _Payload, false) ->
    ok;
maybe_validate_payload(Node, Payload, true) ->
    case maps:get(payload, Node, <<"any">>) of
        <<"any">> ->
            ok;
        _ ->
            do_validate_payload(Node, Payload)
    end.

do_validate_payload(Node, PayloadBin) ->
    Schema = maps:get(payload_schema, Node, #{}),
    try emqx_utils_json:decode(PayloadBin, [return_maps]) of
        PayloadMap ->
            case validate_schema(PayloadMap, Schema) of
                ok -> ok;
                {error, _} -> {error, payload_invalid}
            end
    catch
        _:_ ->
            {error, payload_invalid}
    end.

validate_schema(Value, Schema) when is_map(Schema) ->
    Type = read_any(Schema, [type], undefined),
    case Type of
        <<"object">> ->
            validate_object_schema(Value, Schema);
        object ->
            validate_object_schema(Value, Schema);
        <<"string">> ->
            validate_string_schema(Value, Schema);
        string ->
            validate_string_schema(Value, Schema);
        <<"integer">> ->
            validate_integer_schema(Value, Schema);
        integer ->
            validate_integer_schema(Value, Schema);
        <<"number">> ->
            validate_number_schema(Value, Schema);
        number ->
            validate_number_schema(Value, Schema);
        <<"boolean">> ->
            validate_boolean_schema(Value, Schema);
        boolean ->
            validate_boolean_schema(Value, Schema);
        _ ->
            ok
    end.

validate_object_schema(Value, Schema) when is_map(Value) ->
    Required0 = read_list(Schema, [required], []),
    Required = [to_bin(K) || K <- Required0],
    case lists:all(fun(K) -> maps:is_key(K, Value) end, Required) of
        false ->
            {error, missing_required};
        true ->
            Properties0 = read_map(Schema, [properties], #{}),
            Properties = normalize_nested_map_keys(Properties0),
            case validate_object_properties(Value, Properties) of
                ok ->
                    Additional = read_any(Schema, [additionalProperties], true),
                    case Additional of
                        false ->
                            Keys = maps:keys(Value),
                            Allowed = maps:keys(Properties),
                            case lists:all(fun(K) -> lists:member(K, Allowed) end, Keys) of
                                true -> ok;
                                false -> {error, additional_properties}
                            end;
                        _ ->
                            ok
                    end;
                {error, _} = Error ->
                    Error
            end
    end;
validate_object_schema(_Value, _Schema) ->
    {error, expected_object}.

validate_object_properties(Value, Properties) ->
    maps:fold(
        fun
            (Key, PropSchema0, ok) ->
                case maps:find(Key, Value) of
                    error ->
                        ok;
                    {ok, PropValue} ->
                        PropSchema = normalize_nested_map_keys(PropSchema0),
                        validate_schema(PropValue, PropSchema)
                end;
            (_Key, _PropSchema, Error) ->
                Error
        end,
        ok,
        Properties
    ).

validate_string_schema(Value, Schema) when is_binary(Value) ->
    case read_list(Schema, [enum], undefined) of
        undefined ->
            ok;
        EnumValues ->
            Enum = [to_bin(V) || V <- EnumValues],
            case lists:member(Value, Enum) of
                true -> ok;
                false -> {error, enum_mismatch}
            end
    end;
validate_string_schema(_Value, _Schema) ->
    {error, expected_string}.

validate_integer_schema(Value, _Schema) when is_integer(Value) ->
    ok;
validate_integer_schema(_Value, _Schema) ->
    {error, expected_integer}.

validate_number_schema(Value, _Schema) when is_number(Value) ->
    ok;
validate_number_schema(_Value, _Schema) ->
    {error, expected_number}.

validate_boolean_schema(true, _Schema) ->
    ok;
validate_boolean_schema(false, _Schema) ->
    ok;
validate_boolean_schema(_, _) ->
    {error, expected_boolean}.

compile_var_types(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V0, Acc) ->
            VK = to_bin(K),
            V = normalize_nested_map_keys(V0),
            Acc#{VK => compile_var_matcher(V)}
        end,
        #{},
        Map
    );
compile_var_types(_) ->
    #{}.

compile_payload_types(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V0, Acc) ->
            Acc#{to_bin(K) => normalize_nested_map_keys(V0)}
        end,
        #{},
        Map
    );
compile_payload_types(_) ->
    #{}.

compile_var_matcher(#{type := <<"enum">>, values := Values}) when is_list(Values) ->
    {enum, maps:from_list([{to_bin(V), true} || V <- Values])};
compile_var_matcher(#{type := enum, values := Values}) when is_list(Values) ->
    {enum, maps:from_list([{to_bin(V), true} || V <- Values])};
compile_var_matcher(#{type := <<"string">>, pattern := Pattern}) ->
    compile_regex_matcher(Pattern);
compile_var_matcher(#{type := string, pattern := Pattern}) ->
    compile_regex_matcher(Pattern);
compile_var_matcher(_) ->
    any.

compile_regex_matcher(Pattern0) ->
    Pattern = to_bin(Pattern0),
    case re:compile(Pattern) of
        {ok, RE} -> {regex, RE};
        _ -> any
    end.

compile_children(Children, VarTypes, PayloadTypes) when is_map(Children) ->
    lists:foldl(
        fun
            ({RawKey, RawNode0}, {ok, {LiteralAcc, VarAcc}}) ->
                Key = to_bin(RawKey),
                RawNode = normalize_nested_map_keys(RawNode0),
                case compile_node(Key, RawNode, VarTypes, PayloadTypes) of
                    {ok, Node = #{kind := ?VARIABLE}} ->
                        {ok, {LiteralAcc, [Node | VarAcc]}};
                    {ok, Node} ->
                        {ok, {LiteralAcc#{Key => Node}, VarAcc}};
                    {error, _} = Error ->
                        Error
                end;
            (_KV, Error) ->
                Error
        end,
        {ok, {#{}, []}},
        maps:to_list(Children)
    );
compile_children(_, _, _) ->
    {error, invalid_tree}.

compile_node(Key, Node, VarTypes, PayloadTypes) ->
    Type = node_type(Node),
    case Type of
        ?NAMESPACE ->
            compile_namespace(Key, Node, VarTypes, PayloadTypes);
        ?VARIABLE ->
            compile_variable(Key, Node, VarTypes, PayloadTypes);
        ?ENDPOINT ->
            compile_endpoint(Key, Node, PayloadTypes);
        _ ->
            {error, {invalid_node_type, Key}}
    end.

compile_namespace(Key, Node, VarTypes, PayloadTypes) ->
    case read_map(Node, [children], undefined) of
        undefined ->
            {error, {missing_children, Key}};
        Children ->
            case compile_children(Children, VarTypes, PayloadTypes) of
                {ok, {LiteralChildren, VariableChildren}} ->
                    {ok, #{
                        kind => ?NAMESPACE,
                        key => Key,
                        literal_children => LiteralChildren,
                        variable_children => VariableChildren
                    }};
                {error, _} = Error ->
                    Error
            end
    end.

compile_variable(Key, Node, VarTypes, PayloadTypes) ->
    case parse_braced_var(Key) of
        error ->
            {error, {invalid_variable_key, Key}};
        VarName ->
            VarTypeName = read_bin(Node, [var_type], <<>>),
            case maps:find(VarTypeName, VarTypes) of
                error ->
                    {error, {unknown_variable_type, VarTypeName}};
                {ok, Matcher} ->
                    case read_map(Node, [children], undefined) of
                        undefined ->
                            {error, {missing_children, Key}};
                        Children ->
                            case compile_children(Children, VarTypes, PayloadTypes) of
                                {ok, {LiteralChildren, VariableChildren}} ->
                                    {ok, #{
                                        kind => ?VARIABLE,
                                        key => Key,
                                        var_name => VarName,
                                        var_type => VarTypeName,
                                        matcher => Matcher,
                                        literal_children => LiteralChildren,
                                        variable_children => VariableChildren
                                    }};
                                {error, _} = Error ->
                                    Error
                            end
                    end
            end
    end.

compile_endpoint(Key, Node, PayloadTypes) ->
    case read_map(Node, [children], undefined) of
        undefined ->
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
                            {error, {unknown_payload_type, PayloadName}}
                    end
            end;
        _ ->
            {error, {endpoint_with_children, Key}}
    end.

walk(Node, []) ->
    {ok, Node};
walk(#{kind := ?ENDPOINT}, [_ | _]) ->
    {error, topic_invalid};
walk(Node, [Segment | Rest]) ->
    LiteralChildren = maps:get(literal_children, Node, #{}),
    case maps:find(Segment, LiteralChildren) of
        {ok, Next} ->
            walk(Next, Rest);
        error ->
            VariableChildren = maps:get(variable_children, Node, []),
            case match_variable(VariableChildren, Segment) of
                {ok, Next2} -> walk(Next2, Rest);
                error -> {error, topic_invalid}
            end
    end.

match_variable([], _Segment) ->
    error;
match_variable([Node | Rest], Segment) ->
    case variable_match(maps:get(matcher, Node, any), Segment) of
        true -> {ok, Node};
        false -> match_variable(Rest, Segment)
    end.

variable_match(any, _Segment) ->
    true;
variable_match({enum, ValuesSet}, Segment) ->
    maps:is_key(Segment, ValuesSet);
variable_match({regex, RE}, Segment) ->
    case re:run(Segment, RE, [{capture, none}]) of
        match -> true;
        _ -> false
    end.

node_type(Node) ->
    case read_any(Node, [type], undefined) of
        <<"namespace">> -> ?NAMESPACE;
        namespace -> ?NAMESPACE;
        <<"variable">> -> ?VARIABLE;
        variable -> ?VARIABLE;
        <<"endpoint">> -> ?ENDPOINT;
        endpoint -> ?ENDPOINT;
        _ -> undefined
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
            (<<"additionalProperties">>, V, Acc) -> Acc#{additionalProperties => V};
            (<<"enum">>, V, Acc) -> Acc#{enum => V};
            (K, V, Acc) -> Acc#{to_bin(K) => V}
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

read_list(Map, [K | Ks], Default) ->
    case maps:find(K, Map) of
        {ok, V} when is_list(V) -> V;
        {ok, _} -> Default;
        error -> read_list(Map, Ks, Default)
    end;
read_list(_Map, [], Default) ->
    Default.

read_bin(Map, Keys, Default) ->
    case read_any(Map, Keys, Default) of
        V when is_binary(V) -> V;
        V when is_atom(V) -> atom_to_binary(V, utf8);
        V when is_list(V) -> unicode:characters_to_binary(V);
        _ -> Default
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_list(V) -> unicode:characters_to_binary(V);
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(V) -> iolist_to_binary(io_lib:format("~p", [V])).

default_model_id() ->
    integer_to_binary(erlang:system_time(millisecond)).
