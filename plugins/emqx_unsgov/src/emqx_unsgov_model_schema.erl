%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_model_schema).

-moduledoc """
Compiles variable matchers and validates endpoint payloads via JSON Schema.
""".

-export([compile_var_matcher/1, match_segment/2, normalize_payload_schema/1, validate_payload/2]).

compile_var_matcher(#{type := <<"enum">>, values := Values}) when is_list(Values) ->
    {enum, maps:from_list([{to_bin(V), true} || V <- Values])};
compile_var_matcher(#{type := <<"string">>, pattern := Pattern}) ->
    compile_regex_matcher(Pattern);
compile_var_matcher(_) ->
    any.

match_segment(any, _Segment) ->
    true;
match_segment({enum, ValuesSet}, Segment) ->
    maps:is_key(Segment, ValuesSet);
match_segment({regex, RE}, Segment) ->
    case re:run(Segment, RE, [{capture, none}]) of
        match -> true;
        _ -> false
    end.

validate_payload(Node, PayloadBin) ->
    case maps:get(payload, Node, <<"any">>) of
        <<"any">> ->
            ok;
        _ ->
            do_validate_payload(Node, PayloadBin)
    end.

do_validate_payload(Node, PayloadBin) ->
    Schema = maps:get(payload_schema, Node, #{}),
    try emqx_utils_json:decode(PayloadBin, [return_maps]) of
        PayloadMap when is_map(PayloadMap) ->
            case jesse:validate_with_schema(Schema, PayloadMap, []) of
                {ok, _} -> ok;
                _ -> {error, payload_invalid}
            end;
        _ ->
            {error, payload_invalid}
    catch
        _:_ ->
            {error, payload_invalid}
    end.

normalize_payload_schema(Schema0) when is_map(Schema0) ->
    Schema1 = normalize_schema_json(Schema0),
    case maps:get(<<"type">>, Schema1, undefined) of
        undefined ->
            {ok, Schema1#{<<"type">> => <<"object">>}};
        <<"object">> ->
            {ok, Schema1};
        object ->
            {ok, Schema1#{<<"type">> => <<"object">>}};
        Other ->
            {error, #{cause => payload_schema_not_object, type => Other}}
    end;
normalize_payload_schema(_) ->
    {error, #{cause => invalid_payload_schema}}.

normalize_schema_json(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V, Acc) ->
            Acc#{normalize_schema_key(K) => normalize_schema_value(V)}
        end,
        #{},
        Map
    );
normalize_schema_json(V) ->
    V.

normalize_schema_key(K) when is_binary(K) ->
    K;
normalize_schema_key(K) when is_atom(K) ->
    atom_to_binary(K, utf8);
normalize_schema_key(K) ->
    to_bin(K).

normalize_schema_value(Map) when is_map(Map) ->
    normalize_schema_json(Map);
normalize_schema_value(List) when is_list(List) ->
    [normalize_schema_value(V) || V <- List];
normalize_schema_value(V) ->
    V.

compile_regex_matcher(Pattern0) ->
    Pattern = to_bin(Pattern0),
    case re:compile(Pattern) of
        {ok, RE} -> {regex, RE};
        _ -> any
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(V) -> iolist_to_binary(io_lib:format("~p", [V])).
