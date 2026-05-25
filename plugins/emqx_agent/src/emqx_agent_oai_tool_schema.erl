%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_oai_tool_schema).

-moduledoc """
Validator for the OpenAI-compatible tool schema subset used by EMQX Agent.

The agent accepts tool schemas from external configuration and later uses them
to decide whether model-produced tool arguments are safe to execute.  Full JSON
Schema is intentionally not supported: many JSON Schema features are not
accepted by OpenAI strict function tools, and accepting keywords that we do not
enforce locally would create a false sense of safety.

This module therefore validates a closed, deterministic subset
that can both be sent to an OpenAI-compatible provider and
enforced by the local value validator.
""".

-export([
    json_schema_from_string/2,
    validate_schema/1,
    validate_schema/2,
    validate_oai_schema/1,
    validate_oai_schema_field/1
]).

-define(DEFAULT_MAX_DEPTH, 10).
-define(DEFAULT_MAX_PROPERTIES, 5000).

-type mode() :: root | field.
-type path_segment() :: binary() | non_neg_integer().
-type validation_error() :: #{
    error := atom(),
    path := [path_segment()],
    _ => _
}.
-type validation_result() :: ok | {error, [validation_error()]}.

-spec json_schema_from_string(undefined | binary() | map(), term()) -> undefined | map().
json_schema_from_string(undefined, _Opts) ->
    undefined;
json_schema_from_string(Str, _Opts) when is_binary(Str) ->
    case emqx_utils_json:safe_decode(Str) of
        {ok, Map} when is_map(Map) ->
            Map;
        {ok, _} ->
            throw({invalid_json_schema, "JSON schema must decode to an object"});
        {error, Reason} ->
            throw({invalid_json, Reason})
    end;
json_schema_from_string(Map, _Opts) when is_map(Map) ->
    Map.

-spec validate_oai_schema(undefined | map()) -> ok | {error, binary()}.
validate_oai_schema(undefined) ->
    ok;
validate_oai_schema(Schema) when is_map(Schema) ->
    case validate_schema(Schema, root) of
        ok -> ok;
        {error, Errors} -> {error, format_schema_errors(Errors)}
    end.

-spec validate_oai_schema_field(undefined | map()) -> ok | {error, binary()}.
validate_oai_schema_field(undefined) ->
    ok;
validate_oai_schema_field(Schema) when is_map(Schema) ->
    case validate_schema(Schema, field) of
        ok -> ok;
        {error, Errors} -> {error, format_schema_errors(Errors)}
    end.

-doc """
Validates a root schema suitable for a complete function `parameters` schema.

This is equivalent to `validate_schema(Schema, root)`.  The root schema must be
a non-nullable object schema with `properties`, `required`, and
`additionalProperties: false`; root-level `anyOf` is rejected.
""".
-spec validate_schema(map()) -> validation_result().

validate_schema(Schema) ->
    validate_schema(Schema, root).

-doc """
Validates a schema in either `root` or `field` position.

`root` is for complete tool `parameters` or local response schemas.  `field` is
for nested schemas under object properties, array `items`, or `anyOf` branches.
Field schemas may use nullable type arrays and nested `anyOf`; root schemas may
not.
""".
-spec validate_schema(map(), mode()) -> validation_result().

validate_schema(Schema, Mode) when Mode =:= root; Mode =:= field ->
    Errors0 = validate_node(Schema, Mode, [], 1),
    Errors1 = property_limit_errors(Schema),
    case Errors0 ++ Errors1 of
        [] -> ok;
        _ -> {error, lists:reverse(Errors0 ++ Errors1)}
    end.

validate_node(Schema, _Mode, Path, _Depth) when not is_map(Schema) ->
    [err(schema_not_object, Path, #{})];
validate_node(_Schema, _Mode, Path, Depth) when Depth > ?DEFAULT_MAX_DEPTH ->
    [err(max_depth_exceeded, Path, #{max_depth => ?DEFAULT_MAX_DEPTH})];
validate_node(Schema, root, Path, Depth) ->
    Errors0 =
        case maps:is_key(<<"anyOf">>, Schema) of
            true -> [err(root_any_of_not_allowed, Path ++ [<<"anyOf">>], #{})];
            false -> []
        end,
    validate_typed_schema(Schema, root, Path, Depth, Errors0);
validate_node(Schema, field, Path, Depth) ->
    case maps:is_key(<<"anyOf">>, Schema) of
        true -> validate_anyof_schema(Schema, Path, Depth, []);
        false -> validate_typed_schema(Schema, field, Path, Depth, [])
    end.

validate_anyof_schema(Schema, Path, Depth, Errors0) ->
    Errors1 = reject_unknown_keys(Schema, [<<"anyOf">>, <<"description">>], Path, Errors0),
    Errors2 =
        case maps:is_key(<<"type">>, Schema) of
            true -> [err(type_with_any_of, Path ++ [<<"type">>], #{}) | Errors1];
            false -> Errors1
        end,
    case maps:get(<<"anyOf">>, Schema, undefined) of
        AnyOf when is_list(AnyOf), AnyOf =/= [] ->
            {NestedErrors, _} = lists:foldl(
                fun(SubSchema, {Acc, I}) ->
                    {
                        validate_node(SubSchema, field, Path ++ [<<"anyOf">>, I], Depth + 1) ++ Acc,
                        I + 1
                    }
                end,
                {Errors2, 0},
                AnyOf
            ),
            NestedErrors;
        AnyOf when is_list(AnyOf) ->
            [err(empty_any_of, Path ++ [<<"anyOf">>], #{}) | Errors2];
        _ ->
            [err(any_of_not_array, Path ++ [<<"anyOf">>], #{}) | Errors2]
    end.

validate_typed_schema(Schema, Mode, Path, Depth, Errors0) ->
    case maps:get(<<"type">>, Schema, undefined) of
        undefined ->
            [err(missing_type, Path ++ [<<"type">>], #{}) | Errors0];
        TypeSpec ->
            case parse_type(TypeSpec, Mode) of
                {ok, Type, Nullable} ->
                    validate_by_type(Schema, Type, Nullable, Mode, Path, Depth, Errors0);
                {error, Reason} ->
                    [err(Reason, Path ++ [<<"type">>], #{}) | Errors0]
            end
    end.

validate_by_type(Schema, <<"object">>, Nullable, Mode, Path, Depth, Errors0) ->
    Errors1 =
        case {Mode, Nullable} of
            {root, true} -> [err(root_nullable_not_allowed, Path ++ [<<"type">>], #{}) | Errors0];
            _ -> Errors0
        end,
    Errors2 = reject_unknown_keys(
        Schema,
        [
            <<"type">>,
            <<"description">>,
            <<"properties">>,
            <<"required">>,
            <<"additionalProperties">>
        ],
        Path,
        Errors1
    ),
    validate_object_schema(Schema, Path, Depth, Errors2);
validate_by_type(Schema, <<"array">>, _Nullable, _Mode, Path, Depth, Errors0) ->
    Errors1 = reject_unknown_keys(
        Schema,
        [<<"type">>, <<"description">>, <<"items">>],
        Path,
        Errors0
    ),
    case maps:get(<<"items">>, Schema, undefined) of
        undefined ->
            [err(missing_items, Path ++ [<<"items">>], #{}) | Errors1];
        Items ->
            validate_node(Items, field, Path ++ [<<"items">>], Depth + 1) ++ Errors1
    end;
validate_by_type(Schema, Type, Nullable, _Mode, Path, _Depth, Errors0) ->
    Errors1 = reject_unknown_keys(
        Schema,
        [<<"type">>, <<"description">>, <<"enum">>],
        Path,
        Errors0
    ),
    validate_enum(Schema, Type, Nullable, Path, Errors1).

validate_object_schema(Schema, Path, Depth, Errors0) ->
    validate_properties_is_object(Schema, Path) ++
        validate_required(Schema, Path) ++
        validate_additional_properties(Schema, Path) ++
        validate_required_matches_properties(Schema, Path) ++
        validate_subschemas(Schema, Path, Depth, Errors0).

validate_subschemas(#{<<"properties">> := Properties} = _Schema, Path, Depth, Errors) when
    is_map(Properties)
->
    maps:fold(
        fun(Key, SubSchema, ErrorsAcc) ->
            validate_node(SubSchema, field, Path ++ [<<"properties">>, Key], Depth + 1) ++ ErrorsAcc
        end,
        Errors,
        Properties
    );
validate_subschemas(_Schema, _Path, _Depth, Errors) ->
    Errors.

validate_additional_properties(Schema, Path) ->
    case Schema of
        #{<<"additionalProperties">> := AP} when AP =:= false -> [];
        #{<<"additionalProperties">> := _} ->
            [err(additional_properties_not_false, Path ++ [<<"additionalProperties">>], #{})];
        _ ->
            [err(missing_additional_properties, Path ++ [<<"additionalProperties">>], #{})]
    end.

validate_properties_is_object(Schema, Path) ->
    case Schema of
        #{<<"properties">> := P} when is_map(P) -> [];
        #{<<"properties">> := _} -> [err(properties_not_object, Path ++ [<<"properties">>], #{})];
        _ -> [err(missing_properties, Path ++ [<<"properties">>], #{})]
    end.

validate_required(Schema, Path) ->
    case Schema of
        #{<<"required">> := R} when is_list(R) -> validate_required_list(R, Path);
        #{<<"required">> := _} -> [err(required_not_array, Path ++ [<<"required">>], #{})];
        _ -> [err(missing_required, Path ++ [<<"required">>], #{})]
    end.

validate_required_list(Required, Path) ->
    {Errors, _} = lists:foldl(
        fun
            (Name, {Acc, I}) when is_binary(Name) -> {Acc, I + 1};
            (_Name, {Acc, I}) -> {[err(required_item_not_string, Path ++ [I], #{}) | Acc], I + 1}
        end,
        {[], 0},
        Required
    ),
    Errors.

validate_required_matches_properties(
    #{<<"properties">> := Properties, <<"required">> := Required}, Path
) when is_map(Properties), is_list(Required) ->
    case lists:all(fun is_binary/1, Required) of
        true ->
            PropKeys = lists:sort(maps:keys(Properties)),
            ReqKeys = lists:sort(Required),
            case PropKeys =:= ReqKeys of
                true ->
                    [];
                false ->
                    [
                        err(required_mismatch, Path ++ [<<"required">>], #{
                            properties => PropKeys, required => ReqKeys
                        })
                    ]
            end;
        false ->
            %% `validate_required_list` returns an error for non-string items,
            %% so we avoid emitting additional errors here.
            []
    end;
validate_required_matches_properties(_Schema, _Path) ->
    [].

validate_enum(Schema, Type, Nullable, Path, Errors0) ->
    case maps:get(<<"enum">>, Schema, undefined) of
        undefined ->
            Errors0;
        Enum when is_list(Enum) ->
            {Errors, _} = lists:foldl(
                fun(Value, {Acc, I}) ->
                    case valid_enum_value(Value, Type, Nullable) of
                        true ->
                            {Acc, I + 1};
                        false ->
                            {[err(invalid_enum_value, Path ++ [<<"enum">>, I], #{}) | Acc], I + 1}
                    end
                end,
                {Errors0, 0},
                Enum
            ),
            Errors;
        _ ->
            [err(enum_not_array, Path ++ [<<"enum">>], #{}) | Errors0]
    end.

valid_enum_value(null, _Type, true) -> true;
valid_enum_value(Value, <<"string">>, _Nullable) -> is_binary(Value);
valid_enum_value(Value, <<"integer">>, _Nullable) -> is_integer(Value);
valid_enum_value(Value, <<"number">>, _Nullable) -> is_integer(Value) orelse is_float(Value);
valid_enum_value(Value, <<"boolean">>, _Nullable) -> Value =:= true orelse Value =:= false;
valid_enum_value(_Value, _Type, _Nullable) -> false.

parse_type(Type, root) ->
    case Type of
        <<"object">> -> {ok, <<"object">>, false};
        [_ | _] -> {error, root_nullable_not_allowed};
        _ -> {error, root_type_not_object}
    end;
parse_type(Type, field) when is_binary(Type) ->
    case lists:member(Type, allowed_types()) of
        true -> {ok, Type, false};
        false -> {error, unsupported_type}
    end;
parse_type(Types, field) when is_list(Types) ->
    Unique = lists:usort(Types),
    NonNull = [T || T <- Unique, T =/= <<"null">>],
    case {lists:member(<<"null">>, Unique), length(Unique), NonNull} of
        {true, 2, [Type]} ->
            case lists:member(Type, allowed_types()) of
                true -> {ok, Type, true};
                false -> {error, unsupported_type}
            end;
        {false, _, _} ->
            {error, type_array_without_null};
        _ ->
            {error, invalid_nullable_type}
    end;
parse_type(_Type, field) ->
    {error, invalid_type}.

allowed_types() ->
    [<<"string">>, <<"integer">>, <<"number">>, <<"boolean">>, <<"array">>, <<"object">>].

reject_unknown_keys(Schema, Allowed, Path, Errors) ->
    maps:fold(
        fun(Key, _Value, Acc) ->
            case lists:member(Key, Allowed) of
                true -> Acc;
                false -> [err(unsupported_keyword, Path ++ [Key], #{keyword => Key}) | Acc]
            end
        end,
        Errors,
        Schema
    ).

property_limit_errors(Schema) ->
    Count = count_properties(Schema),
    case Count > ?DEFAULT_MAX_PROPERTIES of
        true -> [err(max_properties_exceeded, [], #{max_properties => ?DEFAULT_MAX_PROPERTIES})];
        false -> []
    end.

count_properties(Schema) when is_map(Schema) ->
    OwnAndNestedCount =
        case Schema of
            #{<<"properties">> := Properties} when is_map(Properties) ->
                OwnCount = maps:size(Properties),
                NestedSchemas = maps:values(Properties),
                NestedCount = lists:sum([count_properties(S) || S <- NestedSchemas]),
                OwnCount + NestedCount;
            _ ->
                0
        end,
    ItemsCount = count_properties(maps:get(<<"items">>, Schema, [])),
    AnyOfCount = lists:sum([count_properties(S) || S <- anyof_list(Schema)]),
    OwnAndNestedCount + ItemsCount + AnyOfCount;
count_properties(_Schema) ->
    0.

anyof_list(#{<<"anyOf">> := L}) when is_list(L) -> L;
anyof_list(_) -> [].

err(Code, Path, Extra) ->
    maps:merge(#{error => Code, path => Path}, Extra).

format_schema_errors(Errors) ->
    iolist_to_binary([
        "Invalid JSON Schema: ",
        lists:join("; ", [format_schema_error(E) || E <- Errors])
    ]).

format_schema_error(#{error := Code, path := Path} = E) ->
    Extra = maps:without([error, path], E),
    Base = io_lib:format("~s at ~s", [Code, path_to_binary(Path)]),
    case maps:size(Extra) of
        0 -> Base;
        _ -> [Base, io_lib:format(" (~p)", [Extra])]
    end.

path_to_binary(Path) ->
    iolist_to_binary(lists:join("/", [to_binary(P) || P <- Path])).

to_binary(B) when is_binary(B) -> B;
to_binary(I) when is_integer(I) -> integer_to_binary(I).
