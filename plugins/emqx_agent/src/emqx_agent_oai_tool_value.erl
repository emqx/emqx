%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_oai_tool_value).

-moduledoc """
Runtime validator for decoded tool-call argument and tool-result values.

Schemas accepted by `emqx_agent_oai_tool_schema` describe the only values that
may be passed to tool handlers or returned to the model.  Model-produced tool
arguments are untrusted input, so this module validates decoded JSON terms
against the already-validated schema before any side-effecting tool code runs.

The implementation intentionally follows the same limited OpenAI-compatible
subset as the schema validator instead of attempting full JSON Schema runtime
semantics.
""".

-export([
    validate_value/2,
    validate_value/3
]).

-define(NULL_VALUE, null).

-type path_segment() :: binary() | non_neg_integer().
-type validation_error() :: #{
    error := atom(),
    path := [path_segment()],
    _ => _
}.
-type validation_result() :: ok | {error, [validation_error()]}.
-type validation_options() :: #{
    null_value => term()
}.

-doc """
Validates a decoded JSON value against a validated schema.

Uses `null` as the JSON null representation.  This is suitable for terms
decoded by the default EMQX JSON decoder configuration.
""".
-spec validate_value(map(), term()) -> validation_result().

validate_value(Schema, Value) ->
    validate_value(Schema, Value, #{}).

-doc """
Validates a decoded JSON value against a validated schema with options.

`null_value` configures which Erlang term represents JSON null.  This keeps the
validator independent from a specific JSON decoder configuration.
""".
-spec validate_value(map(), term(), validation_options()) -> validation_result().

validate_value(Schema, Value, Options) ->
    Null = maps:get(null_value, Options, ?NULL_VALUE),
    Errors = validate_node(Schema, Value, [], Null),
    case Errors of
        [] -> ok;
        _ -> {error, lists:reverse(Errors)}
    end.

validate_node(#{<<"anyOf">> := AnyOf} = _Schema, Value, Path, Null) when is_list(AnyOf) ->
    validate_anyof(AnyOf, Value, Path, Null);
validate_node(Schema, Value, Path, Null) ->
    validate_typed(Schema, Value, Path, Null).

validate_anyof(AnyOf, Value, Path, Null) ->
    case
        lists:any(fun(SubSchema) -> validate_node(SubSchema, Value, Path, Null) =:= [] end, AnyOf)
    of
        true -> [];
        false -> [err(any_of_no_match, Path, #{})]
    end.

validate_typed(Schema, Value, Path, Null) ->
    {Type, Nullable} = schema_type(Schema),
    case Nullable andalso Value =:= Null of
        true ->
            [];
        false ->
            Errors0 = validate_type(Type, Schema, Value, Path, Null),
            validate_enum(Schema, Value, Path, Null, Errors0)
    end.

validate_type(<<"object">>, Schema, Value, Path, Null) when is_map(Value) ->
    Properties = maps:get(<<"properties">>, Schema, #{}),
    Required = maps:get(<<"required">>, Schema, []),
    MissingErrors =
        lists:filtermap(
            fun(Key) ->
                case maps:is_key(Key, Value) of
                    true ->
                        false;
                    false ->
                        {true, err(missing_required_property, Path ++ [Key], #{property => Key})}
                end
            end,
            Required
        ),
    UnexpectedErrors =
        lists:filtermap(
            fun(Key) ->
                case maps:is_key(Key, Properties) of
                    true -> false;
                    false -> {true, err(unexpected_property, Path ++ [Key], #{property => Key})}
                end
            end,
            maps:keys(Value)
        ),
    SubValidationErrors = lists:flatmap(
        fun({Key, SubSchema}) ->
            case Value of
                #{Key := SubValue} -> validate_node(SubSchema, SubValue, Path ++ [Key], Null);
                _ -> []
            end
        end,
        maps:to_list(Properties)
    ),
    MissingErrors ++ UnexpectedErrors ++ SubValidationErrors;
validate_type(<<"object">>, _Schema, _Value, Path, _Null) ->
    [err(wrong_type, Path, #{expected => <<"object">>, actual => actual_type(_Value)})];
validate_type(<<"array">>, Schema, Value, Path, Null) when is_list(Value) ->
    Items = maps:get(<<"items">>, Schema),
    {Errors, _} = lists:foldl(
        fun(Item, {Acc, I}) ->
            ItemErrors = validate_node(Items, Item, Path ++ [I], Null),
            {ItemErrors ++ Acc, I + 1}
        end,
        {[], 0},
        Value
    ),
    Errors;
validate_type(<<"array">>, _Schema, Value, Path, _Null) ->
    [err(wrong_type, Path, #{expected => <<"array">>, actual => actual_type(Value)})];
validate_type(<<"string">>, _Schema, Value, _Path, _Null) when is_binary(Value) ->
    [];
validate_type(<<"string">>, _Schema, Value, Path, _Null) ->
    [err(wrong_type, Path, #{expected => <<"string">>, actual => actual_type(Value)})];
validate_type(<<"integer">>, _Schema, Value, _Path, _Null) when is_integer(Value) ->
    [];
validate_type(<<"integer">>, _Schema, Value, Path, _Null) ->
    [err(wrong_type, Path, #{expected => <<"integer">>, actual => actual_type(Value)})];
validate_type(<<"number">>, _Schema, Value, _Path, _Null) when is_integer(Value); is_float(Value) ->
    [];
validate_type(<<"number">>, _Schema, Value, Path, _Null) ->
    [err(wrong_type, Path, #{expected => <<"number">>, actual => actual_type(Value)})];
validate_type(<<"boolean">>, _Schema, Value, _Path, _Null) when Value =:= true; Value =:= false ->
    [];
validate_type(<<"boolean">>, _Schema, Value, Path, _Null) ->
    [err(wrong_type, Path, #{expected => <<"boolean">>, actual => actual_type(Value)})].

validate_enum(Schema, Value, Path, Null, Errors0) ->
    case maps:get(<<"enum">>, Schema, undefined) of
        undefined ->
            Errors0;
        _Enum when Value =:= Null ->
            Errors0;
        Enum when is_list(Enum) ->
            case lists:member(Value, Enum) of
                true -> Errors0;
                false -> [err(enum_mismatch, Path, #{value => Value}) | Errors0]
            end
    end.

schema_type(Schema) ->
    case maps:get(<<"type">>, Schema) of
        Type when is_binary(Type) ->
            {Type, false};
        Types when is_list(Types) ->
            {[Type], _} = {lists:filter(fun(T) -> T =/= <<"null">> end, Types), Types},
            {Type, true}
    end.

actual_type(Value) when is_binary(Value) -> <<"string">>;
actual_type(Value) when is_integer(Value) -> <<"integer">>;
actual_type(Value) when is_float(Value) -> <<"number">>;
actual_type(Value) when Value =:= true; Value =:= false -> <<"boolean">>;
actual_type(Value) when is_map(Value) -> <<"object">>;
actual_type(Value) when is_list(Value) -> <<"array">>;
actual_type(null) -> <<"null">>;
actual_type(_Value) -> <<"unknown">>.

err(Code, Path, Extra) ->
    maps:merge(#{error => Code, path => Path}, Extra).
