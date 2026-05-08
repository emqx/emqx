%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_schema_oai_tool_converter).

-include_lib("hocon/include/hoconsc.hrl").

-export([to_json_schema/1, to_json_schema/2]).

-type option() :: {exclude, [{module(), atom()} | {module(), atom(), atom()}]}.

-spec to_json_schema(hocon_schema:type()) -> map().
to_json_schema(Type) ->
    to_json_schema(Type, []).

-spec to_json_schema(hocon_schema:type(), [option()]) -> map().
to_json_schema(Type, Opts) ->
    Schema = convert(Type, undefined, #{exclude => excludes(Opts), root => true}),
    ok = validate(Schema),
    Schema.

convert({object, Fields}, Module, Opts) when is_list(Fields) ->
    object_schema(Fields, Module, Opts);
convert(?R_REF(Module, StructName), _LocalModule, Opts) ->
    add_description(
        object_schema(Module:fields(StructName), Module, Opts#{struct => StructName}),
        Module:desc(StructName)
    );
convert(?REF(StructName), Module, Opts) when is_atom(Module) ->
    add_description(
        object_schema(Module:fields(StructName), Module, Opts#{struct => StructName}),
        Module:desc(StructName)
    );
convert(?ARRAY(Item), Module, Opts) ->
    #{
        <<"type">> => <<"array">>,
        <<"items">> => convert(Item, Module, Opts#{root => false})
    };
convert(?ENUM(Items), _Module, _Opts) ->
    #{<<"type">> => <<"string">>, <<"enum">> => [to_binary(I) || I <- Items]};
convert(?UNION(Types, _DisplayName), Module, Opts) ->
    Members = hoconsc:union_members(Types),
    #{<<"anyOf">> => [convert(Member, Module, Opts#{root => false}) || Member <- Members]};
convert(?MAP(_Name, _Type), _Module, _Opts) ->
    %% Open unknown-key objects are not valid in strict OpenAI tool schemas.
    %% LLM-facing schemas should model such inputs as arrays of name/value entries.
    #{<<"type">> => <<"string">>};
convert(Type, Module, _Opts) when ?IS_TYPEREFL(Type) ->
    TypeStr = lists:flatten(typerefl:name(Type)),
    normalize_swagger(emqx_conf_schema_types:readable_swagger(Module, TypeStr));
convert(Atom, _Module, _Opts) when is_atom(Atom) ->
    #{<<"type">> => <<"string">>, <<"enum">> => [atom_to_binary(Atom, utf8)]}.

object_schema(Fields0, Module, Opts) ->
    Struct = maps:get(struct, Opts, undefined),
    Fields = [Field || Field <- Fields0, not excluded_field(Field, Module, Struct, Opts)],
    Props = maps:from_list([field_property(Field, Module, Opts) || Field <- Fields]),
    Required = [to_binary(Name) || {Name, _Hocon} <- Fields],
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => Props,
        <<"required">> => Required,
        <<"additionalProperties">> => false
    }.

field_property({Name, Hocon}, Module, Opts) ->
    Type = hocon_schema:field_schema(Hocon, type),
    Required = hocon_schema:field_schema(Hocon, required) =:= true,
    Schema0 = convert(Type, Module, Opts#{root => false}),
    Schema1 = maybe_nullable(Schema0, Required),
    Schema = add_description(Schema1, hocon_schema:field_schema(Hocon, desc)),
    {to_binary(Name), Schema}.

maybe_nullable(#{<<"type">> := Type} = Schema, false) when is_binary(Type) ->
    Schema#{<<"type">> => [Type, <<"null">>]};
maybe_nullable(Schema, _Required) ->
    Schema.

excluded_field({Name, _Hocon}, Module, Struct, Opts) ->
    Excludes = maps:get(exclude, Opts, []),
    lists:any(
        fun
            ({Module0, Struct0, Name0}) ->
                Module0 =:= Module andalso Struct0 =:= Struct andalso Name0 =:= Name;
            ({Module0, Name0}) ->
                Module0 =:= Module andalso Name0 =:= Name
        end,
        Excludes
    ).

excludes(Opts) ->
    lists:append([Value || {exclude, Value} <- Opts]) ++ default_excludes().

default_excludes() ->
    [{emqx_agent_schema, pipeline, active}].

normalize_swagger(Schema) when is_map(Schema) ->
    Schema1 = maps:from_list([
        {to_binary(K), normalize_swagger_value(K, V)}
     || {K, V} <- maps:to_list(Schema)
    ]),
    keep_supported(Schema1);
normalize_swagger(Value) ->
    normalize_swagger_value(undefined, Value).

normalize_swagger_value(type, Value) ->
    to_binary(Value);
normalize_swagger_value(<<"type">>, Value) ->
    to_binary(Value);
normalize_swagger_value(enum, Values) when is_list(Values) ->
    [to_binary(V) || V <- Values];
normalize_swagger_value(<<"enum">>, Values) when is_list(Values) ->
    [to_binary(V) || V <- Values];
normalize_swagger_value(<<"oneOf">>, Values) when is_list(Values) ->
    [normalize_swagger(V) || V <- Values];
normalize_swagger_value(oneOf, Values) when is_list(Values) ->
    [normalize_swagger(V) || V <- Values];
normalize_swagger_value(Value, Values) when
    (Value =:= items orelse Value =:= <<"items">>) andalso is_map(Values)
->
    normalize_swagger(Values);
normalize_swagger_value(_Key, Value) when is_atom(Value) ->
    to_binary(Value);
normalize_swagger_value(_Key, Value) when is_map(Value) ->
    normalize_swagger(Value);
normalize_swagger_value(_Key, Value) when is_list(Value) ->
    [normalize_swagger_value(undefined, V) || V <- Value];
normalize_swagger_value(_Key, Value) ->
    Value.

keep_supported(#{<<"oneOf">> := OneOf}) ->
    #{<<"anyOf">> => OneOf};
keep_supported(#{<<"type">> := <<"object">>} = Schema) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => maps:get(<<"properties">>, Schema, #{}),
        <<"required">> => maps:get(
            <<"required">>, Schema, maps:keys(maps:get(<<"properties">>, Schema, #{}))
        ),
        <<"additionalProperties">> => false
    };
keep_supported(#{<<"type">> := <<"array">>} = Schema) ->
    #{
        <<"type">> => <<"array">>,
        <<"items">> => maps:get(<<"items">>, Schema, #{<<"type">> => <<"string">>})
    };
keep_supported(#{<<"type">> := Type} = Schema) ->
    maps:with([<<"type">>, <<"enum">>], Schema#{<<"type">> => Type});
keep_supported(Schema) ->
    maps:with([<<"anyOf">>], Schema).

add_description(Schema, Desc) ->
    case resolve_description(Desc) of
        undefined -> Schema;
        Text -> Schema#{<<"description">> => Text}
    end.

resolve_description(?DESC(Namespace, Id)) ->
    case emqx_dashboard_desc_cache:lookup(en, Namespace, Id, desc) of
        undefined -> undefined;
        Text -> binary:replace(to_binary(Text), [<<"\n">>], <<" ">>, [global])
    end;
resolve_description(Text) when is_binary(Text) ->
    Text;
resolve_description(_) ->
    undefined.

validate(Schema) ->
    case emqx_agent_oai_tool_schema:validate_schema(Schema) of
        ok -> ok;
        {error, Errors} -> error({invalid_oai_tool_schema, Errors})
    end.

to_binary(Bin) when is_binary(Bin) -> Bin;
to_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_binary(Int) when is_integer(Int) -> integer_to_binary(Int);
to_binary(List) when is_list(List) -> unicode:characters_to_binary(List).
