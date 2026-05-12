%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_schema_oai_tool_converter).

-export([to_json_schema/1]).

-type path() :: [atom() | binary()].

-spec to_json_schema(path()) -> map().
to_json_schema(Path) when is_list(Path) ->
    Avsc = read_json_file("config_schema.avsc"),
    I18n = read_json_file("config_i18n.json"),
    Ctx = #{types => collect_types(Avsc), i18n => I18n},
    convert(select_path(Path, Avsc), Ctx).

read_json_file(Name) ->
    File = filename:join(code:priv_dir(emqx_agent), Name),
    {ok, Bin} = file:read_file(File),
    emqx_utils_json:decode(Bin).

select_path([], Schema) ->
    Schema;
select_path([Field | Rest], #{<<"type">> := <<"record">>, <<"fields">> := Fields}) ->
    Name = to_binary(Field),
    FieldSchema = hd([F || #{<<"name">> := N} = F <- Fields, N =:= Name]),
    select_path(Rest, maps:get(<<"type">>, FieldSchema));
select_path([items | Rest], #{<<"type">> := <<"array">>, <<"items">> := Items}) ->
    select_path(Rest, Items);
select_path([<<"items">> | Rest], #{<<"type">> := <<"array">>, <<"items">> := Items}) ->
    select_path(Rest, Items).

collect_types(Schema) ->
    collect_types(Schema, #{}).

collect_types(
    #{<<"type">> := <<"record">>, <<"name">> := Name, <<"fields">> := Fields} = Schema, Acc0
) ->
    Acc = maps:put(Name, Schema, Acc0),
    lists:foldl(fun(Field, A) -> collect_types(maps:get(<<"type">>, Field), A) end, Acc, Fields);
collect_types(#{<<"type">> := <<"array">>, <<"items">> := Items}, Acc) ->
    collect_types(Items, Acc);
collect_types(#{<<"type">> := Type}, Acc) ->
    collect_types(Type, Acc);
collect_types(Types, Acc) when is_list(Types) ->
    lists:foldl(fun(Type, A) -> collect_types(Type, A) end, Acc, Types);
collect_types(_Schema, Acc) ->
    Acc.

convert(Type, Ctx) when is_binary(Type) ->
    case primitive(Type) of
        {ok, Primitive} ->
            #{<<"type">> => Primitive};
        error ->
            convert(maps:get(Type, maps:get(types, Ctx)), Ctx)
    end;
convert(#{<<"type">> := <<"record">>, <<"name">> := _Name, <<"fields">> := Fields}, Ctx) ->
    Props = maps:from_list([field_property(Field, Ctx) || Field <- Fields]),
    Required = [Name || #{<<"name">> := Name} <- Fields],
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => Props,
        <<"required">> => Required,
        <<"additionalProperties">> => false
    };
convert(#{<<"type">> := <<"array">>, <<"items">> := Items}, Ctx) ->
    #{<<"type">> => <<"array">>, <<"items">> => convert(Items, Ctx)};
convert(#{<<"type">> := <<"enum">>, <<"symbols">> := Symbols}, _Ctx) ->
    #{<<"type">> => <<"string">>, <<"enum">> => Symbols};
convert(#{<<"type">> := Type}, Ctx) ->
    convert(Type, Ctx);
convert(Types, Ctx) when is_list(Types) ->
    #{<<"anyOf">> => [union_member_schema(Type, Ctx) || Type <- Types]}.

field_property(#{<<"name">> := Name, <<"type">> := Type} = Field, Ctx) ->
    Schema0 = convert(Type, Ctx),
    Schema1 = maybe_type_enum(Name, Field, Schema0),
    {Name, add_description(Schema1, maps:get(<<"description">>, Field, undefined), Ctx)}.

union_member_schema(#{<<"type">> := <<"record">>, <<"name">> := Name} = Record, Ctx) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{Name => convert(Record, Ctx)},
        <<"required">> => [Name],
        <<"additionalProperties">> => false
    };
union_member_schema(Type, Ctx) when is_binary(Type) ->
    case primitive(Type) of
        {ok, _} -> convert(Type, Ctx);
        error -> union_member_schema(maps:get(Type, maps:get(types, Ctx)), Ctx)
    end;
union_member_schema(Type, Ctx) ->
    convert(Type, Ctx).

maybe_type_enum(
    <<"type">>, #{<<"default">> := Default}, #{<<"type">> := <<"string">>} = Schema
) when
    is_binary(Default)
->
    Schema#{<<"enum">> => [Default]};
maybe_type_enum(_Name, _Field, Schema) ->
    Schema.

add_description(Schema, undefined, _Ctx) ->
    Schema;
add_description(Schema, <<"$", _/binary>> = Ref, Ctx) ->
    case maps:get(Ref, maps:get(i18n, Ctx), undefined) of
        #{<<"en">> := Text} -> Schema#{<<"description">> => Text};
        _ -> Schema#{<<"description">> => Ref}
    end;
add_description(Schema, Text, _Ctx) when is_binary(Text) ->
    Schema#{<<"description">> => Text}.

primitive(<<"string">>) -> {ok, <<"string">>};
primitive(<<"boolean">>) -> {ok, <<"boolean">>};
primitive(<<"int">>) -> {ok, <<"integer">>};
primitive(<<"long">>) -> {ok, <<"integer">>};
primitive(<<"float">>) -> {ok, <<"number">>};
primitive(<<"double">>) -> {ok, <<"number">>};
primitive(_) -> error.

to_binary(B) when is_binary(B) -> B;
to_binary(A) when is_atom(A) -> atom_to_binary(A, utf8).
