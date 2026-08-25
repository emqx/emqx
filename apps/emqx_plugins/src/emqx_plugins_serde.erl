%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_serde).

-moduledoc """
Validate plugin configuration against the plugin's Avro schema.

The caller passes the schema (`config_schema.avsc` content) together with the
configuration. The schema is parsed on each call and is not kept in memory.
""".

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("erlavro/include/erlavro.hrl").

-export([
    check_schema/2,
    decode/3
]).

-define(WHICH_OP, "decode_avro_json").

%%-------------------------------------------------------------------------------------------------
%% records
%%-------------------------------------------------------------------------------------------------

-record(plugin_schema_serde, {
    name :: schema_name(),
    eval_context :: term()
}).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

-doc "Check that the schema binary is a valid Avro schema.".
-spec check_schema(name_vsn(), binary()) -> ok | {error, #{reason := bad_schema, _ => _}}.
check_schema(NameVsn, AvscBin) ->
    try
        _ = make_serde(NameVsn, AvscBin),
        ok
    catch
        _Kind:Error ->
            {error, bad_schema_error(Error)}
    end.

-doc "Decode (validate) the JSON configuration against the schema binary.".
-spec decode(name_vsn(), binary(), encoded_data()) ->
    {ok, decoded_data()} | {error, any()}.
decode(SerdeName, AvscBin, RawData) ->
    try
        Serde = make_serde(SerdeName, AvscBin),
        decode_with_serde(Serde, RawData)
    catch
        _Kind:Error ->
            {error, bad_schema_error(Error)}
    end.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

bad_schema_error(Error) ->
    #{reason => bad_schema, details => Error}.

make_serde(NameVsn, AvscBin) when not is_binary(NameVsn) ->
    make_serde(to_bin(NameVsn), AvscBin);
make_serde(NameVsn, AvscBin) ->
    Store0 = avro_schema_store:new([map]),
    %% import the schema into the map store with an assigned name
    %% if it's a named schema (e.g. struct), then Name is added as alias
    Store = avro_schema_store:import_schema_json(NameVsn, AvscBin, Store0),
    #plugin_schema_serde{
        name = NameVsn,
        eval_context = Store
    }.

decode_with_serde(#plugin_schema_serde{} = Serde, Data) ->
    try
        decode_value(Serde, Data)
    catch
        throw:Reason ->
            ?SLOG(error, #{
                msg => "plugin_schema_op_failed",
                which_op => ?WHICH_OP,
                reason => emqx_utils:readable_error_msg(Reason)
            }),
            {error, Reason};
        error:Reason:Stacktrace ->
            %% unexpected errors, log stacktrace
            ?SLOG(warning, #{
                msg => "plugin_schema_op_failed",
                which_op => ?WHICH_OP,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            {error, #{
                which_op => ?WHICH_OP,
                reason => Reason
            }}
    end.

decode_value(#plugin_schema_serde{name = Name, eval_context = Store}, Data) ->
    Opts = avro:make_decoder_options([
        {map_type, map},
        {record_type, map},
        {encoding, avro_json},
        {hook, fun decode_hook/4}
    ]),
    try avro_json_decoder:decode_value(Data, Name, Store, Opts) of
        Decoded ->
            {ok, Decoded}
    catch
        error:function_clause:Stacktrace ->
            case is_avro_decoder_type_mismatch_stack(Stacktrace) of
                true ->
                    {ok, RootType} = avro_schema_store:lookup_type(Name, Store),
                    DecodedData = emqx_utils_json:decode(Data),
                    throw(invalid_type_error(RootType, DecodedData, <<"$">>));
                false ->
                    erlang:raise(error, function_clause, Stacktrace)
            end
    end.

decode_hook(Type, SubNameOrIndex, Data, DecodeFun) ->
    Path0 = get(?MODULE),
    Path = push_path(SubNameOrIndex, Path0),
    put(?MODULE, Path),
    try
        DecodeFun(Data)
    catch
        error:{unknown_union_member, Member} ->
            throw(#{
                reason => invalid_union_member,
                path => format_path(lists:reverse(Path)),
                expected => expected_type(Type, SubNameOrIndex),
                actual => to_bin(Member)
            });
        error:function_clause:Stacktrace ->
            ExpectedType = expected_avro_type(Type, SubNameOrIndex),
            case is_avro_type_mismatch(Stacktrace, ExpectedType, Data) of
                true ->
                    throw(invalid_type_error(ExpectedType, Data, format_path(lists:reverse(Path))));
                false ->
                    erlang:raise(error, function_clause, Stacktrace)
            end;
        error:badarg:Stacktrace ->
            ExpectedType = expected_avro_type(Type, SubNameOrIndex),
            case is_avro_fixed_type_mismatch(Stacktrace, ExpectedType, Data) of
                true ->
                    throw(invalid_type_error(ExpectedType, Data, format_path(lists:reverse(Path))));
                false ->
                    erlang:raise(error, badarg, Stacktrace)
            end
    after
        case Path0 of
            undefined -> erase(?MODULE);
            _ -> put(?MODULE, Path0)
        end
    end.

is_avro_type_mismatch(
    [{avro_json_decoder, parse, [Data, #avro_enum_type{}, _, _], _} | _],
    _Type,
    Data
) ->
    true;
is_avro_type_mismatch(
    [{avro_json_decoder, parse, [Data, #avro_fixed_type{}, _, _], _} | _],
    _Type,
    Data
) ->
    true;
is_avro_type_mismatch([{avro_json_decoder, Function, _, _} | _], _Type, _Data) ->
    lists:member(Function, [parse_prim, parse_record, parse_array, parse_map, parse_union]);
is_avro_type_mismatch(_Stacktrace, _Type, _Data) ->
    false.

is_avro_decoder_type_mismatch_stack([
    {avro_json_decoder, Function, _, _} | _
]) ->
    lists:member(Function, [parse, parse_prim, parse_record, parse_array, parse_map, parse_union]);
is_avro_decoder_type_mismatch_stack(_) ->
    false.

is_avro_fixed_type_mismatch(Stacktrace, #avro_fixed_type{}, Data) when not is_binary(Data) ->
    lists:any(
        fun
            ({avro_json_decoder, parse_bytes, _, _}) -> true;
            (_) -> false
        end,
        Stacktrace
    );
is_avro_fixed_type_mismatch(_Stacktrace, _ExpectedType, _Data) ->
    false.

invalid_type_error(ExpectedType, Data, Path) ->
    #{
        reason => invalid_type,
        path => Path,
        expected => avro:get_type_fullname(ExpectedType),
        actual => json_type(Data)
    }.

push_path(<<>>, undefined) ->
    [];
push_path(none, undefined) ->
    [];
push_path(<<>>, Path) ->
    Path;
push_path(none, Path) ->
    Path;
push_path(SubNameOrIndex, undefined) ->
    [SubNameOrIndex];
push_path(SubNameOrIndex, Path) ->
    [SubNameOrIndex | Path].

format_path([]) ->
    <<"$">>;
format_path(Path) ->
    iolist_to_binary(lists:join(<<".">>, lists:map(fun format_path_part/1, Path))).

format_path_part(Index) when is_integer(Index) ->
    integer_to_binary(Index);
format_path_part(Name) ->
    to_bin(Name).

expected_type(Type, SubNameOrIndex) ->
    avro:get_type_fullname(expected_avro_type(Type, SubNameOrIndex)).

expected_avro_type(#avro_record_type{} = Type, FieldName) when is_binary(FieldName) ->
    avro_record:get_field_type(FieldName, Type);
expected_avro_type(#avro_array_type{} = Type, Index) when is_integer(Index) ->
    avro_array:get_items_type(Type);
expected_avro_type(#avro_map_type{} = Type, Key) when is_binary(Key) ->
    avro_map:get_items_type(Type);
expected_avro_type(Type, _SubNameOrIndex) ->
    Type.

json_type(Value) when is_binary(Value) -> <<"string">>;
json_type(Value) when is_integer(Value) -> <<"integer">>;
json_type(Value) when is_float(Value) -> <<"number">>;
json_type(Value) when is_boolean(Value) -> <<"boolean">>;
json_type(null) -> <<"null">>;
json_type(Value) when is_map(Value) -> <<"object">>;
json_type(Value) when is_list(Value) -> <<"array">>;
json_type(_) -> <<"unknown">>.

to_bin(A) when is_atom(A) -> atom_to_binary(A);
to_bin(L) when is_list(L) -> iolist_to_binary(L);
to_bin(B) when is_binary(B) -> B.
