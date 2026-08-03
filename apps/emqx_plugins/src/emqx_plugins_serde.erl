%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_serde).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("erlavro/include/erlavro.hrl").

%% API
-export([
    start_link/0,
    add_schema/2,
    delete_schema/1
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-export([
    decode/2,
    decode/3,
    encode/2
]).

-define(SERVER, ?MODULE).

%%-------------------------------------------------------------------------------------------------
%% records
%%-------------------------------------------------------------------------------------------------

-record(plugin_schema_serde, {
    name :: schema_name(),
    eval_context :: term()
}).

%%-------------------------------------------------------------------------------------------------
%% messages
%%-------------------------------------------------------------------------------------------------

-record(add_schema, {
    name_vsn :: name_vsn(),
    avsc_bin :: binary()
}).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec add_schema(schema_name(), binary()) -> ok | {error, term()}.
add_schema(NameVsn, AvscBin) ->
    case gen_server:call(?SERVER, #add_schema{name_vsn = NameVsn, avsc_bin = AvscBin}, infinity) of
        ok ->
            ?SLOG(debug, #{msg => "plugin_schema_added", plugin => NameVsn}),
            ok;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "plugin_schema_add_failed",
                plugin => NameVsn,
                reason => emqx_utils:readable_error_msg(Reason)
            }),
            Error
    end.

-spec delete_schema(schema_name()) -> ok | {error, term()}.
delete_schema(NameVsn) ->
    case lookup_serde(NameVsn) of
        {ok, _Serde} ->
            async_delete_serdes([NameVsn]),
            ok;
        {error, not_found} ->
            {error, not_found}
    end.

-spec decode(schema_name(), encoded_data()) -> {ok, decoded_data()} | {error, any()}.
decode(SerdeName, RawData) ->
    with_serde(
        ?FUNCTION_NAME,
        SerdeName,
        [RawData]
    ).

-spec decode(schema_name(), binary(), encoded_data()) ->
    {ok, decoded_data()} | {error, any()}.
decode(SerdeName, AvscBin, RawData) ->
    try
        Serde = make_serde(SerdeName, AvscBin),
        with_serde_record(?FUNCTION_NAME, Serde, [RawData])
    catch
        _Kind:Error ->
            {error, #{reason => bad_schema, details => Error}}
    end.

-spec encode(schema_name(), decoded_data()) -> {ok, encoded_data()} | {error, any()}.
encode(SerdeName, Data) ->
    with_serde(
        ?FUNCTION_NAME,
        SerdeName,
        [Data]
    ).

%%-------------------------------------------------------------------------------------------------
%% `gen_server' API
%%-------------------------------------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    ok = emqx_utils_ets:new(?PLUGIN_SERDE_TAB, [
        public, ordered_set, {keypos, #plugin_schema_serde.name}
    ]),
    State = #{},
    Avscs = emqx_plugins_fs:read_avsc_bin_all(),
    %% force build all schemas at startup
    %% otherwise plugin schema may not be available when needed
    _ = build_serdes(Avscs),
    {ok, State}.

handle_call(#add_schema{name_vsn = NameVsn, avsc_bin = AvscBin}, _From, State) ->
    BuildRes = do_build_serde({NameVsn, AvscBin}),
    {reply, BuildRes, State};
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({delete_serdes, Names}, State) ->
    lists:foreach(fun ensure_serde_absent/1, Names),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

lookup_serde(SchemaName) ->
    case ets:lookup(?PLUGIN_SERDE_TAB, to_bin(SchemaName)) of
        [] ->
            {error, not_found};
        [Serde] ->
            {ok, Serde}
    end.

build_serdes(Avscs) ->
    ok = lists:foreach(fun do_build_serde/1, Avscs).

do_build_serde({NameVsn, AvscBin}) ->
    try
        Serde = make_serde(NameVsn, AvscBin),
        true = ets:insert(?PLUGIN_SERDE_TAB, Serde),
        ok
    catch
        Kind:Error:Stacktrace ->
            ?SLOG(
                error,
                #{
                    msg => "error_building_plugin_schema_serde",
                    name => NameVsn,
                    kind => Kind,
                    error => Error,
                    stacktrace => Stacktrace
                }
            ),
            {error, #{reason => bad_schema, details => Error}}
    end.

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

ensure_serde_absent(Name) when not is_binary(Name) ->
    ensure_serde_absent(to_bin(Name));
ensure_serde_absent(Name) ->
    case lookup_serde(Name) of
        {ok, _Serde} ->
            _ = ets:delete(?PLUGIN_SERDE_TAB, Name),
            ok;
        {error, not_found} ->
            ok
    end.

async_delete_serdes(Names) ->
    gen_server:cast(?MODULE, {delete_serdes, Names}).

with_serde(Op, SerdeName, Args) ->
    case lookup_serde(SerdeName) of
        {ok, Serde} ->
            with_serde_record(Op, Serde, Args);
        {error, not_found} ->
            {error, #{
                error_msg => error_msg(Op),
                reason => plugin_serde_not_found,
                serde_name => SerdeName
            }}
    end.

with_serde_record(Op, #plugin_schema_serde{} = Serde, Args) ->
    WhichOp = which_op(Op),
    try
        eval_serde(Op, Serde, Args)
    catch
        throw:Reason ->
            ?SLOG(error, #{
                msg => "plugin_schema_op_failed",
                which_op => WhichOp,
                reason => emqx_utils:readable_error_msg(Reason)
            }),
            {error, Reason};
        error:Reason:Stacktrace ->
            %% unexpected errors, log stacktrace
            ?SLOG(warning, #{
                msg => "plugin_schema_op_failed",
                which_op => WhichOp,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            {error, #{
                which_op => WhichOp,
                reason => Reason
            }}
    end.

eval_serde(decode, #plugin_schema_serde{name = Name, eval_context = Store}, [Data]) ->
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
    end;
eval_serde(encode, #plugin_schema_serde{name = Name, eval_context = Store}, [Data]) ->
    {ok, avro_json_encoder:encode(Store, Name, Data)};
eval_serde(_, _, _) ->
    throw(#{error_msg => "unexpected_plugin_avro_op"}).

which_op(Op) ->
    atom_to_list(Op) ++ "_avro_json".

error_msg(Op) ->
    atom_to_list(Op) ++ "_avro_data".

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
