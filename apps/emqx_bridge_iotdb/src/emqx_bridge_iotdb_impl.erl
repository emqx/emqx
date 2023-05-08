%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_impl).

-include("emqx_bridge_iotdb.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_query/3,
    on_query_async/4
]).

-type config() ::
    #{
        base_url := #{
            scheme := http | https,
            host := iolist(),
            port := inet:port_number(),
            path := '_'
        },
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        pool_size := pos_integer(),
        request := undefined | map(),
        is_aligned := boolean(),
        iotdb_version := binary(),
        device_id := binary() | undefined,
        atom() => '_'
    }.

-type state() ::
    #{
        base_path := '_',
        base_url := #{
            scheme := http | https,
            host := iolist(),
            port := inet:port_number(),
            path := '_'
        },
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        pool_size := pos_integer(),
        request := undefined | map(),
        is_aligned := boolean(),
        iotdb_version := binary(),
        device_id := binary() | undefined,
        atom() => '_'
    }.

-type manager_id() :: binary().

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------
callback_mode() -> async_if_possible.

-spec on_start(manager_id(), config()) -> {ok, state()} | no_return().
on_start(InstanceId, Config) ->
    %% [FIXME] The configuration passed in here is pre-processed and transformed
    %% in emqx_bridge_resource:parse_confs/2.
    case emqx_connector_http:on_start(InstanceId, Config) of
        {ok, State} ->
            ?SLOG(info, #{
                msg => "iotdb_bridge_started",
                instance_id => InstanceId,
                request => maps:get(request, State, <<>>)
            }),
            ?tp(iotdb_bridge_started, #{}),
            {ok, maps:merge(Config, State)};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_iotdb_bridge",
                instance_id => InstanceId,
                base_url => maps:get(request, Config, <<>>),
                reason => Reason
            }),
            throw(failed_to_start_iotdb_bridge)
    end.

-spec on_stop(manager_id(), state()) -> ok | {error, term()}.
on_stop(InstanceId, State) ->
    ?SLOG(info, #{
        msg => "stopping_iotdb_bridge",
        connector => InstanceId
    }),
    Res = emqx_connector_http:on_stop(InstanceId, State),
    ?tp(iotdb_bridge_stopped, #{instance_id => InstanceId}),
    Res.

-spec on_get_status(manager_id(), state()) ->
    {connected, state()} | {disconnected, state(), term()}.
on_get_status(InstanceId, State) ->
    emqx_connector_http:on_get_status(InstanceId, State).

-spec on_query(manager_id(), {send_message, map()}, state()) ->
    {ok, pos_integer(), [term()], term()}
    | {ok, pos_integer(), [term()]}
    | {error, term()}.
on_query(InstanceId, {send_message, Message}, State) ->
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_called",
        instance_id => InstanceId,
        send_message => Message,
        state => emqx_utils:redact(State)
    }),
    IoTDBPayload = make_iotdb_insert_request(Message, State),
    handle_response(
        emqx_connector_http:on_query(
            InstanceId, {send_message, IoTDBPayload}, State
        )
    ).

-spec on_query_async(manager_id(), {send_message, map()}, {function(), [term()]}, state()) ->
    {ok, pid()}.
on_query_async(InstanceId, {send_message, Message}, ReplyFunAndArgs0, State) ->
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_async_called",
        instance_id => InstanceId,
        send_message => Message,
        state => emqx_utils:redact(State)
    }),
    IoTDBPayload = make_iotdb_insert_request(Message, State),
    ReplyFunAndArgs =
        {
            fun(Result) ->
                Response = handle_response(Result),
                emqx_resource:apply_reply_fun(ReplyFunAndArgs0, Response)
            end,
            []
        },
    emqx_connector_http:on_query_async(
        InstanceId, {send_message, IoTDBPayload}, ReplyFunAndArgs, State
    ).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

preproc_data(DataList) ->
    lists:map(
        fun(
            #{
                measurement := Measurement,
                data_type := DataType,
                value := Value
            } = Data
        ) ->
            #{
                timestamp => emqx_plugin_libs_rule:preproc_tmpl(
                    maps:get(<<"timestamp">>, Data, <<"now">>)
                ),
                measurement => emqx_plugin_libs_rule:preproc_tmpl(Measurement),
                data_type => DataType,
                value => emqx_plugin_libs_rule:preproc_tmpl(Value)
            }
        end,
        DataList
    ).

proc_data(PreProcessedData, Msg) ->
    NowNS = erlang:system_time(nanosecond),
    Nows = #{
        now_ms => erlang:convert_time_unit(NowNS, nanosecond, millisecond),
        now_us => erlang:convert_time_unit(NowNS, nanosecond, microsecond),
        now_ns => NowNS
    },
    lists:map(
        fun(
            #{
                timestamp := TimestampTkn,
                measurement := Measurement,
                data_type := DataType,
                value := ValueTkn
            }
        ) ->
            #{
                timestamp => iot_timestamp(
                    emqx_plugin_libs_rule:proc_tmpl(TimestampTkn, Msg), Nows
                ),
                measurement => emqx_plugin_libs_rule:proc_tmpl(Measurement, Msg),
                data_type => DataType,
                value => proc_value(DataType, ValueTkn, Msg)
            }
        end,
        PreProcessedData
    ).

iot_timestamp(Timestamp, #{now_ms := NowMs}) when
    Timestamp =:= <<"now">>; Timestamp =:= <<"now_ms">>; Timestamp =:= <<>>
->
    NowMs;
iot_timestamp(Timestamp, #{now_us := NowUs}) when Timestamp =:= <<"now_us">> ->
    NowUs;
iot_timestamp(Timestamp, #{now_ns := NowNs}) when Timestamp =:= <<"now_ns">> ->
    NowNs;
iot_timestamp(Timestamp, _) when is_binary(Timestamp) ->
    binary_to_integer(Timestamp).

proc_value(<<"TEXT">>, ValueTkn, Msg) ->
    case emqx_plugin_libs_rule:proc_tmpl(ValueTkn, Msg) of
        <<"undefined">> -> null;
        Val -> Val
    end;
proc_value(<<"BOOLEAN">>, ValueTkn, Msg) ->
    convert_bool(replace_var(ValueTkn, Msg));
proc_value(Int, ValueTkn, Msg) when Int =:= <<"INT32">>; Int =:= <<"INT64">> ->
    convert_int(replace_var(ValueTkn, Msg));
proc_value(Int, ValueTkn, Msg) when Int =:= <<"FLOAT">>; Int =:= <<"DOUBLE">> ->
    convert_float(replace_var(ValueTkn, Msg)).

replace_var(Tokens, Data) when is_list(Tokens) ->
    [Val] = emqx_plugin_libs_rule:proc_tmpl(Tokens, Data, #{return => rawlist}),
    Val;
replace_var(Val, _Data) ->
    Val.

convert_bool(B) when is_boolean(B) -> B;
convert_bool(1) -> true;
convert_bool(0) -> false;
convert_bool(<<"1">>) -> true;
convert_bool(<<"0">>) -> false;
convert_bool(<<"true">>) -> true;
convert_bool(<<"True">>) -> true;
convert_bool(<<"TRUE">>) -> true;
convert_bool(<<"false">>) -> false;
convert_bool(<<"False">>) -> false;
convert_bool(<<"FALSE">>) -> false;
convert_bool(undefined) -> null.

convert_int(Int) when is_integer(Int) -> Int;
convert_int(Float) when is_float(Float) -> floor(Float);
convert_int(Str) when is_binary(Str) ->
    try
        binary_to_integer(Str)
    catch
        _:_ ->
            convert_int(binary_to_float(Str))
    end;
convert_int(undefined) ->
    null.

convert_float(Float) when is_float(Float) -> Float;
convert_float(Int) when is_integer(Int) -> Int * 10 / 10;
convert_float(Str) when is_binary(Str) ->
    try
        binary_to_float(Str)
    catch
        _:_ ->
            convert_float(binary_to_integer(Str))
    end;
convert_float(undefined) ->
    null.

make_iotdb_insert_request(Message, State) ->
    IsAligned = maps:get(is_aligned, State, false),
    DeviceId = device_id(Message, State),
    IotDBVsn = maps:get(iotdb_version, State, ?VSN_1_0_X),
    Payload = make_list(maps:get(payload, Message)),
    PreProcessedData = preproc_data(Payload),
    DataList = proc_data(PreProcessedData, Message),
    InitAcc = #{timestamps => [], measurements => [], dtypes => [], values => []},
    Rows = replace_dtypes(aggregate_rows(DataList, InitAcc), IotDBVsn),
    maps:merge(Rows, #{
        iotdb_field_key(is_aligned, IotDBVsn) => IsAligned,
        iotdb_field_key(device_id, IotDBVsn) => DeviceId
    }).

replace_dtypes(Rows, IotDBVsn) ->
    {Types, Map} = maps:take(dtypes, Rows),
    Map#{iotdb_field_key(data_types, IotDBVsn) => Types}.

aggregate_rows(DataList, InitAcc) ->
    lists:foldr(
        fun(
            #{
                timestamp := Timestamp,
                measurement := Measurement,
                data_type := DataType,
                value := Data
            },
            #{
                timestamps := AccTs,
                measurements := AccM,
                dtypes := AccDt,
                values := AccV
            } = Acc
        ) ->
            Timestamps = [Timestamp | AccTs],
            case index_of(Measurement, AccM) of
                0 ->
                    Acc#{
                        timestamps => Timestamps,
                        values => [pad_value(Data, length(AccTs)) | pad_existing_values(AccV)],
                        measurements => [Measurement | AccM],
                        dtypes => [DataType | AccDt]
                    };
                Index ->
                    Acc#{
                        timestamps => Timestamps,
                        values => insert_value(Index, Data, AccV),
                        measurements => AccM,
                        dtypes => AccDt
                    }
            end
        end,
        InitAcc,
        DataList
    ).

pad_value(Data, N) ->
    [Data | lists:duplicate(N, null)].

pad_existing_values(Values) ->
    [[null | Value] || Value <- Values].

index_of(E, List) ->
    string:str(List, [E]).

insert_value(_Index, _Data, []) ->
    [];
insert_value(1, Data, [Value | Values]) ->
    [[Data | Value] | insert_value(0, Data, Values)];
insert_value(Index, Data, [Value | Values]) ->
    [[null | Value] | insert_value(Index - 1, Data, Values)].

iotdb_field_key(is_aligned, ?VSN_1_0_X) ->
    <<"is_aligned">>;
iotdb_field_key(is_aligned, ?VSN_0_13_X) ->
    <<"isAligned">>;
iotdb_field_key(device_id, ?VSN_1_0_X) ->
    <<"device">>;
iotdb_field_key(device_id, ?VSN_0_13_X) ->
    <<"deviceId">>;
iotdb_field_key(data_types, ?VSN_1_0_X) ->
    <<"data_types">>;
iotdb_field_key(data_types, ?VSN_0_13_X) ->
    <<"dataTypes">>.

make_list(List) when is_list(List) -> List;
make_list(Data) -> [Data].

device_id(Message, State) ->
    case maps:get(device_id, State, undefined) of
        undefined ->
            case maps:get(payload, Message) of
                #{device_id := DeviceId} ->
                    DeviceId;
                _NotFound ->
                    Topic = maps:get(topic, Message),
                    case re:replace(Topic, "/", ".", [global, {return, binary}]) of
                        <<"root.", _/binary>> = Device -> Device;
                        Device -> <<"root.", Device/binary>>
                    end
            end;
        DeviceId ->
            DeviceIdTkn = emqx_plugin_libs_rule:preproc_tmpl(DeviceId),
            emqx_plugin_libs_rule:proc_tmpl(DeviceIdTkn, Message)
    end.

handle_response({ok, 200, _Headers, Body} = Resp) ->
    eval_response_body(Body, Resp);
handle_response({ok, 200, Body} = Resp) ->
    eval_response_body(Body, Resp);
handle_response({ok, Code, _Headers, Body}) ->
    {error, #{code => Code, body => Body}};
handle_response({ok, Code, Body}) ->
    {error, #{code => Code, body => Body}};
handle_response({error, _} = Error) ->
    Error.

eval_response_body(Body, Resp) ->
    case emqx_utils_json:decode(Body) of
        #{<<"code">> := 200} -> Resp;
        Reason -> {error, Reason}
    end.
