%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_connector).

-behaviour(emqx_connector_examples).

-behaviour(emqx_resource).

-include("emqx_bridge_iotdb.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_query/3,
    on_query_async/4,
    on_batch_query/3,
    on_batch_query_async/4,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3,
    on_format_query_result/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    connector_examples/1,
    connector_example_values/0
]).

%% emqx_connector_resource behaviour callbacks
-export([connector_config/2]).

-type config() ::
    #{
        request_base := #{
            scheme := http | https,
            host := iolist(),
            port := inet:port_number()
        },
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        pool_size := pos_integer(),
        iotdb_version := atom(),
        request => undefined | map(),
        atom() => _
    }.

-type state() ::
    #{
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        channels := map(),
        iotdb_version := atom(),
        request => undefined | map(),
        atom() => _
    }.

-type manager_id() :: binary().

-define(CONNECTOR_TYPE, iotdb).
-define(IOTDB_PING_PATH, <<"ping">>).

-import(hoconsc, [mk/2, enum/1, ref/2]).

%%-------------------------------------------------------------------------------------
%% connector examples
%%-------------------------------------------------------------------------------------
connector_examples(Method) ->
    [
        #{
            <<"iotdb">> =>
                #{
                    summary => <<"Apache IoTDB Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_example_values()
                    )
                }
        }
    ].

connector_example_values() ->
    #{
        name => <<"iotdb_connector">>,
        type => iotdb,
        enable => true,
        iotdb_version => ?VSN_1_3_X,
        authentication => #{
            <<"username">> => <<"root">>,
            <<"password">> => <<"******">>
        },
        base_url => <<"http://iotdb.local:18080/">>,
        connect_timeout => <<"15s">>,
        pool_type => <<"random">>,
        pool_size => 8,
        enable_pipelining => 100,
        ssl => #{enable => false}
    }.

%%-------------------------------------------------------------------------------------
%% schema
%%-------------------------------------------------------------------------------------
namespace() -> "iotdb".

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    proplists_without(
        [url, request, retry_interval, headers],
        emqx_bridge_http_schema:fields("config_connector")
    ) ++
        fields("connection_fields");
fields("connection_fields") ->
    [
        {base_url,
            mk(
                emqx_schema:url(),
                #{
                    required => true,
                    desc => ?DESC(emqx_bridge_iotdb, "config_base_url")
                }
            )},
        {iotdb_version,
            mk(
                hoconsc:enum([?VSN_1_3_X, ?VSN_1_1_X, ?VSN_1_0_X, ?VSN_0_13_X]),
                #{
                    desc => ?DESC(emqx_bridge_iotdb, "config_iotdb_version"),
                    default => ?VSN_1_3_X
                }
            )},
        {authentication,
            mk(
                hoconsc:union([ref(?MODULE, auth_basic)]),
                #{
                    default => auth_basic, desc => ?DESC("config_authentication")
                }
            )}
    ];
fields(auth_basic) ->
    [
        {username, mk(binary(), #{required => true, desc => ?DESC("config_auth_basic_username")})},
        {password,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("config_auth_basic_password")
            })}
    ];
fields("post") ->
    emqx_connector_schema:type_and_name_fields(enum([iotdb])) ++ fields(config);
fields("put") ->
    fields(config);
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc(config) ->
    ?DESC("desc_config");
desc(auth_basic) ->
    "Basic Authentication";
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for IoTDB using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

connector_config(Conf, #{name := Name, parse_confs := ParseConfs}) ->
    #{
        base_url := BaseUrl,
        authentication :=
            #{
                username := Username,
                password := Password0
            }
    } = Conf,

    Password = emqx_secret:unwrap(Password0),
    BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),

    WebhookConfig =
        Conf#{
            url => BaseUrl,
            headers => [
                {<<"Content-type">>, <<"application/json">>},
                {<<"Authorization">>, BasicToken}
            ]
        },
    ParseConfs(
        <<"http">>,
        Name,
        WebhookConfig
    ).

proplists_without(Keys, List) ->
    [El || El = {K, _} <- List, not lists:member(K, Keys)].

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------
callback_mode() -> async_if_possible.

-spec on_start(manager_id(), config()) -> {ok, state()} | no_return().
on_start(InstanceId, #{iotdb_version := Version} = Config) ->
    %% [FIXME] The configuration passed in here is pre-processed and transformed
    %% in emqx_bridge_resource:parse_confs/2.
    case emqx_bridge_http_connector:on_start(InstanceId, Config) of
        {ok, State} ->
            ?SLOG(info, #{
                msg => "iotdb_bridge_started",
                instance_id => InstanceId,
                request => emqx_utils:redact(maps:get(request, State, <<>>))
            }),
            ?tp(iotdb_bridge_started, #{instance_id => InstanceId}),
            {ok, State#{iotdb_version => Version, channels => #{}}};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_iotdb_bridge",
                instance_id => InstanceId,
                request => emqx_utils:redact(maps:get(request, Config, <<>>)),
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
    Res = emqx_bridge_http_connector:on_stop(InstanceId, State),
    ?tp(iotdb_bridge_stopped, #{instance_id => InstanceId}),
    Res.

-spec on_get_status(manager_id(), state()) ->
    connected | connecting | {disconnected, state(), term()}.
on_get_status(InstanceId, State) ->
    Func = fun(Worker, Timeout) ->
        Request = {?IOTDB_PING_PATH, [], undefined},
        NRequest = emqx_bridge_http_connector:formalize_request(get, Request),
        Result0 = ehttpc:request(Worker, get, NRequest, Timeout),
        case emqx_bridge_http_connector:transform_result(Result0) of
            {ok, 200, _, Body} ->
                case emqx_utils_json:decode(Body) of
                    #{<<"code">> := 200} ->
                        ok;
                    Json ->
                        {error, {unexpected_status, Json}}
                end;
            {error, _} = Error ->
                Error;
            Result ->
                {error, {unexpected_ping_result, Result}}
        end
    end,
    emqx_bridge_http_connector:on_get_status(InstanceId, State, Func).

-spec on_query(manager_id(), {send_message, map()}, state()) ->
    {ok, pos_integer(), [term()], term()}
    | {ok, pos_integer(), [term()]}
    | {error, term()}.
on_query(
    InstanceId,
    {ChannelId, _Message} = Req,
    #{iotdb_version := IoTDBVsn, channels := Channels} = State
) ->
    ?tp(iotdb_bridge_on_query, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),

    case try_render_messages([Req], IoTDBVsn, Channels) of
        {ok, [IoTDBPayload]} ->
            handle_response(
                emqx_bridge_http_connector:on_query(
                    InstanceId, {ChannelId, IoTDBPayload}, State
                )
            );
        Error ->
            Error
    end.

-spec on_query_async(manager_id(), {send_message, map()}, {function(), [term()]}, state()) ->
    {ok, pid()} | {error, empty_request}.
on_query_async(
    InstanceId,
    {ChannelId, _Message} = Req,
    ReplyFunAndArgs0,
    #{iotdb_version := IoTDBVsn, channels := Channels} = State
) ->
    ?tp(iotdb_bridge_on_query_async, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_async_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),
    case try_render_messages([Req], IoTDBVsn, Channels) of
        {ok, [IoTDBPayload]} ->
            ReplyFunAndArgs =
                {
                    fun(Result) ->
                        Response = handle_response(Result),
                        emqx_resource:apply_reply_fun(ReplyFunAndArgs0, Response)
                    end,
                    []
                },
            emqx_bridge_http_connector:on_query_async(
                InstanceId, {ChannelId, IoTDBPayload}, ReplyFunAndArgs, State
            );
        Error ->
            Error
    end.

on_batch_query_async(
    InstId,
    Requests,
    Callback,
    #{iotdb_version := IoTDBVsn, channels := Channels} = State
) ->
    ?tp(iotdb_bridge_on_batch_query_async, #{instance_id => InstId}),
    [{ChannelId, _Message} | _] = Requests,
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_batch_async_called",
        instance_id => InstId,
        send_message => Requests,
        state => emqx_utils:redact(State)
    }),
    case try_render_messages(Requests, IoTDBVsn, Channels) of
        {ok, IoTDBPayloads} ->
            ReplyFunAndArgs =
                {
                    fun(Result) ->
                        Response = handle_response(Result),
                        emqx_resource:apply_reply_fun(Callback, Response)
                    end,
                    []
                },
            lists:map(
                fun(IoTDBPayload) ->
                    emqx_bridge_http_connector:on_query_async(
                        InstId, {ChannelId, IoTDBPayload}, ReplyFunAndArgs, State
                    )
                end,
                IoTDBPayloads
            );
        Error ->
            Error
    end.

on_batch_query(
    InstId,
    [{ChannelId, _Message}] = Requests,
    #{iotdb_version := IoTDBVsn, channels := Channels} = State
) ->
    ?tp(iotdb_bridge_on_batch_query, #{instance_id => InstId}),
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_batch_query_called",
        instance_id => InstId,
        send_message => Requests,
        state => emqx_utils:redact(State)
    }),

    case try_render_messages(Requests, IoTDBVsn, Channels) of
        {ok, IoTDBPayloads} ->
            lists:map(
                fun(IoTDBPayload) ->
                    handle_response(
                        emqx_bridge_http_connector:on_query(
                            InstId, {ChannelId, IoTDBPayload}, State
                        )
                    )
                end,
                IoTDBPayloads
            );
        Error ->
            Error
    end.

on_format_query_result(Result) ->
    emqx_bridge_http_connector:on_format_query_result(Result).

on_add_channel(
    InstanceId,
    #{iotdb_version := Version, channels := Channels} = OldState0,
    ChannelId,
    #{
        parameters := #{data := Data} = Parameter
    }
) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            {error, already_exists};
        _ ->
            %% update HTTP channel
            InsertTabletPathV1 = <<"rest/v1/insertTablet">>,
            InsertTabletPathV2 = <<"rest/v2/insertTablet">>,

            Path =
                case Version of
                    ?VSN_1_1_X -> InsertTabletPathV2;
                    ?VSN_1_3_X -> InsertTabletPathV2;
                    _ -> InsertTabletPathV1
                end,

            HTTPReq = #{
                parameters => Parameter#{
                    path => Path,
                    method => <<"post">>
                }
            },

            {ok, OldState} = emqx_bridge_http_connector:on_add_channel(
                InstanceId, OldState0, ChannelId, HTTPReq
            ),

            %% update IoTDB channel
            DeviceId = maps:get(device_id, Parameter, <<>>),
            Channel = Parameter#{
                device_id => emqx_placeholder:preproc_tmpl(DeviceId),
                data := preproc_data_template(Data)
            },
            Channels2 = Channels#{ChannelId => Channel},
            {ok, OldState#{channels := Channels2}}
    end.

on_remove_channel(InstanceId, #{channels := Channels} = OldState0, ChannelId) ->
    {ok, OldState} = emqx_bridge_http_connector:on_remove_channel(InstanceId, OldState0, ChannelId),
    Channels2 = maps:remove(ChannelId, Channels),
    {ok, OldState#{channels => Channels2}}.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_get_channel_status(InstanceId, _ChannelId, State) ->
    case on_get_status(InstanceId, State) of
        ?status_connected ->
            ?status_connected;
        _ ->
            ?status_disconnected
    end.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

get_payload(#{payload := Payload}) ->
    Payload;
get_payload(#{<<"payload">> := Payload}) ->
    Payload;
get_payload(Payload) ->
    Payload.

parse_payload(ParsedPayload) when is_map(ParsedPayload) ->
    ParsedPayload;
parse_payload(UnparsedPayload) when is_binary(UnparsedPayload) ->
    emqx_utils_json:decode(UnparsedPayload);
parse_payload(UnparsedPayloads) when is_list(UnparsedPayloads) ->
    lists:map(fun parse_payload/1, UnparsedPayloads).

preproc_data_list(DataList) ->
    lists:foldl(
        fun preproc_data/2,
        [],
        DataList
    ).

preproc_data(
    #{
        <<"measurement">> := Measurement,
        <<"data_type">> := DataType,
        <<"value">> := Value
    } = Data,
    Acc
) ->
    [
        #{
            timestamp => maybe_preproc_tmpl(
                maps:get(<<"timestamp">>, Data, <<"now">>)
            ),
            measurement => emqx_placeholder:preproc_tmpl(Measurement),
            data_type => emqx_placeholder:preproc_tmpl(DataType),
            value => maybe_preproc_tmpl(Value)
        }
        | Acc
    ];
preproc_data(_NoMatch, Acc) ->
    ?SLOG(
        warning,
        #{
            msg => "iotdb_bridge_preproc_data_failed",
            required_fields => ['measurement', 'data_type', 'value'],
            received => _NoMatch
        }
    ),
    Acc.

maybe_preproc_tmpl(Value) when is_binary(Value) ->
    emqx_placeholder:preproc_tmpl(Value);
maybe_preproc_tmpl(Value) ->
    Value.

proc_data(PreProcessedData, Msg, IoTDBVsn) ->
    NowNS = erlang:system_time(nanosecond),
    Nows = #{
        now_ms => erlang:convert_time_unit(NowNS, nanosecond, millisecond),
        now_us => erlang:convert_time_unit(NowNS, nanosecond, microsecond),
        now_ns => NowNS
    },
    proc_data(PreProcessedData, Msg, Nows, IoTDBVsn, []).

proc_data(
    [
        #{
            timestamp := TimestampTkn,
            measurement := Measurement,
            data_type := DataType0,
            value := ValueTkn
        }
        | T
    ],
    Msg,
    Nows,
    IotDbVsn,
    Acc
) ->
    DataType = list_to_binary(
        string:uppercase(binary_to_list(emqx_placeholder:proc_tmpl(DataType0, Msg)))
    ),
    try
        proc_data(T, Msg, Nows, IotDbVsn, [
            #{
                timestamp => iot_timestamp(IotDbVsn, TimestampTkn, Msg, Nows),
                measurement => emqx_placeholder:proc_tmpl(Measurement, Msg),
                data_type => DataType,
                value => proc_value(DataType, ValueTkn, Msg)
            }
            | Acc
        ])
    catch
        throw:Reason ->
            {error, Reason};
        Error:Reason:Stacktrace ->
            ?SLOG(debug, #{exception => Error, reason => Reason, stacktrace => Stacktrace}),
            {error, invalid_data}
    end;
proc_data([], _Msg, _Nows, _IotDbVsn, Acc) ->
    {ok, lists:reverse(Acc)}.

iot_timestamp(_IotDbVsn, Timestamp, _, _) when is_integer(Timestamp) ->
    Timestamp;
iot_timestamp(IotDbVsn, TimestampTkn, Msg, Nows) ->
    iot_timestamp(IotDbVsn, emqx_placeholder:proc_tmpl(TimestampTkn, Msg), Nows).

%% > v1.3.0 don't allow write nanoseconds nor microseconds
iot_timestamp(?VSN_1_3_X, <<"now_us">>, #{now_ms := NowMs}) ->
    NowMs;
iot_timestamp(?VSN_1_3_X, <<"now_ns">>, #{now_ms := NowMs}) ->
    NowMs;
iot_timestamp(_IotDbVsn, <<"now_us">>, #{now_us := NowUs}) ->
    NowUs;
iot_timestamp(_IotDbVsn, <<"now_ns">>, #{now_ns := NowNs}) ->
    NowNs;
iot_timestamp(_IotDbVsn, Timestamp, #{now_ms := NowMs}) when
    Timestamp =:= <<"now">>; Timestamp =:= <<"now_ms">>; Timestamp =:= <<>>
->
    NowMs;
iot_timestamp(_IotDbVsn, Timestamp, _) when is_binary(Timestamp) ->
    binary_to_integer(Timestamp).

proc_value(<<"TEXT">>, ValueTkn, Msg) ->
    case emqx_placeholder:proc_tmpl(ValueTkn, Msg) of
        <<"undefined">> -> null;
        Val -> Val
    end;
proc_value(<<"BOOLEAN">>, ValueTkn, Msg) ->
    convert_bool(replace_var(ValueTkn, Msg));
proc_value(Int, ValueTkn, Msg) when Int =:= <<"INT32">>; Int =:= <<"INT64">> ->
    convert_int(replace_var(ValueTkn, Msg));
proc_value(Int, ValueTkn, Msg) when Int =:= <<"FLOAT">>; Int =:= <<"DOUBLE">> ->
    convert_float(replace_var(ValueTkn, Msg));
proc_value(Type, _, _) ->
    throw(#{reason => invalid_type, type => Type}).

replace_var(Tokens, Data) when is_list(Tokens) ->
    [Val] = emqx_placeholder:proc_tmpl(Tokens, Data, #{return => rawlist}),
    Val;
replace_var(Val, _Data) ->
    Val.

convert_bool(B) when is_boolean(B) -> B;
convert_bool(null) -> null;
convert_bool(undefined) -> null;
convert_bool(1) -> true;
convert_bool(0) -> false;
convert_bool(<<"1">>) -> true;
convert_bool(<<"0">>) -> false;
convert_bool(<<"true">>) -> true;
convert_bool(<<"True">>) -> true;
convert_bool(<<"TRUE">>) -> true;
convert_bool(<<"false">>) -> false;
convert_bool(<<"False">>) -> false;
convert_bool(<<"FALSE">>) -> false.

convert_int(Int) when is_integer(Int) -> Int;
convert_int(Float) when is_float(Float) -> floor(Float);
convert_int(Str) when is_binary(Str) ->
    try
        binary_to_integer(Str)
    catch
        _:_ ->
            convert_int(binary_to_float(Str))
    end;
convert_int(null) ->
    null;
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
convert_float(null) ->
    null;
convert_float(undefined) ->
    null.

make_iotdb_insert_request(DataList, IsAligned, DeviceId, IoTDBVsn) ->
    InitAcc = #{timestamps => [], measurements => [], dtypes => [], values => []},
    Rows = replace_dtypes(aggregate_rows(DataList, InitAcc), IoTDBVsn),
    maps:merge(Rows, #{
        iotdb_field_key(is_aligned, IoTDBVsn) => IsAligned,
        iotdb_field_key(device_id, IoTDBVsn) => DeviceId
    }).

replace_dtypes(Rows0, IoTDBVsn) ->
    {Types, Rows} = maps:take(dtypes, Rows0),
    Rows#{iotdb_field_key(data_types, IoTDBVsn) => Types}.

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

iotdb_field_key(is_aligned, ?VSN_1_3_X) ->
    <<"is_aligned">>;
iotdb_field_key(is_aligned, ?VSN_1_1_X) ->
    <<"is_aligned">>;
iotdb_field_key(is_aligned, ?VSN_1_0_X) ->
    <<"is_aligned">>;
iotdb_field_key(is_aligned, ?VSN_0_13_X) ->
    <<"isAligned">>;
iotdb_field_key(device_id, ?VSN_1_3_X) ->
    <<"device">>;
iotdb_field_key(device_id, ?VSN_1_1_X) ->
    <<"device">>;
iotdb_field_key(device_id, ?VSN_1_0_X) ->
    <<"device">>;
iotdb_field_key(device_id, ?VSN_0_13_X) ->
    <<"deviceId">>;
iotdb_field_key(data_types, ?VSN_1_3_X) ->
    <<"data_types">>;
iotdb_field_key(data_types, ?VSN_1_1_X) ->
    <<"data_types">>;
iotdb_field_key(data_types, ?VSN_1_0_X) ->
    <<"data_types">>;
iotdb_field_key(data_types, ?VSN_0_13_X) ->
    <<"dataTypes">>.

to_list(List) when is_list(List) -> List;
to_list(Data) -> [Data].

%% If device_id is missing from the channel data, try to find it from the payload
device_id(Message, Payloads, Channel) ->
    case maps:get(device_id, Channel, []) of
        [] ->
            maps:get(<<"device_id">>, hd(Payloads), undefined);
        DeviceIdTkn ->
            emqx_placeholder:proc_tmpl(DeviceIdTkn, Message)
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

preproc_data_template(DataList) ->
    Atom2Bin = fun
        (Atom) when is_atom(Atom) ->
            erlang:atom_to_binary(Atom);
        (Bin) ->
            Bin
    end,
    lists:map(
        fun(
            #{
                timestamp := Timestamp,
                measurement := Measurement,
                data_type := DataType,
                value := Value
            }
        ) ->
            #{
                timestamp => emqx_placeholder:preproc_tmpl(Atom2Bin(Timestamp)),
                measurement => emqx_placeholder:preproc_tmpl(Measurement),
                data_type => emqx_placeholder:preproc_tmpl(Atom2Bin(DataType)),
                value => emqx_placeholder:preproc_tmpl(Value)
            }
        end,
        DataList
    ).

try_render_messages([{ChannelId, _} | _] = Msgs, IoTDBVsn, Channels) ->
    case maps:find(ChannelId, Channels) of
        {ok, Channel} ->
            case do_render_message(Msgs, Channel, IoTDBVsn, #{}) of
                RenderMsgs when is_map(RenderMsgs) ->
                    {ok,
                        lists:map(
                            fun({{DeviceId, IsAligned}, DataList}) ->
                                make_iotdb_insert_request(DataList, IsAligned, DeviceId, IoTDBVsn)
                            end,
                            maps:to_list(RenderMsgs)
                        )};
                Error ->
                    Error
            end;
        _ ->
            {error, {unrecoverable_error, {invalid_channel_id, ChannelId}}}
    end.

do_render_message([], _Channel, _IoTDBVsn, Acc) ->
    Acc;
do_render_message([{_, Msg} | Msgs], Channel, IoTDBVsn, Acc) ->
    case render_channel_message(Channel, IoTDBVsn, Msg) of
        {ok, NewDataList, DeviceId, IsAligned} ->
            Fun = fun(V) -> NewDataList ++ V end,
            Acc1 = maps:update_with({DeviceId, IsAligned}, Fun, NewDataList, Acc),
            do_render_message(Msgs, Channel, IoTDBVsn, Acc1);
        Error ->
            Error
    end.

render_channel_message(#{is_aligned := IsAligned} = Channel, IoTDBVsn, Message) ->
    Payloads = to_list(parse_payload(get_payload(Message))),
    case device_id(Message, Payloads, Channel) of
        undefined ->
            {error, device_id_missing};
        DeviceId ->
            case get_data_template(Channel, Payloads) of
                [] ->
                    {error, invalid_template};
                DataTemplate ->
                    case proc_data(DataTemplate, Message, IoTDBVsn) of
                        {ok, DataList} ->
                            {ok, DataList, DeviceId, IsAligned};
                        Error ->
                            Error
                    end
            end
    end.

%% Get the message template.
%% In order to be compatible with 4.4, the template version has higher priority
%% This is a template, using it
get_data_template(#{data := Data}, _Payloads) when Data =/= [] ->
    Data;
%% This is a self-describing message
get_data_template(#{data := []}, Payloads) ->
    preproc_data_list(Payloads).
