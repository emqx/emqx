%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_connector).

-feature(maybe_expr, enable).

-behaviour(emqx_connector_examples).

-behaviour(emqx_resource).

-include("emqx_bridge_iotdb.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    callback_mode/1,
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

-export([connect/1, do_get_status/1]).

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
        driver := driver(),
        request_base => #{
            scheme := http | https,
            host := iolist(),
            port := inet:port_number()
        },
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        pool_size := pos_integer(),
        iotdb_version => atom(),
        protocol_version => atom(),
        request => undefined | map(),
        atom() => _
    }.

-type state() ::
    #{
        driver := driver(),
        channels := map(),
        iotdb_version := atom(),
        atom() => _
    }.

-type manager_id() :: binary().

-define(CONNECTOR_TYPE, iotdb).
-define(IOTDB_PING_PATH, <<"ping">>).

%% timer:seconds(10)).
-define(DEFAULT_THRIFT_TIMEOUT, 10000).

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
    [].

fields("config_restapi") ->
    proplists_without(
        [url, request, retry_interval, headers],
        emqx_bridge_http_schema:fields("config_connector")
    ) ++ common_fields(restapi) ++
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
                hoconsc:union([ref(?MODULE, authentication)]),
                #{
                    default => auth_basic, desc => ?DESC("config_authentication")
                }
            )}
    ];
fields(authentication) ->
    [
        {username, mk(binary(), #{required => true, desc => ?DESC("config_auth_username")})},
        {password,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("config_auth_password")
            })}
    ];
fields("config_thrift") ->
    Meta = #{desc => ?DESC("server")},
    emqx_connector_schema:common_fields() ++
        common_fields(thrift) ++
        [
            {server, emqx_schema:servers_sc(Meta, ?THRIFT_HOST_OPTIONS)},
            {protocol_version,
                mk(
                    hoconsc:enum([?PROTOCOL_V1, ?PROTOCOL_V2, ?PROTOCOL_V3]),
                    #{
                        desc => ?DESC("config_protocol_version"),
                        default => ?PROTOCOL_V3
                    }
                )},
            {'zoneId',
                mk(
                    binary(),
                    #{default => <<"Asia/Shanghai">>, desc => ?DESC("config_zoneId")}
                )},
            {pool_size,
                mk(
                    pos_integer(),
                    #{
                        default => 8,
                        desc => ?DESC("pool_size")
                    }
                )},
            {connect_timeout,
                mk(
                    emqx_schema:timeout_duration_ms(),
                    #{
                        default => <<"10s">>,
                        desc => ?DESC("connect_timeout")
                    }
                )},
            {recv_timeout,
                mk(
                    emqx_schema:timeout_duration_ms(),
                    #{
                        default => <<"10s">>,
                        desc => ?DESC("recv_timeout")
                    }
                )}
        ] ++ fields(authentication) ++ emqx_connector_schema_lib:ssl_fields() ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("post_" ++ Driver) ->
    emqx_connector_schema:type_and_name_fields(enum([iotdb])) ++ fields("config_" ++ Driver);
fields("put_" ++ Driver) ->
    fields("config_" ++ Driver);
fields("get_" ++ Driver) ->
    emqx_bridge_schema:status_fields() ++ fields("post_" ++ Driver).

common_fields(Driver) ->
    [
        {driver,
            mk(
                hoconsc:enum([Driver]),
                #{
                    desc => ?DESC("config_driver"),
                    default => <<"restapi">>
                }
            )}
    ].

desc(authentication) ->
    ?DESC("config_authentication");
desc(connector_resource_opts) ->
    "Connector resource options";
desc(Struct) when is_list(Struct) ->
    case string:split(Struct, "_") of
        ["config", _] ->
            ?DESC("desc_config");
        [Method, _] when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
            ["Configuration for IoTDB using `", string:to_upper(Method), "` method."];
        _ ->
            undefined
    end;
desc(_) ->
    undefined.

connector_config(#{driver := restapi} = Conf, #{name := Name, parse_confs := ParseConfs}) ->
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
    );
connector_config(Conf, _) ->
    Conf.

proplists_without(Keys, List) ->
    [El || El = {K, _} <- List, not lists:member(K, Keys)].

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------
resource_type() -> iotdb.

callback_mode() -> async_if_possible.

callback_mode(#{driver := restapi}) ->
    async_if_possible;
callback_mode(#{driver := thrift}) ->
    always_sync.

-spec on_start(manager_id(), config()) -> {ok, state()} | no_return().
on_start(InstanceId, #{driver := restapi, iotdb_version := Version} = Config) ->
    %% [FIXME] The configuration passed in here is pre-processed and transformed
    %% in emqx_bridge_resource:parse_confs/2.
    case emqx_bridge_http_connector:on_start(InstanceId, Config) of
        {ok, State} ->
            ?SLOG(info, #{
                msg => "iotdb_bridge_started",
                instance_id => InstanceId,
                request => emqx_utils:redact(maps:get(request, State, <<>>))
            }),
            ?tp(iotdb_bridge_started, #{driver => restapi, instance_id => InstanceId}),
            {ok, State#{driver => restapi, iotdb_version => Version, channels => #{}}};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_iotdb_bridge",
                instance_id => InstanceId,
                request => emqx_utils:redact(maps:get(request, Config, <<>>)),
                reason => Reason
            }),
            throw(failed_to_start_iotdb_bridge)
    end;
on_start(
    InstanceId,
    #{
        driver := thrift,
        protocol_version := ProtocolVsn,
        server := Server,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    IoTDBOpts0 = maps:with(['zoneId', username, password], Config),

    Version =
        case ProtocolVsn of
            ?PROTOCOL_V1 ->
                0;
            ?PROTOCOL_V2 ->
                1;
            ?PROTOCOL_V3 ->
                2
        end,

    Addresses = emqx_schema:parse_servers(Server, ?THRIFT_HOST_OPTIONS),

    DriverOpts = maps:merge(
        #{
            connect_timeout => ?DEFAULT_THRIFT_TIMEOUT,
            recv_timeout => ?DEFAULT_THRIFT_TIMEOUT
        },
        normalize_thrift_timeout(maps:with([connect_timeout, recv_timeout], Config))
    ),

    DriverOpts1 =
        case maps:get(enable, SSL) of
            true ->
                DriverOpts#{
                    ssltransport => true,
                    ssloptions => emqx_tls_lib:to_client_opts(SSL)
                };
            false ->
                DriverOpts
        end,

    IoTDBOpts = IoTDBOpts0#{
        version => Version,
        addresses => Addresses,
        options => DriverOpts1
    },

    Options = [
        {pool_size, PoolSize},
        {iotdb_options, IoTDBOpts}
    ],

    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            ?SLOG(info, #{
                msg => "iotdb_bridge_started",
                instance_id => InstanceId
            }),

            ?tp(iotdb_bridge_started, #{driver => thrift, instance_id => InstanceId}),

            {ok, #{
                driver => thrift,
                iotdb_version => ProtocolVsn,
                channels => #{}
            }};
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
on_stop(InstanceId, #{driver := restapi} = State) ->
    ?SLOG(info, #{
        msg => "stopping_iotdb_bridge",
        connector => InstanceId
    }),
    Res = emqx_bridge_http_connector:on_stop(InstanceId, State),
    ?tp(iotdb_bridge_stopped, #{instance_id => InstanceId}),
    Res;
on_stop(InstanceId, #{driver := thrift} = _State) ->
    ?SLOG(info, #{
        msg => "stopping_iotdb_bridge",
        connector => InstanceId
    }),

    ?tp(iotdb_bridge_stopped, #{instance_id => InstanceId}),
    emqx_resource_pool:stop(InstanceId).

-spec on_get_status(manager_id(), state()) ->
    connected | connecting | {disconnected, state(), term()}.
on_get_status(InstanceId, #{driver := restapi} = State) ->
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
    emqx_bridge_http_connector:on_get_status(InstanceId, State, Func);
on_get_status(InstanceId, #{driver := thrift} = _State) ->
    case emqx_resource_pool:health_check_workers(InstanceId, fun ?MODULE:do_get_status/1) of
        true ->
            ?status_connected;
        false ->
            ?status_disconnected
    end.

do_get_status(Conn) ->
    case iotdb:ping(Conn) of
        {ok, _} ->
            true;
        {error, _} ->
            false
    end.

connect(Opts) ->
    {iotdb_options, #{password := Password} = IoTDBOpts0} = lists:keyfind(iotdb_options, 1, Opts),
    IoTDBOpts = IoTDBOpts0#{password := emqx_secret:unwrap(Password)},
    iotdb:start_link(IoTDBOpts).

-spec on_query(manager_id(), {send_message, map()}, state()) ->
    {ok, pos_integer(), [term()], term()}
    | {ok, pos_integer(), [term()]}
    | {error, term()}.
on_query(
    InstanceId,
    {ChannelId, _Message} = Req,
    State
) ->
    ?tp(iotdb_bridge_on_query, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),

    case try_render_records([Req], State) of
        {ok, Records} ->
            handle_response(
                do_on_query(InstanceId, ChannelId, Records, State)
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
    #{driver := restapi} = State
) ->
    ?tp(iotdb_bridge_on_query_async, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_async_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),
    case try_render_records([Req], State) of
        {ok, Records} ->
            ReplyFunAndArgs =
                {
                    fun(Result) ->
                        Response = handle_response(Result),
                        emqx_resource:apply_reply_fun(ReplyFunAndArgs0, Response)
                    end,
                    []
                },
            emqx_bridge_http_connector:on_query_async(
                InstanceId, {ChannelId, Records}, ReplyFunAndArgs, State
            );
        Error ->
            Error
    end;
on_query_async(
    InstanceId,
    Req,
    _ReplyFunAndArgs0,
    #{driver := thrift}
) ->
    ?SLOG(error, #{
        msg => "iotdb_bridge_async_query_failed",
        instance_id => InstanceId,
        send_message => Req,
        reason => ?THRIFT_NOT_SUPPORT_ASYNC_MSG
    }),
    {error, not_support}.

on_batch_query_async(
    InstId,
    Requests,
    Callback,
    #{driver := restapi} = State
) ->
    ?tp(iotdb_bridge_on_batch_query_async, #{instance_id => InstId}),
    [{ChannelId, _Message} | _] = Requests,
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_query_batch_async_called",
        instance_id => InstId,
        send_message => Requests,
        state => emqx_utils:redact(State)
    }),
    case try_render_records(Requests, State) of
        {ok, Records} ->
            ReplyFunAndArgs =
                {
                    fun(Result) ->
                        Response = handle_response(Result),
                        emqx_resource:apply_reply_fun(Callback, Response)
                    end,
                    []
                },
            handle_response(
                emqx_bridge_http_connector:on_query_async(
                    InstId, {ChannelId, Records}, ReplyFunAndArgs, State
                )
            );
        Error ->
            Error
    end;
on_batch_query_async(
    InstanceId,
    Req,
    _ReplyFunAndArgs0,
    #{driver := thrift}
) ->
    ?SLOG(error, #{
        msg => "iotdb_bridge_async_query_failed",
        instance_id => InstanceId,
        send_message => Req,
        reason => ?THRIFT_NOT_SUPPORT_ASYNC_MSG
    }),
    {error, not_support}.

on_batch_query(
    InstId,
    [{ChannelId, _Message} | _] = Requests,
    State
) ->
    ?tp(iotdb_bridge_on_batch_query, #{instance_id => InstId}),
    ?SLOG(debug, #{
        msg => "iotdb_bridge_on_batch_query_called",
        instance_id => InstId,
        send_message => Requests,
        state => emqx_utils:redact(State)
    }),

    case try_render_records(Requests, State) of
        {ok, Records} ->
            handle_response(
                do_on_query(InstId, ChannelId, Records, State)
            );
        Error ->
            Error
    end.

on_format_query_result(Result) ->
    emqx_bridge_http_connector:on_format_query_result(Result).

on_add_channel(
    _InstanceId,
    _State0,
    _ChannelId,
    #{
        parameters := #{data := []} = _Parameter
    }
) ->
    {error, <<"The data template cannot be empty">>};
on_add_channel(
    InstanceId,
    #{driver := restapi, iotdb_version := Version, channels := Channels} = OldState0,
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
            case Version of
                ?VSN_1_3_X ->
                    Path = <<"rest/v2/insertRecords">>,
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
                    {ok, OldState#{channels := Channels2}};
                _ ->
                    {error, <<"REST API only supports IoTDB 1.3.x and later">>}
            end
    end;
on_add_channel(
    _InstanceId,
    #{driver := thrift},
    _ChannelId,
    #{
        resource_opts := #{query_mode := async}
    }
) ->
    {error, <<"Thrift does not support async mode">>};
on_add_channel(
    _InstanceId,
    #{driver := thrift, channels := Channels} = OldState,
    ChannelId,
    #{
        parameters := #{data := Data} = Parameter
    }
) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            {error, already_exists};
        _ ->
            %% update IoTDB channel
            DeviceId = maps:get(device_id, Parameter, <<>>),
            Channel = Parameter#{
                device_id => emqx_placeholder:preproc_tmpl(DeviceId),
                %% The template process will reverse the order of the values
                %% so we can reverse the template here to reduce some runtime cost                                 %%
                data := lists:reverse(preproc_data_template(Data))
            },
            Channels2 = Channels#{ChannelId => Channel},
            {ok, OldState#{channels := Channels2}}
    end.

on_remove_channel(InstanceId, #{driver := restapi, channels := Channels} = OldState0, ChannelId) ->
    {ok, OldState} = emqx_bridge_http_connector:on_remove_channel(InstanceId, OldState0, ChannelId),
    Channels2 = maps:remove(ChannelId, Channels),
    {ok, OldState#{channels => Channels2}};
on_remove_channel(_InstanceId, #{driver := thrift, channels := Channels} = OldState, ChannelId) ->
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
    {ok, ParsedPayload};
parse_payload(UnparsedPayload) when is_binary(UnparsedPayload) ->
    try emqx_utils_json:decode(UnparsedPayload) of
        Term when is_map(Term) -> {ok, Term};
        _ ->
            %% a plain text will be returned as it is, but here we hope it is a map
            {error, {invalid_data, <<"The payload is not a JSON data">>}}
    catch
        _:_ ->
            {error, {invalid_data, <<"The payload is not a valid JSON data">>}}
    end.

iot_timestamp(Timestamp, _, _) when is_integer(Timestamp) ->
    {ok, Timestamp};
iot_timestamp(TimestampTkn, Msg, Nows) ->
    iot_timestamp(emqx_placeholder:proc_tmpl(TimestampTkn, Msg), Nows).

%% IoTDB allows us/ms/ns,
%% but an instance only supports one time unit,
%% and the time unit cannot be changed after the database is started.
iot_timestamp(<<"now_us">>, #{now_us := NowUs}) ->
    {ok, NowUs};
iot_timestamp(<<"now_ns">>, #{now_ns := NowNs}) ->
    {ok, NowNs};
iot_timestamp(Timestamp, #{now_ms := NowMs}) when
    Timestamp =:= <<"now">>; Timestamp =:= <<"now_ms">>; Timestamp =:= <<>>
->
    {ok, NowMs};
iot_timestamp(Timestamp, _) when is_binary(Timestamp) ->
    case string:to_integer(Timestamp) of
        {Timestamp1, <<>>} ->
            {ok, Timestamp1};
        _ ->
            {error, {invalid_data, <<"Timestamp is undefined or not a integer">>}}
    end.

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

%% If device_id is missing from the channel data, try to find it from the payload
device_id(Message, Payload, Channel) ->
    case maps:get(device_id, Channel, []) of
        [] ->
            maps:get(<<"device_id">>, Payload, <<"undefined">>);
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
handle_response({ok, _} = Resp) ->
    Resp;
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
                data_type => string:uppercase(Atom2Bin(DataType)),
                value => emqx_placeholder:preproc_tmpl(Value)
            }
        end,
        DataList
    ).

do_on_query(InstanceId, ChannelId, Data, #{driver := restapi} = State) ->
    emqx_bridge_http_connector:on_query(InstanceId, {ChannelId, Data}, State);
do_on_query(InstanceId, _ChannelId, Data, #{driver := thrift} = _State) ->
    ecpool:pick_and_do(InstanceId, {iotdb, insert_records, [Data]}, no_handover).

%% 1. The default timeout in Thrift is `infinity`, but it may cause stuck
%% 2. The schema of `timeout` accepts a zero value, but the Thrift driver not
%% 3. If the timeout is too small, the driver may not work properly
normalize_thrift_timeout(Timeouts) ->
    maps:map(
        fun
            (_K, V) when V >= ?DEFAULT_THRIFT_TIMEOUT ->
                V;
            (K, V) ->
                ?SLOG(warning, #{
                    msg => "iotdb_thrift_timeout_normalized",
                    reason => "The timeout is too small for the Thrift driver to work",
                    timeout => K,
                    from => V,
                    to => ?DEFAULT_THRIFT_TIMEOUT,
                    unit => millisecond
                }),
                ?DEFAULT_THRIFT_TIMEOUT
        end,
        Timeouts
    ).

%%-------------------------------------------------------------------------------------
%% batch
%%-------------------------------------------------------------------------------------
try_render_records([{ChannelId, _} | _] = Msgs, #{driver := Driver, channels := Channels}) ->
    case maps:find(ChannelId, Channels) of
        {ok, #{is_aligned := IsAligned} = Channel} ->
            EmptyRecords = #{
                timestamps => [],
                measurements_list => [],
                data_types_list => [],
                values_list => [],
                devices => [],
                is_aligned_name(Driver) => IsAligned
            },
            do_render_record(Msgs, Channel, EmptyRecords);
        _ ->
            {error, {unrecoverable_error, {invalid_channel_id, ChannelId}}}
    end.

do_render_record([], _Channel, Acc) ->
    {ok, Acc};
do_render_record([{_, Msg} | Msgs], Channel, Acc) ->
    case render_channel_record(Channel, Msg) of
        {ok, Record} ->
            do_render_record(Msgs, Channel, append_record(Record, Acc));
        Error ->
            Error
    end.

render_channel_record(#{data := DataTemplate} = Channel, Msg) ->
    maybe
        {ok, Payload} ?= parse_payload(get_payload(Msg)),
        DeviceId = device_id(Msg, Payload, Channel),
        true ?= (<<"undefined">> =/= DeviceId),
        #{timestamp := TimestampTkn} = hd(DataTemplate),
        NowNS = erlang:system_time(nanosecond),
        Nows = #{
            now_ms => erlang:convert_time_unit(NowNS, nanosecond, millisecond),
            now_us => erlang:convert_time_unit(NowNS, nanosecond, microsecond),
            now_ns => NowNS
        },
        {ok, MeasurementAcc, TypeAcc, ValueAcc} ?=
            proc_record_data(
                DataTemplate,
                Msg,
                [],
                [],
                []
            ),
        {ok, Timestamp} ?= iot_timestamp(TimestampTkn, Msg, Nows),
        {ok, #{
            timestamp => Timestamp,
            measurements => MeasurementAcc,
            data_types => TypeAcc,
            values => ValueAcc,
            device_id => DeviceId
        }}
    else
        false ->
            {error, {invalid_data, <<"Can not find the device ID">>}};
        Error ->
            Error
    end.

proc_record_data(
    [
        #{
            measurement := Measurement,
            data_type := DataType,
            value := ValueTkn
        }
        | T
    ],
    Msg,
    MeasurementAcc,
    TypeAcc,
    ValueAcc
) ->
    try
        proc_record_data(
            T,
            Msg,
            [emqx_placeholder:proc_tmpl(Measurement, Msg) | MeasurementAcc],
            [DataType | TypeAcc],
            [proc_value(DataType, ValueTkn, Msg) | ValueAcc]
        )
    catch
        throw:Reason ->
            {error, Reason};
        Error:Reason ->
            ?SLOG(debug, #{exception => Error, reason => Reason}),
            {error, {invalid_data, Reason}}
    end;
proc_record_data([], _Msg, MeasurementAcc, TypeAcc, ValueAcc) ->
    {ok, MeasurementAcc, TypeAcc, ValueAcc}.

append_record(
    #{
        timestamp := Ts,
        measurements := Measurements,
        data_types := DataTypes,
        values := Vals,
        device_id := DeviceId
    },
    #{
        timestamps := TsL,
        measurements_list := MeasL,
        data_types_list := DtL,
        values_list := ValL,
        devices := DevL
    } = Records
) ->
    Records#{
        timestamps := [Ts | TsL],
        measurements_list := [Measurements | MeasL],
        data_types_list := [DataTypes | DtL],
        values_list := [Vals | ValL],
        devices := [DeviceId | DevL]
    }.

is_aligned_name(restapi) ->
    is_aligned;
is_aligned_name(thrift) ->
    'isAligned'.
