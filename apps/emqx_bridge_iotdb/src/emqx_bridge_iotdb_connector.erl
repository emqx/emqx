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
        {sql_dialect,
            mk(
                hoconsc:union([tree, ref(?MODULE, sql_dialect_table)]),
                #{
                    desc => ?DESC(emqx_bridge_iotdb, "config_sql_dialect"),
                    default => tree
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
fields(sql_dialect_table) ->
    [
        {database,
            mk(
                binary(),
                #{
                    desc => ?DESC(emqx_bridge_iotdb, "config_database"),
                    required => false
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
            {sql_dialect,
                mk(
                    hoconsc:union([tree, ref(?MODULE, sql_dialect_table)]),
                    #{
                        desc => ?DESC(emqx_bridge_iotdb, "config_sql_dialect"),
                        default => tree
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
desc(sql_dialect_table) ->
    ?DESC(emqx_bridge_iotdb, "config_sql_dialect");
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
    State0 = init_connector_state(Config),
    case emqx_bridge_http_connector:on_start(InstanceId, Config) of
        {ok, State1} ->
            ?SLOG(info, #{
                msg => "iotdb_bridge_started",
                instance_id => InstanceId,
                request => emqx_utils:redact(maps:get(request, State1, <<>>))
            }),
            ?tp(iotdb_bridge_started, #{driver => restapi, instance_id => InstanceId}),
            State2 = maps:merge(State0, State1),
            {ok, State2#{driver => restapi, iotdb_version => Version, channels => #{}}};
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
    State0 = init_connector_state(Config),

    IoTDBOpts0 = maps:with(['zoneId', username, password], Config),
    IoTDBOpts1 = maps:merge(IoTDBOpts0, State0),

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

    IoTDBOpts = IoTDBOpts1#{
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

            {ok, State0#{
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
            throw({failed_to_start_iotdb_bridge, Reason})
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
on_get_status(ConnResId, #{driver := restapi} = ConnState) ->
    maybe
        ok ?= check_auth_restapi(ConnResId, ConnState),
        check_ping_restapi(ConnResId, ConnState)
    end;
on_get_status(ConnResId, #{driver := thrift} = _ConnState) ->
    Opts = #{check_fn => fun ?MODULE:do_get_status/1},
    emqx_resource_pool:common_health_check_workers(ConnResId, Opts).

check_ping_restapi(ConnResId, ConnState) ->
    Fn = fun(Worker, Timeout) ->
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
    emqx_bridge_http_connector:on_get_status(ConnResId, ConnState, Fn).

check_auth_restapi(ConnResId, ConnState) ->
    #{request := #{headers := Headers0}} = ConnState,
    Headers = emqx_bridge_http_connector:render_headers(Headers0),
    Req =
        {post,
            {<<"rest/v2/nonQuery">>, Headers,
                emqx_utils_json:encode(#{sql => <<"show databases">>})}},
    Res = emqx_bridge_http_connector:on_query(ConnResId, Req, ConnState),
    case emqx_bridge_http_connector:transform_result(Res) of
        {ok, 200, _, _} ->
            ok;
        {ok, 200, _} ->
            ok;
        {error, {_IsRecoverable, #{status_code := 401, body := RespBody0}}} ->
            RespBody =
                case emqx_utils_json:safe_decode(RespBody0) of
                    {ok, RespBody1} -> RespBody1;
                    {error, _} -> RespBody0
                end,
            {?status_disconnected, RespBody};
        {error, {_IsRecoverable, Reason}} ->
            {?status_disconnected, Reason};
        {error, Reason} ->
            {?status_disconnected, Reason};
        Error ->
            {?status_disconnected, Error}
    end.

do_get_status(Conn) ->
    case iotdb:ping(Conn) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
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
        {ok, WriteToTable, Records} ->
            handle_response(
                do_on_query(InstanceId, ChannelId, WriteToTable, Records, State)
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
        {ok, _WriteToTable, Records} ->
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
        {ok, _WriteToTable, Records} ->
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
        {ok, WriteToTable, Records} ->
            handle_response(
                do_on_query(InstId, ChannelId, WriteToTable, Records, State)
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
    maybe
        WriteToTable = maps:get(write_to_table, Parameter, false),
        SqlDialect = maps:get(sql_dialect, OldState0, tree),
        DeviceId =
            case WriteToTable of
                true ->
                    maps:get(table, Parameter, <<>>);
                false ->
                    maps:get(device_id, Parameter, <<>>)
            end,
        ok ?= check_channel_exists(ChannelId, Channels),
        ok ?= check_write_to_table(WriteToTable, SqlDialect),
        ok ?= check_restapi_version(Version),
        {ok, DeviceIdTemplate} ?= preproc_device_id(WriteToTable, DeviceId),
        {ok, DataTemplate} ?= preproc_data_template(WriteToTable, Data),
        Path =
            case SqlDialect of
                tree ->
                    <<"rest/v2/insertRecords">>;
                table ->
                    <<"rest/table/v1/insertTablet">>
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
        Channel = Parameter#{
            device_id => DeviceIdTemplate,
            data := DataTemplate
        },
        Channels2 = Channels#{ChannelId => Channel},
        {ok, OldState#{channels := Channels2}}
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
    maybe
        SqlDialect = maps:get(sql_dialect, OldState, tree),
        WriteToTable = maps:get(write_to_table, Parameter, false),
        ok ?= check_channel_exists(ChannelId, Channels),
        ok ?= check_write_to_table(WriteToTable, SqlDialect),
        DeviceId =
            case WriteToTable of
                true ->
                    maps:get(table, Parameter, <<>>);
                false ->
                    maps:get(device_id, Parameter, <<>>)
            end,
        {ok, DeviceIdTemplate} ?= preproc_device_id(WriteToTable, DeviceId),
        {ok, DataTemplate} ?= preproc_data_template(WriteToTable, Data),
        %% update IoTDB channel
        Channel = Parameter#{
            device_id => DeviceIdTemplate,
            data := DataTemplate
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

preproc_device_id(_WriteToTable = false, DeviceId) ->
    {ok, emqx_placeholder:preproc_tmpl(DeviceId)};
preproc_device_id(_WriteToTable = true, <<>>) ->
    throw(<<"Table name cannot be empty in table model">>);
preproc_device_id(_WriteToTable = true, DeviceId) ->
    HasVar = lists:any(
        fun
            ({var, _}) -> true;
            (_) -> false
        end,
        emqx_placeholder:preproc_tmpl(DeviceId)
    ),
    case HasVar of
        true ->
            throw(<<"Table name cannot contain variables in table model">>);
        false ->
            {ok, DeviceId}
    end.

preproc_data_template(WriteToTable, DataList) ->
    try
        Templates = preproc_data_template(WriteToTable, DataList, []),
        {ok, Templates}
    catch
        throw:Reason ->
            {error, Reason}
    end.

preproc_data_template(_, [], Acc) ->
    lists:reverse(Acc);
preproc_data_template(
    WriteToTable = false,
    [
        #{
            timestamp := Timestamp,
            measurement := Measurement,
            data_type := DataType,
            value := Value
        }
        | T
    ],
    Acc
) ->
    Template = #{
        timestamp => emqx_placeholder:preproc_tmpl(to_bin(Timestamp)),
        measurement => emqx_placeholder:preproc_tmpl(Measurement),
        data_type => string:uppercase(to_bin(DataType)),
        value => emqx_placeholder:preproc_tmpl(Value)
    },
    preproc_data_template(WriteToTable, T, [Template | Acc]);
preproc_data_template(
    WriteToTable = true,
    [
        #{
            timestamp := Timestamp,
            measurement := Measurement,
            data_type := DataType,
            column_category := ColumnCategory,
            value := Value
        }
        | T
    ],
    Acc
) ->
    Template = #{
        timestamp => emqx_placeholder:preproc_tmpl(to_bin(Timestamp)),
        measurement => emqx_placeholder:preproc_tmpl(Measurement),
        data_type => string:uppercase(to_bin(DataType)),
        column_category => ColumnCategory,
        value => emqx_placeholder:preproc_tmpl(Value)
    },
    preproc_data_template(WriteToTable, T, [Template | Acc]);
preproc_data_template(_, [_Data | _], _) ->
    throw(<<"Invalid data template">>).

do_on_query(InstanceId, ChannelId, _WriteToTable, Data, #{driver := restapi} = State) ->
    %% HTTP connector already calls `emqx_trace:rendered_action_template`.
    emqx_bridge_http_connector:on_query(InstanceId, {ChannelId, Data}, State);
do_on_query(InstanceId, ChannelId, _WriteToTable = false, Data, #{driver := thrift} = _State) ->
    emqx_trace:rendered_action_template(ChannelId, #{records => Data}),
    ecpool:pick_and_do(InstanceId, {iotdb, insert_records, [Data]}, no_handover);
do_on_query(InstanceId, ChannelId, _WriteToTable = true, Data, #{driver := thrift} = _State) ->
    emqx_trace:rendered_action_template(ChannelId, #{records => Data}),
    ecpool:pick_and_do(InstanceId, {iotdb, insert_tablet, [Data]}, no_handover).

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

try_render_records([{ChannelId, _} | _] = Msgs, #{driver := Driver, channels := Channels} = State) ->
    case maps:find(ChannelId, Channels) of
        {ok, Channel} ->
            Database = maps:get(database, State, <<>>),
            WriteToTable = maps:get(write_to_table, Channel, false),
            InitialAcc = init_render_acc(Driver, WriteToTable, Database, Channel),
            case do_render_record(Msgs, Channel, Driver, InitialAcc) of
                {ok, Acc} ->
                    case WriteToTable of
                        true ->
                            %% Due to the measurements are variables,
                            %% we need to validate the consistency in multiple records.
                            Acc1 = validate_measurements_consistency(Acc),
                            {ok, WriteToTable, Acc1};
                        false ->
                            {ok, WriteToTable, Acc}
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, {unrecoverable_error, {invalid_channel_id, ChannelId}}}
    end.

validate_measurements_consistency(#{measurements := [Used | More]} = Acc) ->
    do_validate_measurements_consistency(Used, More),
    Acc#{measurements => Used};
validate_measurements_consistency(#{column_names := [Used | More]} = Acc) ->
    do_validate_measurements_consistency(Used, More),
    Acc#{column_names => Used}.

do_validate_measurements_consistency(Used, More) ->
    case lists:all(fun(M) -> M =:= Used end, More) of
        false ->
            ?SLOG(warning, #{
                msg => "ignore_inconsistent_column_names_in_batch",
                hint => "Use the first record's column names to insert into the table",
                ignored => More,
                used => Used
            });
        true ->
            ok
    end.

init_render_acc(Driver, _WriteToTable = false, _Database, Channel) ->
    IsAligned = maps:get(is_aligned, Channel, false),
    #{
        timestamps => [],
        measurements_list => [],
        data_types_list => [],
        values_list => [],
        devices => [],
        is_aligned_name(Driver) => IsAligned
    };
init_render_acc(Driver = restapi, _WriteToTable = true, Database, Channel) ->
    IsAligned = maps:get(is_aligned, Channel, false),
    ColumnCategories = encode_column_categories(Driver, maps:get(data, Channel)),
    DataTypes = encode_data_types(maps:get(data, Channel)),
    #{
        database => Database,
        table => maps:get(device_id, Channel),
        column_names => [],
        column_catogories => ColumnCategories,
        data_types => DataTypes,
        timestamps => [],
        values => [],
        is_aligned => IsAligned
    };
init_render_acc(Driver = thrift, _WriteToTable = true, _Database, Channel) ->
    IsAligned = maps:get(is_aligned, Channel, false),
    ColumnCategories = encode_column_categories(Driver, maps:get(data, Channel)),
    DataTypes = encode_data_types(maps:get(data, Channel)),
    #{
        'deviceId' => maps:get(device_id, Channel),
        measurements => [],
        'columnCategories' => ColumnCategories,
        dtypes => DataTypes,
        timestamps => [],
        values => lists:duplicate(length(DataTypes), []),
        'isAligned' => IsAligned,
        'writeToTable' => true
    }.

append_record(
    _Driver,
    _WriteToTable = false,
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
    };
append_record(
    _Driver = restapi,
    _WriteToTable = true,
    #{
        timestamp := Ts,
        measurements := Measurements,
        values := Vals
    },
    #{
        timestamps := TsL,
        column_names := ColumnNamesL,
        values := ValL
    } = Records
) ->
    Records#{
        timestamps := [Ts | TsL],
        column_names := [Measurements | ColumnNamesL],
        values := [Vals | ValL]
    };
append_record(
    _Driver = thrift,
    _WriteToTable = true,
    #{
        timestamp := Ts,
        measurements := Measurements,
        values := Vals
    },
    #{
        timestamps := TsL,
        measurements := MeasurementsL,
        values := ValsL
    } = Records
) ->
    Records#{
        timestamps := [Ts | TsL],
        measurements := [Measurements | MeasurementsL],
        values := append_value(Vals, ValsL)
    }.

append_value(Vals, ValsL) ->
    LengthVals = length(Vals),
    LengthValsL = length(ValsL),
    case LengthVals =:= LengthValsL of
        true ->
            lists:zipwith(fun(Val, ValL) -> lists:reverse([Val | ValL]) end, Vals, ValsL);
        false ->
            throw(<<"The values are not consistent in the batch">>)
    end.

is_aligned_name(restapi) ->
    is_aligned;
is_aligned_name(thrift) ->
    'isAligned'.

do_render_record([], _Channel, _Driver, Acc) ->
    {ok, Acc};
do_render_record([{_, Msg} | Msgs], Channel, Driver, Acc) ->
    case render_channel_record(Channel, Msg) of
        {ok, Record} ->
            WriteToTable = maps:get(write_to_table, Channel, false),
            NewAcc = append_record(Driver, WriteToTable, Record, Acc),
            do_render_record(Msgs, Channel, Driver, NewAcc);
        Error ->
            Error
    end.

render_channel_record(#{data := DataTemplate, write_to_table := false} = Channel, Msg) ->
    maybe
        {ok, Payload} ?= parse_payload(get_payload(Msg)),
        DeviceId = device_id(Msg, Payload, Channel),
        true ?= (<<"undefined">> =/= DeviceId),
        #{timestamp := TimestampTkn} = hd(DataTemplate),
        NowNs = erlang:system_time(nanosecond),
        Nows = #{
            now_ms => erlang:convert_time_unit(NowNs, nanosecond, millisecond),
            now_us => erlang:convert_time_unit(NowNs, nanosecond, microsecond),
            now_ns => NowNs
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
    end;
render_channel_record(#{data := DataTemplate, write_to_table := true} = _Channel, Msg) ->
    maybe
        #{timestamp := TimestampTkn} = hd(DataTemplate),
        NowNs = erlang:system_time(nanosecond),
        Nows = #{
            now_ms => erlang:convert_time_unit(NowNs, nanosecond, millisecond),
            now_us => erlang:convert_time_unit(NowNs, nanosecond, microsecond),
            now_ns => NowNs
        },
        {ok, MeasurementAcc, ValueAcc} ?=
            proc_record_data_for_table(
                DataTemplate,
                Msg,
                [],
                []
            ),
        {ok, Timestamp} ?= iot_timestamp(TimestampTkn, Msg, Nows),
        {ok, #{
            timestamp => Timestamp,
            measurements => MeasurementAcc,
            values => ValueAcc
        }}
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
    {ok, lists:reverse(MeasurementAcc), lists:reverse(TypeAcc), lists:reverse(ValueAcc)}.

proc_record_data_for_table(
    [
        #{
            data_type := DataType,
            measurement := Measurement,
            value := ValueTkn
        }
        | T
    ],
    Msg,
    MeasurementAcc,
    ValueAcc
) ->
    try
        proc_record_data_for_table(
            T,
            Msg,
            [emqx_placeholder:proc_tmpl(Measurement, Msg) | MeasurementAcc],
            [proc_value(DataType, ValueTkn, Msg) | ValueAcc]
        )
    catch
        throw:Reason ->
            {error, Reason};
        Error:Reason ->
            ?SLOG(debug, #{exception => Error, reason => Reason}),
            {error, {invalid_data, Reason}}
    end;
proc_record_data_for_table([], _Msg, MeasurementAcc, ValueAcc) ->
    {ok, lists:reverse(MeasurementAcc), lists:reverse(ValueAcc)}.

init_connector_state(Config) ->
    case parse_sql_dialect(maps:get(sql_dialect, Config, tree)) of
        {table, <<>>} ->
            throw({failed_to_start_iotdb_bridge, "database is required when sql_dialect is table"});
        {table, Database} when is_binary(Database) ->
            #{sql_dialect => table, database => Database};
        {tree, _} ->
            #{sql_dialect => tree}
    end.

parse_sql_dialect(#{database := Database}) ->
    {table, Database};
parse_sql_dialect(SqlDialect) when SqlDialect =:= tree orelse SqlDialect =:= undefined ->
    {tree, <<>>}.

to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom);
to_bin(Bin) when is_binary(Bin) ->
    Bin.

check_channel_exists(ChannelId, Channels) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            {error, already_exists};
        false ->
            ok
    end.

check_write_to_table(_WriteToTable = true, _SqlDialect = table) ->
    ok;
check_write_to_table(_WriteToTable = false, _SqlDialect = tree) ->
    ok;
check_write_to_table(WriteToTable, SqlDialect) ->
    ErrMsg = <<
        "The write_to_table=",
        (to_bin(WriteToTable))/binary,
        " in action parameters is not supported in this SQL dialect: ",
        (to_bin(SqlDialect))/binary
    >>,
    {error, ErrMsg}.

check_restapi_version(_Version = ?VSN_1_3_X) ->
    ok;
check_restapi_version(_Version) ->
    {error, <<"REST API only supports IoTDB 1.3.x and later">>}.

encode_column_categories(Driver, DataTemplate) when is_list(DataTemplate) ->
    lists:map(
        fun(#{column_category := ColumnCategory}) ->
            encode_column_category(ColumnCategory, Driver)
        end,
        DataTemplate
    ).

encode_column_category(tag, _Driver = thrift) ->
    0;
encode_column_category(field, _Driver = thrift) ->
    1;
encode_column_category(attribute, _Driver = thrift) ->
    2;
encode_column_category(tag, _Driver = restapi) ->
    <<"TAG">>;
encode_column_category(field, _Driver = restapi) ->
    <<"FIELD">>;
encode_column_category(attribute, _Driver = restapi) ->
    <<"ATTRIBUTE">>.

encode_data_types(DataTemplate) when is_list(DataTemplate) ->
    lists:map(
        fun(#{data_type := DataType}) ->
            string:uppercase(to_bin(DataType))
        end,
        DataTemplate
    ).
