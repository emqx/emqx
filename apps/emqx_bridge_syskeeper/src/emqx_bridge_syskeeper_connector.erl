%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_syskeeper_connector).

-behaviour(emqx_connector_examples).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, desc/1, connector_examples/1]).

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([
    connect/1
]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-define(CONNECTOR_TYPE, syskeeper_forwarder).
-define(SYSKEEPER_HOST_OPTIONS, #{
    default_port => 9092
}).

-define(EXTRA_CALL_TIMEOUT, 2000).

%% -------------------------------------------------------------------------------------------------
%% api
namespace() -> "syskeeper_forwarder".

connector_examples(Method) ->
    [
        #{
            <<"syskeeper_forwarder">> => #{
                summary => <<"Syskeeper Forwarder Connector">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ],
            actions => [<<"my_action">>]
        },
        values(post)
    );
values(post) ->
    maps:merge(
        #{
            name => <<"syskeeper_forwarder">>,
            type => <<"syskeeper_forwarder">>
        },
        values(put)
    );
values(put) ->
    #{
        enable => true,
        server => <<"127.0.0.1:9092">>,
        ack_mode => <<"no_ack">>,
        ack_timeout => <<"10s">>,
        pool_size => 16
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    emqx_connector_schema:common_fields() ++
        fields("connection_fields") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields("connection_fields") ->
    [
        {server, server()},
        {ack_mode,
            mk(
                enum([need_ack, no_ack]),
                #{desc => ?DESC(ack_mode), default => <<"no_ack">>}
            )},
        {ack_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{desc => ?DESC(ack_timeout), default => <<"10s">>}
            )},
        {pool_size, fun
            (default) ->
                16;
            (Other) ->
                emqx_connector_schema_lib:pool_size(Other)
        end}
    ];
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get";
    Field == "post";
    Field == "put"
->
    Fields =
        fields("connection_fields") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(
        Field ++ "_connector", ?CONNECTOR_TYPE, Fields
    ).

desc(config) ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Syskeeper Proxy using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?SYSKEEPER_HOST_OPTIONS).

%% -------------------------------------------------------------------------------------------------
%% `emqx_resource' API
resource_type() -> syskeeper.

callback_mode() -> always_sync.

query_mode(_) -> sync.

on_start(
    InstanceId,
    #{
        server := Server,
        pool_size := PoolSize,
        ack_timeout := AckTimeout
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_syskeeper_connector",
        connector => InstanceId,
        config => Config
    }),

    HostCfg = emqx_schema:parse_server(Server, ?SYSKEEPER_HOST_OPTIONS),

    Options = [
        {options,
            maps:merge(
                HostCfg,
                maps:with([ack_mode, ack_timeout], Config)
            )},
        {pool_size, PoolSize}
    ],

    State = #{
        pool_name => InstanceId,
        ack_timeout => AckTimeout,
        channels => #{}
    },
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            {ok, State};
        Error ->
            Error
    end.

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_syskeeper_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(InstanceId).

on_query(InstanceId, {_MessageTag, _} = Query, State) ->
    do_query(InstanceId, [Query], State);
on_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

%% we only support batch insert
on_batch_query(InstanceId, [{_MessageTag, _} | _] = Query, State) ->
    do_query(InstanceId, Query, State);
on_batch_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

on_get_status(_InstanceId, #{pool_name := Pool, ack_timeout := AckTimeout}) ->
    Health = emqx_resource_pool:health_check_workers(
        Pool, {emqx_bridge_syskeeper_client, heartbeat, [AckTimeout + ?EXTRA_CALL_TIMEOUT]}
    ),
    status_result(Health).

status_result(true) -> ?status_connected;
status_result(false) -> ?status_disconnected;
status_result({error, _}) -> ?status_disconnected.

on_add_channel(
    _InstanceId,
    #{channels := Channels} = OldState,
    ChannelId,
    #{
        parameters := #{
            target_topic := TargetTopic,
            template := Template
        } = Parameters
    }
) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            {error, already_exists};
        _ ->
            Channel = #{
                target_qos => maps:get(target_qos, Parameters, undefined),
                target_topic => emqx_placeholder:preproc_tmpl(TargetTopic),
                template => emqx_placeholder:preproc_tmpl(Template)
            },
            Channels2 = Channels#{ChannelId => Channel},
            {ok, OldState#{channels => Channels2}}
    end.

on_remove_channel(_InstanceId, #{channels := Channels} = OldState, ChannelId) ->
    Channels2 = maps:remove(ChannelId, Channels),
    {ok, OldState#{channels => Channels2}}.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_get_channel_status(_InstanceId, ChannelId, #{channels := Channels}) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            ?status_connected;
        _ ->
            {error, not_exists}
    end.

%% -------------------------------------------------------------------------------------------------
%% Helper fns

do_query(
    InstanceId,
    Query,
    #{pool_name := PoolName, ack_timeout := AckTimeout, channels := Channels} = State
) ->
    ?TRACE(
        "QUERY",
        "syskeeper_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),

    Result =
        case try_render_message(Query, Channels) of
            {ok, Msg} ->
                [{ChannelID, _} | _] = Query,
                emqx_trace:rendered_action_template(ChannelID, #{message => Msg}),
                ecpool:pick_and_do(
                    PoolName,
                    {emqx_bridge_syskeeper_client, forward, [Msg, AckTimeout + ?EXTRA_CALL_TIMEOUT]},
                    no_handover
                );
            Error ->
                Error
        end,

    case Result of
        {error, Reason} ->
            ?tp(
                syskeeper_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "syskeeper_connector_do_query_failed",
                connector => InstanceId,
                query => Query,
                reason => Reason
            }),
            case Reason of
                ecpool_empty ->
                    {error, {recoverable_error, Reason}};
                _ ->
                    Result
            end;
        _ ->
            ?tp(
                syskeeper_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

connect(Opts) ->
    Options = proplists:get_value(options, Opts),
    emqx_bridge_syskeeper_client:start_link(Options).

try_render_message(Datas, Channels) ->
    try_render_message(Datas, Channels, []).

try_render_message([{MessageTag, Data} | T], Channels, Acc) ->
    case maps:find(MessageTag, Channels) of
        {ok, Channel} ->
            case render_message(Data, Channel) of
                {ok, Msg} ->
                    try_render_message(T, Channels, [Msg | Acc]);
                Error ->
                    Error
            end;
        _ ->
            {error, {unrecoverable_error, {invalid_message_tag, MessageTag}}}
    end;
try_render_message([], _Channels, Acc) ->
    {ok, lists:reverse(Acc)}.

render_message(#{id := Id, qos := QoS, clientid := From} = Data, #{
    target_qos := TargetQoS, target_topic := TargetTopicTks, template := Template
}) ->
    Msg = maps:with([qos, flags, topic, payload, timestamp], Data),
    Topic = emqx_placeholder:proc_tmpl(TargetTopicTks, Msg),
    {ok, Msg#{
        id => emqx_guid:from_hexstr(Id),
        qos :=
            case TargetQoS of
                undefined ->
                    QoS;
                _ ->
                    TargetQoS
            end,
        from => From,
        topic := Topic,
        payload := format_data(Template, Msg)
    }};
render_message(Data, _Channel) ->
    {error, {unrecoverable_error, {invalid_data, Data}}}.

format_data([], Msg) ->
    emqx_utils_json:encode(Msg);
format_data(Tokens, Msg) ->
    emqx_placeholder:proc_tmpl(Tokens, Msg).
