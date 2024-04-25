%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_connector).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-behaviour(emqx_resource).
-behaviour(ecpool_worker).

%% ecpool
-export([connect/1]).

-export([on_message_received/3]).
-export([handle_disconnect/1]).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1
]).

-export([on_async_result/2]).

-type name() :: term().

-type option() ::
    {name, name()}
    | {ingress, map()}
    %% see `emqtt:option()`
    | {client_opts, map()}.

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HEALTH_CHECK_TIMEOUT, 1000).
-define(INGRESS, "I").
-define(EGRESS, "E").

%% ===================================================================
%% When use this bridge as a data source, ?MODULE:on_message_received will be called
%% if the bridge received msgs from the remote broker.

on_message_received(Msg, HookPoints, ResId) ->
    emqx_resource_metrics:received_inc(ResId),
    lists:foreach(
        fun(HookPoint) ->
            emqx_hooks:run(HookPoint, [Msg])
        end,
        HookPoints
    ),
    ok.

%% ===================================================================
callback_mode() -> async_if_possible.

on_start(ResourceId, #{server := Server} = Conf) ->
    ?SLOG(info, #{
        msg => "starting_mqtt_connector",
        connector => ResourceId,
        config => emqx_utils:redact(Conf)
    }),
    TopicToHandlerIndex = emqx_topic_index:new(),
    StartConf = Conf#{topic_to_handler_index => TopicToHandlerIndex},
    case start_mqtt_clients(ResourceId, StartConf) of
        {ok, Result1} ->
            {ok, Result1#{
                installed_channels => #{},
                clean_start => maps:get(clean_start, Conf),
                topic_to_handler_index => TopicToHandlerIndex,
                server => Server
            }};
        {error, Reason} ->
            {error, Reason}
    end.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        clean_start := CleanStart
    } = OldState,
    ChannelId,
    #{config_root := actions} = ChannelConfig
) ->
    %% Publisher channel
    %% make a warning if clean_start is set to false
    case CleanStart of
        false ->
            ?tp(
                mqtt_clean_start_egress_action_warning,
                #{
                    channel_id => ChannelId,
                    resource_id => _InstId
                }
            ),
            ?SLOG(warning, #{
                msg => "mqtt_publisher_clean_start_false",
                reason => "clean_start is set to false when using MQTT publisher action, " ++
                    "which may cause unexpected behavior. " ++
                    "For example, if the client ID is already subscribed to topics, " ++
                    "we might receive messages that are unhanded.",
                channel => ChannelId,
                config => emqx_utils:redact(ChannelConfig)
            });
        true ->
            ok
    end,
    RemoteParams0 = maps:get(parameters, ChannelConfig),
    {LocalParams, RemoteParams} = take(local, RemoteParams0, #{}),
    ChannelState = emqx_bridge_mqtt_egress:config(#{remote => RemoteParams, local => LocalParams}),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState};
on_add_channel(
    _ResourceId,
    #{
        installed_channels := InstalledChannels,
        pool_name := PoolName,
        topic_to_handler_index := TopicToHandlerIndex,
        server := Server
    } = OldState,
    ChannelId,
    #{hookpoints := HookPoints} = ChannelConfig
) ->
    %% Add ingress channel
    RemoteParams0 = maps:get(parameters, ChannelConfig),
    {LocalParams, RemoteParams} = take(local, RemoteParams0, #{}),
    ChannelState0 = #{
        hookpoints => HookPoints,
        server => Server,
        config_root => sources,
        local => LocalParams,
        remote => RemoteParams
    },
    ChannelState1 = mk_ingress_config(ChannelId, ChannelState0, TopicToHandlerIndex),
    ok = emqx_bridge_mqtt_ingress:subscribe_channel(PoolName, ChannelState1),
    NewInstalledChannels = maps:put(ChannelId, ChannelState1, InstalledChannels),
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        pool_name := PoolName,
        topic_to_handler_index := TopicToHandlerIndex
    } = OldState,
    ChannelId
) ->
    ChannelState = maps:get(ChannelId, InstalledChannels),
    case ChannelState of
        #{
            config_root := sources
        } ->
            emqx_bridge_mqtt_ingress:unsubscribe_channel(
                PoolName, ChannelState, ChannelId, TopicToHandlerIndex
            ),
            ok;
        _ ->
            ok
    end,
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    _ResId,
    ChannelId,
    #{
        installed_channels := Channels
    } = _State
) when is_map_key(ChannelId, Channels) ->
    %% The channel should be ok as long as the MQTT client is ok
    connected.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

start_mqtt_clients(ResourceId, Conf) ->
    ClientOpts = mk_client_opts(ResourceId, Conf),
    start_mqtt_clients(ResourceId, Conf, ClientOpts).

start_mqtt_clients(ResourceId, StartConf, ClientOpts) ->
    PoolName = <<ResourceId/binary>>,
    #{
        pool_size := PoolSize
    } = StartConf,
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {client_opts, ClientOpts}
    ],
    ok = emqx_resource:allocate_resource(ResourceId, pool_name, PoolName),
    case emqx_resource_pool:start(PoolName, ?MODULE, Options) of
        ok ->
            {ok, #{pool_name => PoolName}};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

on_stop(ResourceId, State) ->
    ?SLOG(info, #{
        msg => "stopping_mqtt_connector",
        connector => ResourceId
    }),
    %% on_stop can be called with State = undefined
    StateMap =
        case State of
            Map when is_map(State) ->
                Map;
            _ ->
                #{}
        end,
    case maps:get(topic_to_handler_index, StateMap, undefined) of
        undefined ->
            ok;
        TopicToHandlerIndex ->
            ets:delete(TopicToHandlerIndex)
    end,
    Allocated = emqx_resource:get_allocated_resources(ResourceId),
    ok = stop_helper(Allocated),
    ?tp(mqtt_connector_stopped, #{instance_id => ResourceId}),
    ok.

stop_helper(#{pool_name := PoolName}) ->
    emqx_resource_pool:stop(PoolName).

on_query(
    ResourceId,
    {ChannelId, Msg},
    #{pool_name := PoolName} = State
) ->
    ?TRACE(
        "QUERY",
        "send_msg_to_remote_node",
        #{
            message => Msg,
            connector => ResourceId,
            channel_id => ChannelId
        }
    ),
    Channels = maps:get(installed_channels, State),
    ChannelConfig = maps:get(ChannelId, Channels),
    handle_send_result(with_egress_client(ChannelId, PoolName, send, [Msg, ChannelConfig]));
on_query(ResourceId, {_ChannelId, Msg}, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        connector => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

on_query_async(
    ResourceId,
    {ChannelId, Msg},
    CallbackIn,
    #{pool_name := PoolName} = State
) ->
    ?TRACE("QUERY", "async_send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    Callback = {fun on_async_result/2, [CallbackIn]},
    Channels = maps:get(installed_channels, State),
    ChannelConfig = maps:get(ChannelId, Channels),
    Result = with_egress_client(ChannelId, PoolName, send_async, [Msg, Callback, ChannelConfig]),
    case Result of
        ok ->
            ok;
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, Reason} ->
            {error, classify_error(Reason)}
    end;
on_query_async(ResourceId, {_ChannelId, Msg}, _Callback, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        connector => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

with_egress_client(ActionID, ResourceId, Fun, Args) ->
    TraceRenderedCTX = emqx_trace:make_rendered_action_template_trace_context(ActionID),
    ecpool:pick_and_do(
        ResourceId, {emqx_bridge_mqtt_egress, Fun, [TraceRenderedCTX | Args]}, no_handover
    ).

on_async_result(Callback, Result) ->
    apply_callback_function(Callback, handle_send_result(Result)).

apply_callback_function(F, Result) when is_function(F) ->
    erlang:apply(F, [Result]);
apply_callback_function({F, A}, Result) when is_function(F), is_list(A) ->
    erlang:apply(F, A ++ [Result]);
apply_callback_function({M, F, A}, Result) when is_atom(M), is_atom(F), is_list(A) ->
    erlang:apply(M, F, A ++ [Result]).

handle_send_result(ok) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_SUCCESS}}) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}) ->
    ok;
handle_send_result({ok, Reply}) ->
    {error, classify_reply(Reply)};
handle_send_result({error, Reason}) ->
    {error, classify_error(Reason)}.

classify_reply(Reply = #{reason_code := _}) ->
    {unrecoverable_error, Reply}.

classify_error(disconnected = Reason) ->
    {recoverable_error, Reason};
classify_error(ecpool_empty) ->
    {recoverable_error, disconnected};
classify_error({disconnected, _RC, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({shutdown, _} = Reason) ->
    {recoverable_error, Reason};
classify_error(shutdown = Reason) ->
    {recoverable_error, Reason};
classify_error({unrecoverable_error, _Reason} = Error) ->
    Error;
classify_error(Reason) ->
    {unrecoverable_error, Reason}.

on_get_status(_ResourceId, State) ->
    Pools = maps:to_list(maps:with([pool_name], State)),
    Workers = [{Pool, Worker} || {Pool, PN} <- Pools, {_Name, Worker} <- ecpool:workers(PN)],
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            combine_status(Statuses)
    catch
        exit:timeout ->
            connecting
    end.

get_status({_Pool, Worker}) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} ->
            emqx_bridge_mqtt_ingress:status(Client);
        {error, _} ->
            disconnected
    end.

combine_status(Statuses) ->
    %% NOTE
    %% Natural order of statuses: [connected, connecting, disconnected]
    %% * `disconnected` wins over any other status
    %% * `connecting` wins over `connected`
    case lists:reverse(lists:usort(Statuses)) of
        [Status | _] ->
            Status;
        [] ->
            disconnected
    end.

mk_ingress_config(
    ChannelId,
    IngressChannelConfig,
    TopicToHandlerIndex
) ->
    HookPoints = maps:get(hookpoints, IngressChannelConfig, []),
    NewConf = IngressChannelConfig#{
        on_message_received => {?MODULE, on_message_received, [HookPoints, ChannelId]},
        ingress_list => [IngressChannelConfig]
    },
    emqx_bridge_mqtt_ingress:config(NewConf, ChannelId, TopicToHandlerIndex).

mk_client_opts(
    ResourceId,
    Config = #{
        server := Server,
        keepalive := KeepAlive,
        ssl := #{enable := EnableSsl} = Ssl
    }
) ->
    HostPort = emqx_bridge_mqtt_connector_schema:parse_server(Server),
    Options = maps:with(
        [
            proto_ver,
            username,
            password,
            clean_start,
            retry_interval,
            max_inflight,
            % Opening a connection in bridge mode will form a non-standard mqtt connection message.
            % A load balancing server (such as haproxy) is often set up before the emqx broker server.
            % When the load balancing server enables mqtt connection packet inspection,
            % non-standard mqtt connection packets might be filtered out by LB.
            bridge_mode,
            topic_to_handler_index
        ],
        Config
    ),
    Name = parse_id_to_name(ResourceId),
    mk_client_opt_password(Options#{
        hosts => [HostPort],
        clientid => clientid(Name, Config),
        connect_timeout => 30,
        keepalive => ms_to_s(KeepAlive),
        force_ping => true,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    }).

parse_id_to_name(Id) ->
    {_Type, Name} = emqx_connector_resource:parse_connector_id(Id, #{atom_name => false}),
    Name.

mk_client_opt_password(Options = #{password := Secret}) ->
    %% TODO: Teach `emqtt` to accept 0-arity closures as passwords.
    Options#{password := emqx_secret:unwrap(Secret)};
mk_client_opt_password(Options) ->
    Options.

ms_to_s(Ms) ->
    erlang:ceil(Ms / 1000).

clientid(Name, _Conf = #{clientid_prefix := Prefix}) when
    is_binary(Prefix) andalso Prefix =/= <<>>
->
    emqx_bridge_mqtt_lib:clientid_base([Prefix, $:, Name]);
clientid(Name, _Conf) ->
    emqx_bridge_mqtt_lib:clientid_base([Name]).

%% @doc Start an ingress bridge worker.
-spec connect([option() | {ecpool_worker_id, pos_integer()}]) ->
    {ok, pid()} | {error, _Reason}.
connect(Options) ->
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    ?SLOG(debug, #{
        msg => "ingress_client_starting",
        options => emqx_utils:redact(Options)
    }),
    Name = proplists:get_value(name, Options),
    ClientOpts = proplists:get_value(client_opts, Options),
    case emqtt:start_link(mk_client_opts(Name, WorkerId, ClientOpts)) of
        {ok, Pid} ->
            connect(Pid, Name);
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_start_failed",
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

mk_client_opts(
    Name,
    WorkerId,
    ClientOpts = #{
        clientid := ClientId,
        topic_to_handler_index := TopicToHandlerIndex
    }
) ->
    ClientOpts#{
        clientid := mk_clientid(WorkerId, ClientId),
        msg_handler => mk_client_event_handler(Name, TopicToHandlerIndex)
    }.

mk_clientid(WorkerId, ClientId) ->
    emqx_bridge_mqtt_lib:bytes23([ClientId], WorkerId).

mk_client_event_handler(Name, TopicToHandlerIndex) ->
    #{
        publish => {fun emqx_bridge_mqtt_ingress:handle_publish/3, [Name, TopicToHandlerIndex]},
        disconnected => {fun ?MODULE:handle_disconnect/1, []}
    }.

-spec connect(pid(), name()) ->
    {ok, pid()} | {error, _Reason}.
connect(Pid, Name) ->
    case emqtt:connect(Pid) of
        {ok, _Props} ->
            {ok, Pid};
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => "ingress_client_connect_failed",
                reason => Reason,
                name => Name
            }),
            _ = catch emqtt:stop(Pid),
            Error
    end.

handle_disconnect(_Reason) ->
    ok.

take(Key, Map0, Default) ->
    case maps:take(Key, Map0) of
        {Value, Map} ->
            {Value, Map};
        error ->
            {Default, Map0}
    end.
