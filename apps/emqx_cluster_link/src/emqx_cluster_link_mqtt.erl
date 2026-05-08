%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_mqtt).

-include("emqx_cluster_link.hrl").
-include("emqx_cluster_link_internal.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-behaviour(ecpool_worker).

%% ecpool
-export([connect/1]).

%% emqtt
-export([handle_disconnect/4]).

-behaviour(emqx_resource).
-export([
    callback_mode/0,
    resource_type/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2
]).

-export([
    resource_id/1,
    ensure_msg_fwd_resource/1,
    remove_msg_fwd_resource/1,
    decode_forwarded_msg/1,
    decode_resp/1
]).

%% Route replication protocol
-export([
    publish_actor_init_sync/6,
    mk_actor_init_ack/3,
    mk_actor_init_ack_error/3,
    publish_route_sync/4,
    publish_heartbeat/3,
    decode_route_op/1,
    encode_field/2
]).

-export([
    forward/2
]).

-export([
    get_all_resources_cluster/0,
    get_resource_cluster/1
]).

%% BpAPI / RPC Targets
-export([
    get_resource_local_v1/1,
    get_all_resources_local_v1/0
]).

-define(MSG_CLIENTID_SUFFIX, ":msg:").

-define(MSG_POOL_PREFIX, "emqx_cluster_link_mqtt:msg:").
-define(RES_NAME(Prefix, ClusterName), <<Prefix, ClusterName/binary>>).
-define(MSG_RES_ID(ClusterName), ?RES_NAME(?MSG_POOL_PREFIX, ClusterName)).
-define(HEALTH_CHECK_TIMEOUT, 1000).
-define(RES_GROUP, <<"emqx_cluster_link">>).

-define(PROTO_VER, 1).

-define(F_OPERATION, '$op').
-define(OP_ROUTE, <<"route">>).
-define(OP_HEARTBEAT, <<"heartbeat">>).
-define(OP_ACTOR_INIT, <<"actor_init">>).
-define(OP_ACTOR_INIT_ACK, <<"actor_init_ack">>).

-define(F_ACTOR, 10).
-define(F_INCARNATION, 11).
-define(F_ROUTES, 12).
-define(F_TARGET_CLUSTER, 13).
-define(F_PROTO_VER, 14).
-define(F_RESULT, 15).
-define(F_NEED_BOOTSTRAP, 16).

-define(ROUTE_DELETE, 100).

-define(AUTO_RECONNECT_INTERVAL_S, 2).
-define(DISCONNECT_REASON(WorkerId), {disconnect_reason, WorkerId}).

-type cluster_name() :: binary().

-spec resource_id(cluster_name()) -> resource_id().
resource_id(ClusterName) ->
    ?MSG_RES_ID(ClusterName).

-spec ensure_msg_fwd_resource(emqx_cluster_link_schema:link()) ->
    {ok, emqx_resource:resource_data() | already_started} | {error, Reason :: term()}.
ensure_msg_fwd_resource(#{name := Name, resource_opts := ResOpts} = ClusterConf) ->
    ResOpts1 = ResOpts#{
        query_mode => async,
        spawn_buffer_workers => true,
        start_after_created => true
    },
    emqx_resource:create_local(?MSG_RES_ID(Name), ?RES_GROUP, ?MODULE, ClusterConf, ResOpts1).

-spec remove_msg_fwd_resource(cluster_name()) -> ok | {error, Reason :: term()}.
remove_msg_fwd_resource(ClusterName) ->
    emqx_resource:remove_local(?MSG_RES_ID(ClusterName)).

-spec get_all_resources_cluster() ->
    [{node(), emqx_rpc:erpc(#{cluster_name() => emqx_resource:resource_data()})}].
get_all_resources_cluster() ->
    Nodes = emqx:running_nodes(),
    Results = emqx_cluster_link_proto_v1:get_all_resources(Nodes),
    lists:zip(Nodes, Results).

-spec get_resource_cluster(cluster_name()) ->
    [{node(), {ok, {ok, emqx_resource:resource_data()} | {error, not_found}} | _Error}].
get_resource_cluster(ClusterName) ->
    Nodes = emqx:running_nodes(),
    Results = emqx_cluster_link_proto_v1:get_resource(Nodes, ClusterName),
    lists:zip(Nodes, Results).

%% RPC Target in `emqx_cluster_link_proto_v1'.
-spec get_resource_local_v1(cluster_name()) ->
    {ok, emqx_resource:resource_data()} | {error, not_found}.
get_resource_local_v1(ClusterName) ->
    case emqx_resource:get_instance(?MSG_RES_ID(ClusterName)) of
        {ok, _ResourceGroup, ResourceData} ->
            {ok, ResourceData};
        {error, not_found} ->
            {error, not_found}
    end.

%% RPC Target in `emqx_cluster_link_proto_v1'.
-spec get_all_resources_local_v1() -> #{cluster_name() => emqx_resource:resource_data()}.
get_all_resources_local_v1() ->
    lists:foldl(
        fun
            (?MSG_RES_ID(Name) = Id, Acc) ->
                case emqx_resource:get_instance(Id) of
                    {ok, ?RES_GROUP, ResourceData} ->
                        Acc#{Name => ResourceData};
                    _ ->
                        Acc
                end;
            (_Id, Acc) ->
                %% Doesn't follow the naming pattern; manually crafted?
                Acc
        end,
        #{},
        emqx_resource:list_group_instances(?RES_GROUP)
    ).

%%--------------------------------------------------------------------
%% emqx_resource callbacks (message forwarding)
%%--------------------------------------------------------------------

callback_mode() -> async_if_possible.

-spec resource_type() -> atom().
resource_type() ->
    cluster_link_mqtt.

on_start(ResourceId, #{name := ClusterName, pool_size := PoolSize} = ClusterConf) ->
    PoolName = ResourceId,
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {pool_type, hash},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL_S},
        {target_cluster, ClusterName},
        {client_opts, emqx_cluster_link_config:mk_emqtt_options(ClusterConf)}
    ],
    ok = emqx_resource:allocate_resource(ResourceId, ?MODULE, pool_name, PoolName),
    case emqx_resource_pool:start(PoolName, ?MODULE, Options) of
        ok ->
            LocalCluster = emqx_cluster_link_config:cluster(),
            {ok, #{pool_name => PoolName, topic => ?MSG_FWD_TOPIC(LocalCluster)}};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

on_stop(ResourceId, _State) ->
    #{pool_name := PoolName} = emqx_resource:get_allocated_resources(ResourceId),
    emqx_resource_pool:stop(PoolName).

on_query(
    _ResourceId,
    FwdMsg = #message{qos = FwdQoS, extra = PoolKey},
    _State = #{pool_name := PoolName, topic := LinkTopic}
) ->
    QoS = min(?QOS_1, FwdQoS),
    Payload = encode_payload(FwdMsg#message{extra = #{}}),
    PubResult = ecpool:pick_and_do(
        {PoolName, PoolKey},
        fun(ConnPid) ->
            emqtt:publish(ConnPid, LinkTopic, Payload, QoS)
        end,
        no_handover
    ),
    ?tp_debug("cluster_link_message_forwarded", #{
        pool => PoolName,
        message => FwdMsg,
        pub_result => PubResult
    }),
    handle_send_result(PubResult).

on_query_async(
    _ResourceId,
    FwdMsg = #message{qos = FwdQoS, extra = PoolKey},
    CallbackIn,
    _State = #{pool_name := PoolName, topic := LinkTopic}
) ->
    Callback = {fun on_async_result/2, [CallbackIn]},
    QoS = min(?QOS_1, FwdQoS),
    Payload = encode_payload(FwdMsg#message{extra = #{}}),
    Result = ecpool:pick_and_do(
        {PoolName, PoolKey},
        fun(ConnPid) ->
            PubResult = emqtt:publish_async(ConnPid, LinkTopic, Payload, QoS, Callback),
            ?tp_debug("cluster_link_message_forwarded", #{
                pool => PoolName,
                message => FwdMsg,
                pub_result => PubResult
            }),
            PubResult
        end,
        no_handover
    ),
    %% This result could be `{error, ecpool_empty}', for example, which should be
    %% recoverable.  If we didn't handle it here, it would be considered unrecoverable.
    handle_send_result(Result).

%% copied from emqx_bridge_mqtt_connector

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
classify_error(Reason) ->
    {unrecoverable_error, Reason}.

%% copied from emqx_bridge_mqtt_connector
on_get_status(ResourceId, #{pool_name := PoolName} = _State) ->
    Workers = ecpool:workers(PoolName),
    DisconnectReasons = last_disconnect_reasons(ResourceId),
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            enrich_status(combine_status(Statuses), DisconnectReasons)
    catch
        exit:timeout ->
            ?status_connecting
    end.

enrich_status(?status_connected, _) ->
    ?status_connected;
enrich_status(?status_disconnected, [LastDisconnectReason | _]) ->
    %% NOTE: Picking only the first one for the sake of simplicity.
    {?status_disconnected, LastDisconnectReason};
enrich_status(Status, _) ->
    Status.

get_status({_Name, Worker}) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} -> status(Client);
        {error, _} -> ?status_disconnected
    end.

status(Pid) ->
    try
        case proplists:get_value(socket, emqtt:info(Pid)) of
            Socket when Socket /= undefined ->
                ?status_connected;
            undefined ->
                ?status_disconnected
        end
    catch
        exit:{noproc, _} ->
            ?status_disconnected
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
            ?status_disconnected
    end.

%%--------------------------------------------------------------------
%% ecpool
%%--------------------------------------------------------------------

connect(Options) ->
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    ResourceId = proplists:get_value(name, Options),
    TargetCluster = proplists:get_value(target_cluster, Options),
    ClientOpts0 = #{clientid := ClientId0} = proplists:get_value(client_opts, Options),
    ClientIdBase = emqx_bridge_mqtt_lib:clientid_base([ClientId0, ?MSG_CLIENTID_SUFFIX]),
    ClientId = iolist_to_binary([ClientIdBase, $:, integer_to_binary(WorkerId)]),
    ClientOpts = ClientOpts0#{
        clientid => ClientId,
        msg_handler => #{
            disconnected => mk_disconnect_handler(ResourceId, WorkerId, ClientId, TargetCluster)
        }
    },
    case emqtt:start_link(ClientOpts) of
        {ok, Pid} ->
            case emqtt:connect(Pid) of
                {ok, _Props} ->
                    ok = clear_disconnect_reason(ResourceId, WorkerId),
                    {ok, Pid};
                Error ->
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "cluster_link_forwarding_client_start_failed",
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

mk_disconnect_handler(ResourceId, WorkerId, ClientId, TargetCluster) ->
    {fun ?MODULE:handle_disconnect/4, [
        ResourceId,
        WorkerId,
        #{
            target_cluster => TargetCluster,
            client_id => ClientId
        }
    ]}.

-spec handle_disconnect(
    {disconnected, emqx_types:reason_code(), emqx_types:properties()}
    | {parse_packets_error, _Reason, _ParserState}
    | {connack_error, _Reason :: atom()}
    | connack_timeout
    | _SocketError,
    _ResourceId :: resource_id(),
    _WorkerId :: pos_integer(),
    #{atom => any()}
) -> ok.
handle_disconnect(Reason, ResourceId, WorkerId, EventCtx) ->
    case Reason of
        {disconnected, RC, _Props} ->
            %% NOTE
            %% Emit warning if RC hints at misconfiguration or integration issues.
            Level =
                case RC of
                    ?RC_SUCCESS -> info;
                    ?RC_SERVER_BUSY -> info;
                    ?RC_SERVER_UNAVAILABLE -> info;
                    ?RC_SERVER_SHUTTING_DOWN -> info;
                    ?RC_SESSION_TAKEN_OVER -> info;
                    _ -> warning
                end,
            Info = #{
                cause => broker_disconnect,
                reason => emqx_reason_codes:name(RC),
                reason_code => RC
            },
            report_disconnect(ResourceId, WorkerId, Level, EventCtx, Info);
        {parse_packets_error, ParserReason, _ParserState} ->
            Info = #{cause => malformed_packet, reason => ParserReason},
            report_disconnect(ResourceId, WorkerId, warning, EventCtx, Info);
        {connack_error, RCName} ->
            Info = #{cause => broker_connect_refused, reason => RCName},
            report_disconnect(ResourceId, WorkerId, Info);
        connack_timeout ->
            Info = #{cause => broker_connect_timeout},
            report_disconnect(ResourceId, WorkerId, Info);
        SocketError ->
            Info = #{cause => socket_error, reason => SocketError},
            report_disconnect(ResourceId, WorkerId, info, EventCtx, Info)
    end.

report_disconnect(ResourceId, WorkerId, Info) ->
    clear_disconnect_reason(ResourceId, WorkerId),
    emqx_resource:allocate_resource(ResourceId, ?MODULE, ?DISCONNECT_REASON(WorkerId), Info).

report_disconnect(ResourceId, WorkerId, Level, EventCtx, Info) ->
    ?SLOG(
        Level,
        maps:merge(EventCtx#{msg => "cluster_link_forwarding_client_disconnected"}, Info)
    ),
    report_disconnect(ResourceId, WorkerId, Info).

last_disconnect_reasons(ResourceId) ->
    Allocated = emqx_resource:get_allocated_resources_list(ResourceId),
    [Reason || {_ResourceId, ?DISCONNECT_REASON(_), Reason} <- Allocated].

clear_disconnect_reason(ResourceId, WorkerId) ->
    emqx_resource:deallocate_resource(ResourceId, ?DISCONNECT_REASON(WorkerId)).

%%--------------------------------------------------------------------
%% Protocol
%%--------------------------------------------------------------------

publish_actor_init_sync(ClientPid, ReqId, RespTopic, TargetCluster, Actor, Incarnation) ->
    Topic = ?ROUTE_TOPIC(emqx_cluster_link_config:cluster()),
    Payload = #{
        ?F_OPERATION => ?OP_ACTOR_INIT,
        ?F_PROTO_VER => ?PROTO_VER,
        ?F_TARGET_CLUSTER => TargetCluster,
        ?F_ACTOR => Actor,
        ?F_INCARNATION => Incarnation
    },
    Properties = #{
        'Response-Topic' => RespTopic,
        'Correlation-Data' => ReqId
    },
    emqtt:publish(ClientPid, Topic, Properties, encode_payload(Payload), [{qos, ?QOS_1}]).

publish_route_sync(ClientPid, Actor, Incarnation, Updates) ->
    Topic = ?ROUTE_TOPIC(emqx_cluster_link_config:cluster()),
    Payload = #{
        ?F_OPERATION => ?OP_ROUTE,
        ?F_ACTOR => Actor,
        ?F_INCARNATION => Incarnation,
        ?F_ROUTES => Updates
    },
    emqtt:publish(ClientPid, Topic, encode_payload(Payload), ?QOS_1).

publish_heartbeat(ClientPid, Actor, Incarnation) ->
    Topic = ?ROUTE_TOPIC(emqx_cluster_link_config:cluster()),
    Payload = #{
        ?F_OPERATION => ?OP_HEARTBEAT,
        ?F_ACTOR => Actor,
        ?F_INCARNATION => Incarnation
    },
    emqtt:publish_async(ClientPid, Topic, encode_payload(Payload), ?QOS_0, {fun(_) -> ok end, []}).

-spec mk_actor_init_ack(_Actor :: binary(), boolean(), _ReplyTo :: emqx_types:message()) ->
    emqx_types:message().
mk_actor_init_ack(Actor, NeedBootstrap, MsgIn) ->
    Payload = #{
        ?F_OPERATION => ?OP_ACTOR_INIT_ACK,
        ?F_PROTO_VER => ?PROTO_VER,
        ?F_ACTOR => Actor,
        ?F_RESULT => ok,
        ?F_NEED_BOOTSTRAP => NeedBootstrap
    },
    mk_response(?QOS_1, Payload, MsgIn).

-spec mk_actor_init_ack_error(_Actor :: binary(), {error, _}, _ReplyTo :: emqx_types:message()) ->
    emqx_types:message().
mk_actor_init_ack_error(Actor, Error, MsgIn) ->
    Payload = #{
        ?F_OPERATION => ?OP_ACTOR_INIT_ACK,
        ?F_PROTO_VER => ?PROTO_VER,
        ?F_ACTOR => Actor,
        ?F_RESULT => Error,
        ?F_NEED_BOOTSTRAP => undefined
    },
    mk_response(?QOS_1, Payload, MsgIn).

mk_response(QoS, Payload, MsgIn) ->
    #{
        'Response-Topic' := RespTopic,
        'Correlation-Data' := ReqId
    } = emqx_message:get_header(properties, MsgIn),
    emqx_message:make(
        undefined,
        QoS,
        RespTopic,
        encode_payload(Payload),
        #{},
        #{properties => #{'Correlation-Data' => ReqId}}
    ).

decode_route_op(Payload) ->
    decode_route_op1(decode_payload(Payload)).

decode_route_op1(#{
    ?F_OPERATION := ?OP_ACTOR_INIT,
    ?F_PROTO_VER := ProtoVer,
    ?F_TARGET_CLUSTER := TargetCluster,
    ?F_ACTOR := Actor,
    ?F_INCARNATION := Incr
}) ->
    Info = #{
        target_cluster => TargetCluster,
        proto_ver => ProtoVer
    },
    {actor_init, #{actor => Actor, incarnation => Incr}, Info};
decode_route_op1(#{
    ?F_OPERATION := ?OP_ROUTE,
    ?F_ACTOR := Actor,
    ?F_INCARNATION := Incr,
    ?F_ROUTES := RouteOps
}) ->
    RouteOps1 = lists:map(fun(Op) -> decode_field(route, Op) end, RouteOps),
    {route_updates, #{actor => Actor, incarnation => Incr}, RouteOps1};
decode_route_op1(#{
    ?F_OPERATION := ?OP_HEARTBEAT,
    ?F_ACTOR := Actor,
    ?F_INCARNATION := Incr
}) ->
    {heartbeat, #{actor => Actor, incarnation => Incr}};
decode_route_op1(Payload) ->
    {error, {unknown_payload, Payload}}.

decode_resp(Payload) ->
    decode_resp1(decode_payload(Payload)).

decode_resp1(#{
    ?F_OPERATION := ?OP_ACTOR_INIT_ACK,
    ?F_ACTOR := Actor,
    ?F_PROTO_VER := ProtoVer,
    ?F_RESULT := InitResult,
    ?F_NEED_BOOTSTRAP := NeedBootstrap
}) ->
    {actor_init_ack, #{
        actor => Actor, result => InitResult, proto_ver => ProtoVer, need_bootstrap => NeedBootstrap
    }}.

decode_forwarded_msg(Payload) ->
    case decode_payload(Payload) of
        #message{} = Msg ->
            Msg;
        _ ->
            ?SLOG(warning, #{
                msg => "unexpected_cluster_link_forwarded_msg_payload",
                payload => Payload
            }),
            {error, Payload}
    end.

encode_field(route, {add, Route = {_Topic, _ID}}) ->
    Route;
encode_field(route, {delete, {Topic, ID}}) ->
    {?ROUTE_DELETE, Topic, ID}.

decode_field(route, {?ROUTE_DELETE, Topic, ID}) ->
    {delete, {Topic, ID}};
decode_field(route, Route = {_Topic, _ID}) ->
    {add, Route}.

encode_payload(Payload) ->
    erlang:term_to_binary(Payload).

decode_payload(Payload) ->
    erlang:binary_to_term(Payload, [safe]).

%%--------------------------------------------------------------------
%% emqx_external_broker
%%--------------------------------------------------------------------

forward(ClusterName, #delivery{message = Msg}) ->
    %% NOTE
    %% Attaching pick key to the message to pick forwarding connection accordingly.
    Key = choose_pick_key(emqx_cluster_link_config:link(ClusterName)),
    FwdMsg = Msg#message{extra = Key},
    QueryOpts = #{pick_key => Key},
    emqx_resource:query(?MSG_RES_ID(ClusterName), FwdMsg, QueryOpts).

choose_pick_key(#{message_dispatch_strategy := random}) ->
    %% NOTE: Cheap and has high rate of change.
    erlang:monotonic_time();
choose_pick_key(_LinkConf) ->
    self().
