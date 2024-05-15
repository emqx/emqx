%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_mqtt).

-include("emqx_cluster_link.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

%-include_lib("emqtt/include/emqtt.hrl").

-behaviour(emqx_resource).
-behaviour(ecpool_worker).

%% ecpool
-export([connect/1]).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2
]).

-export([
    ensure_msg_fwd_resource/1,
    stop_msg_fwd_resource/1,
    start_routing_pool/1,
    stop_routing_pool/1,
    routing_pool_workers/1,
    init_link/1,
    ack_link/4,
    remove_link/1,
    publish_route_op/4,
    publish_routes/3,
    cleanup_routes/1,
    decode_ctrl_msg/2,
    decode_route_op/1,
    decode_forwarded_msg/1
]).

-export([
    publish_actor_init_sync/3,
    publish_route_sync/4,
    encode_field/2
]).

-export([
    forward/2
]).

-define(ROUTE_CLIENTID_SUFFIX, ":route:").
-define(MSG_CLIENTID_SUFFIX, ":msg:").
-define(CLIENTID(Base, Suffix), emqx_bridge_mqtt_lib:clientid_base([Base, Suffix])).

-define(MQTT_HOST_OPTS, #{default_port => 1883}).
-define(MY_CLUSTER_NAME, emqx_cluster_link_config:cluster()).

-define(ROUTE_TOPIC, <<?ROUTE_TOPIC_PREFIX, (?MY_CLUSTER_NAME)/binary>>).
-define(MSG_FWD_TOPIC, <<?MSG_TOPIC_PREFIX, (?MY_CLUSTER_NAME)/binary>>).
-define(CTRL_TOPIC(ClusterName), <<?CTRL_TOPIC_PREFIX, (ClusterName)/binary>>).

%% ecpool and emqx_resource names
-define(ROUTE_POOL_PREFIX, "emqx_cluster_link_mqtt:route:").
-define(MSG_POOL_PREFIX, "emqx_cluster_link_mqtt:msg:").
-define(RES_NAME(Prefix, ClusterName), <<Prefix, ClusterName/binary>>).
-define(ROUTE_POOL_NAME(ClusterName), ?RES_NAME(?ROUTE_POOL_PREFIX, ClusterName)).
-define(MSG_RES_ID(ClusterName), ?RES_NAME(?MSG_POOL_PREFIX, ClusterName)).
-define(HEALTH_CHECK_TIMEOUT, 1000).
-define(RES_GROUP, <<"emqx_cluster_link">>).
-define(DEFAULT_POOL_KEY, <<"default">>).

%% Protocol
-define(PROTO_VER, <<"1.0">>).
-define(INIT_LINK_OP, <<"init_link">>).
-define(ACK_LINK_OP, <<"ack_link">>).
-define(UNLINK_OP, <<"unlink">>).
-define(BATCH_ROUTES_OP, <<"add_routes">>).
-define(CLEANUP_ROUTES_OP, <<"cleanup_routes">>).
%% It's worth optimizing non-batch op payload size,
%% thus it's encoded as a plain binary
-define(TOPIC_WITH_OP(Op, Topic), <<Op/binary, "_", Topic/binary>>).

-define(DECODE(Payload), erlang:binary_to_term(Payload, [safe])).
-define(ENCODE(Payload), erlang:term_to_binary(Payload)).

-define(F_OPERATION, '$op').
-define(OP_ROUTE, <<"route">>).
-define(OP_ACTOR_INIT, <<"actor_init">>).

-define(F_ACTOR, 10).
-define(F_INCARNATION, 11).
-define(F_ROUTES, 12).

-define(ROUTE_DELETE, 100).

-define(PUB_TIMEOUT, 10_000).

ensure_msg_fwd_resource(#{upstream := Name, pool_size := PoolSize} = ClusterConf) ->
    ResConf = #{
        query_mode => async,
        start_after_created => true,
        start_timeout => 5000,
        health_check_interval => 5000,
        %% TODO: configure res_buf_worker pool separately?
        worker_pool_size => PoolSize
    },
    emqx_resource:create_local(?MSG_RES_ID(Name), ?RES_GROUP, ?MODULE, ClusterConf, ResConf).

stop_msg_fwd_resource(ClusterName) ->
    emqx_resource:stop(?MSG_RES_ID(ClusterName)).

%%--------------------------------------------------------------------
%% emqx_resource callbacks (message forwarding)
%%--------------------------------------------------------------------

callback_mode() -> async_if_possible.

on_start(ResourceId, #{pool_size := PoolSize} = ClusterConf) ->
    PoolName = ResourceId,
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {pool_type, hash},
        {client_opts, emqtt_client_opts(?MSG_CLIENTID_SUFFIX, ClusterConf)}
    ],
    ok = emqx_resource:allocate_resource(ResourceId, pool_name, PoolName),
    case emqx_resource_pool:start(PoolName, ?MODULE, Options) of
        ok ->
            {ok, #{pool_name => PoolName, topic => ?MSG_FWD_TOPIC}};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

on_stop(ResourceId, _State) ->
    #{pool_name := PoolName} = emqx_resource:get_allocated_resources(ResourceId),
    emqx_resource_pool:stop(PoolName).

on_query(_ResourceId, FwdMsg, #{pool_name := PoolName, topic := LinkTopic} = _State) when
    is_record(FwdMsg, message)
->
    #message{topic = Topic, qos = QoS} = FwdMsg,
    handle_send_result(
        ecpool:pick_and_do(
            {PoolName, Topic},
            fun(ConnPid) ->
                emqtt:publish(ConnPid, LinkTopic, ?ENCODE(FwdMsg), QoS)
            end,
            no_handover
        )
    );
on_query(_ResourceId, {Topic, Props, Payload, QoS}, #{pool_name := PoolName} = _State) ->
    handle_send_result(
        ecpool:pick_and_do(
            {PoolName, Topic},
            fun(ConnPid) ->
                emqtt:publish(ConnPid, Topic, Props, ?ENCODE(Payload), [{qos, QoS}])
            end,
            no_handover
        )
    ).

on_query_async(
    _ResourceId, FwdMsg, CallbackIn, #{pool_name := PoolName, topic := LinkTopic} = _State
) ->
    Callback = {fun on_async_result/2, [CallbackIn]},
    #message{topic = Topic, qos = QoS} = FwdMsg,
    %% TODO check message ordering, pick by topic,client pair?
    ecpool:pick_and_do(
        {PoolName, Topic},
        fun(ConnPid) ->
            %% #delivery{} record has no valuable data for a remote link...
            Payload = ?ENCODE(FwdMsg),
            %% TODO: check override QOS requirements (if any)
            emqtt:publish_async(ConnPid, LinkTopic, Payload, QoS, Callback)
        end,
        no_handover
    ).

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
on_get_status(_ResourceId, #{pool_name := PoolName} = _State) ->
    Workers = [Worker || {_Name, Worker} <- ecpool:workers(PoolName)],
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            combine_status(Statuses)
    catch
        exit:timeout ->
            connecting
    end.

get_status(Worker) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} -> status(Client);
        {error, _} -> disconnected
    end.

status(Pid) ->
    try
        case proplists:get_value(socket, emqtt:info(Pid)) of
            Socket when Socket /= undefined ->
                connected;
            undefined ->
                connecting
        end
    catch
        exit:{noproc, _} ->
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

%%--------------------------------------------------------------------
%% ecpool
%%--------------------------------------------------------------------

connect(Options) ->
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    #{clientid := ClientId} = ClientOpts = proplists:get_value(client_opts, Options),
    ClientId1 = emqx_bridge_mqtt_lib:bytes23([ClientId], WorkerId),
    ClientOpts1 = ClientOpts#{clientid => ClientId1},
    case emqtt:start_link(ClientOpts1) of
        {ok, Pid} ->
            case emqtt:connect(Pid) of
                {ok, _Props} ->
                    {ok, Pid};
                Error ->
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_start_failed",
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

%%--------------------------------------------------------------------
%% Routing
%%--------------------------------------------------------------------

routing_pool_workers(#{upstream := ClusterName} = _ClusterConf) ->
    ecpool:workers(?ROUTE_POOL_NAME(ClusterName)).

start_routing_pool(#{upstream := ClusterName} = ClusterConf) ->
    start_pool(?ROUTE_POOL_NAME(ClusterName), ?ROUTE_CLIENTID_SUFFIX, ClusterConf).

stop_routing_pool(ClusterName) ->
    ecpool:stop_sup_pool(?ROUTE_POOL_NAME(ClusterName)).

init_link(ClusterName) ->
    Payload = #{
        <<"op">> => ?INIT_LINK_OP,
        <<"proto_ver">> => ?PROTO_VER,
        <<"upstream">> => ClusterName,
        %% TODO: may no need to reserve it as it is a map?
        <<"extra">> => #{}
    },
    ReqId = emqx_utils_conv:bin(emqx_utils:gen_id(16)),
    Properties = #{
        'Response-Topic' => ?CTRL_TOPIC(ClusterName),
        'Correlation-Data' => ReqId
    },
    Topic = ?CTRL_TOPIC(?MY_CLUSTER_NAME),
    {ReqId, publish(sync, ClusterName, ?DEFAULT_POOL_KEY, Payload, Properties, Topic, ?QOS_1)}.

ack_link(ClusterName, Result, RespTopic, ReqId) ->
    Payload = #{
        <<"op">> => ?ACK_LINK_OP,
        %% The links may compare and downgrade/adjust protocol in future
        <<"proto_ver">> => ?PROTO_VER,
        %% may be used in future to avoud re-bootrstrapping all the routes,
        %% for example, if the connection was abrupted for a while but the cluster was healthy
        %% and didn't lost any routes. In that case, retrying lost route updates would be sufficient.
        %% For now, it's always true for simplicitiy reasons.
        <<"need_bootstrap">> => true,
        <<"extra">> => #{}
    },
    Payload1 =
        case Result of
            {ok, _} ->
                Payload#{<<"result">> => <<"ok">>};
            {error, Reason} ->
                Payload#{<<"result">> => <<"error">>, reason => Reason}
        end,
    Props = #{'Correlation-Data' => ReqId},
    Query = {RespTopic, Props, Payload1, ?QOS_1},
    %% Using msg forwading resource to send the response back.
    %% TODO: maybe async query?
    emqx_resource:query(?MSG_RES_ID(ClusterName), Query, #{
        query_mode => simple_sync, pick_key => RespTopic
    }).

remove_link(ClusterName) ->
    Payload = #{<<"op">> => ?UNLINK_OP},
    Topic = ?CTRL_TOPIC(?MY_CLUSTER_NAME),
    publish(sync, ClusterName, ?DEFAULT_POOL_KEY, Payload, #{}, Topic, ?QOS_0).

publish_routes(QueryType, ClusterName, Topics) ->
    %% Picks the same pool worker consistently.
    %% Although, as writes are idompotent we can pick it randomly - TBD.
    publish_routes(QueryType, ClusterName, ?DEFAULT_POOL_KEY, Topics).

publish_routes(QueryType, ClusterName, PoolKey, Topics) ->
    Payload = #{<<"op">> => ?BATCH_ROUTES_OP, <<"topics">> => Topics},
    publish(QueryType, ClusterName, PoolKey, Payload).

cleanup_routes(ClusterName) ->
    Payload = #{<<"op">> => ?CLEANUP_ROUTES_OP},
    publish(sync, ClusterName, ?DEFAULT_POOL_KEY, Payload, #{}, ?ROUTE_TOPIC, ?QOS_0).

publish_route_op(QueryType, ClusterName, Op, Topic) when Op =:= <<"add">>; Op =:= <<"delete">> ->
    Payload = ?TOPIC_WITH_OP(Op, Topic),
    publish(QueryType, ClusterName, Topic, Payload).

publish(QueryType, ClusterName, PoolKey, Payload) ->
    publish(QueryType, ClusterName, PoolKey, Payload, #{}).

publish(QueryType, ClusterName, PoolKey, Payload, Props) ->
    %% Deletes are not implemented for now, writes are idempotent, so QOS_1 is fine.
    publish(QueryType, ClusterName, PoolKey, Payload, Props, ?ROUTE_TOPIC, ?QOS_1).

publish(async, ClusterName, PoolKey, Payload, Props, Topic, QoS) ->
    ecpool:pick_and_do(
        {?ROUTE_POOL_NAME(ClusterName), PoolKey},
        fun(ConnPid) ->
            Ref = erlang:make_ref(),
            Cb = {fun publish_result/3, [self(), Ref]},
            emqtt:publish_async(
                ConnPid, Topic, Props, ?ENCODE(Payload), [{qos, QoS}], ?PUB_TIMEOUT, Cb
            ),
            Ref
        end,
        no_handover
    );
publish(sync, ClusterName, PoolKey, Payload, Props, Topic, QoS) ->
    ecpool:pick_and_do(
        {?ROUTE_POOL_NAME(ClusterName), PoolKey},
        fun(ConnPid) ->
            emqtt:publish(ConnPid, Topic, Props, ?ENCODE(Payload), [{qos, QoS}])
        end,
        no_handover
    ).

publish_result(Caller, Ref, Result) ->
    case handle_send_result(Result) of
        ok ->
            %% avoid extra message passing, we only care about errors for now
            ok;
        Err ->
            Caller ! {pub_result, Ref, Err}
    end.

%%% New leader-less Syncer/Actor implementation

publish_actor_init_sync(ClientPid, Actor, Incarnation) ->
    %% TODO: handshake (request / response) to make sure the link is established
    PubTopic = ?ROUTE_TOPIC,
    Payload = #{
        ?F_OPERATION => ?OP_ACTOR_INIT,
        ?F_ACTOR => Actor,
        ?F_INCARNATION => Incarnation
    },
    emqtt:publish(ClientPid, PubTopic, ?ENCODE(Payload), ?QOS_1).

publish_route_sync(ClientPid, Actor, Incarnation, Updates) ->
    PubTopic = ?ROUTE_TOPIC,
    Payload = #{
        ?F_OPERATION => ?OP_ROUTE,
        ?F_ACTOR => Actor,
        ?F_INCARNATION => Incarnation,
        ?F_ROUTES => Updates
    },
    emqtt:publish(ClientPid, PubTopic, ?ENCODE(Payload), ?QOS_1).

%%--------------------------------------------------------------------
%% Protocol
%%--------------------------------------------------------------------

decode_ctrl_msg(Payload, ClusterName) ->
    decode_ctrl_msg1(?DECODE(Payload), ClusterName).

decode_ctrl_msg1(
    #{
        <<"op">> := ?INIT_LINK_OP,
        <<"proto_ver">> := ProtoVer,
        <<"upstream">> := UpstreamName
    },
    ClusterName
) ->
    ProtoVer1 = decode_proto_ver(ProtoVer, ClusterName),
    %% UpstreamName is the name the remote linked cluster refers to this cluster,
    %% so it must equal to the local cluster name, more clear naming is desired...
    MyClusterName = ?MY_CLUSTER_NAME,
    case UpstreamName of
        MyClusterName ->
            {init_link, {ok, #{proto_ver => ProtoVer1}}};
        _ ->
            ?SLOG(error, #{
                msg => "misconfigured_cluster_link_name",
                %% How this cluster names itself
                local_name => MyClusterName,
                %% How the remote cluster names itself
                link_name => ClusterName,
                %% How the remote cluster names this local cluster
                upstream_name => UpstreamName
            }),
            {init_link, {error, <<"bad_upstream_name">>}}
    end;
decode_ctrl_msg1(
    #{
        <<"op">> := ?ACK_LINK_OP,
        <<"result">> := <<"ok">>,
        <<"proto_ver">> := ProtoVer,
        <<"need_bootstrap">> := IsBootstrapNeeded
    },
    ClusterName
) ->
    ProtoVer1 = decode_proto_ver(ProtoVer, ClusterName),
    {ack_link, {ok, #{proto_ver => ProtoVer1, need_bootstrap => IsBootstrapNeeded}}};
decode_ctrl_msg1(
    #{
        <<"op">> := ?ACK_LINK_OP,
        <<"result">> := <<"error">>,
        <<"reason">> := Reason
    },
    _ClusterName
) ->
    {ack_link, {error, Reason}};
decode_ctrl_msg1(#{<<"op">> := ?UNLINK_OP}, _ClusterName) ->
    unlink.

decode_route_op(Payload) ->
    decode_route_op1(?DECODE(Payload)).

decode_route_op1(#{
    ?F_OPERATION := ?OP_ACTOR_INIT,
    ?F_ACTOR := Actor,
    ?F_INCARNATION := Incr
}) ->
    {actor_init, #{actor => Actor, incarnation => Incr}};
decode_route_op1(#{
    ?F_OPERATION := ?OP_ROUTE,
    ?F_ACTOR := Actor,
    ?F_INCARNATION := Incr,
    ?F_ROUTES := RouteOps
}) ->
    RouteOps1 = lists:map(fun(Op) -> decode_field(route, Op) end, RouteOps),
    {route_updates, #{actor => Actor, incarnation => Incr}, RouteOps1};
%%decode_route_op1(<<"add_", Topic/binary>>) ->
%%    {add, Topic};
%%decode_route_op1(<<"delete_", Topic/binary>>) ->
%%    {delete, Topic};
%%decode_route_op1(#{<<"op">> := ?BATCH_ROUTES_OP, <<"topics">> := Topics}) when is_list(Topics) ->
%%    {add, Topics};
%%decode_route_op1(#{<<"op">> := ?CLEANUP_ROUTES_OP}) ->
%%    cleanup_routes;
decode_route_op1(Payload) ->
    ?SLOG(warning, #{
        msg => "unexpected_cluster_link_route_op_payload",
        payload => Payload
    }),
    {error, Payload}.

decode_forwarded_msg(Payload) ->
    case ?DECODE(Payload) of
        #message{} = Msg ->
            Msg;
        _ ->
            ?SLOG(warning, #{
                msg => "unexpected_cluster_link_forwarded_msg_payload",
                payload => Payload
            }),
            {error, Payload}
    end.

decode_proto_ver(ProtoVer, ClusterName) ->
    {MyMajor, MyMinor} = decode_proto_ver1(?PROTO_VER),
    case decode_proto_ver1(ProtoVer) of
        {Major, Minor} = Res when
            Major > MyMajor;
            Minor > MyMinor
        ->
            ?SLOG(notice, #{
                msg => "different_cluster_link_protocol_versions",
                protocol_version => ?PROTO_VER,
                link_protocol_version => ProtoVer,
                link_name => ClusterName
            }),
            Res;
        Res ->
            Res
    end.

decode_proto_ver1(ProtoVer) ->
    [Major, Minor] = binary:split(ProtoVer, <<".">>),
    %% Let it fail (for now), we don't expect invalid data to pass through the linking protocol..
    {emqx_utils_conv:int(Major), emqx_utils_conv:int(Minor)}.

encode_field(route, {add, Route = {_Topic, _ID}}) ->
    Route;
encode_field(route, {delete, {Topic, ID}}) ->
    {?ROUTE_DELETE, Topic, ID}.

decode_field(route, {?ROUTE_DELETE, Route = {_Topic, _ID}}) ->
    {delete, Route};
decode_field(route, Route = {_Topic, _ID}) ->
    {add, Route}.

%%--------------------------------------------------------------------
%% emqx_external_broker
%%--------------------------------------------------------------------

forward({external, {link, ClusterName}}, #delivery{message = #message{topic = Topic} = Msg}) ->
    QueryOpts = #{pick_key => Topic},
    emqx_resource:query(?MSG_RES_ID(ClusterName), Msg, QueryOpts).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

emqtt_client_opts(
    ClientIdSuffix, #{server := Server, ssl := #{enable := EnableSsl} = Ssl} = ClusterConf
) ->
    BaseClientId = maps:get(client_id, ClusterConf, ?MY_CLUSTER_NAME),
    ClientId = ?CLIENTID(BaseClientId, ClientIdSuffix),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?MQTT_HOST_OPTS),
    Opts = #{
        host => Host,
        port => Port,
        clientid => ClientId,
        proto_ver => v5,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    },
    with_password(with_user(Opts, ClusterConf), ClusterConf).

with_user(Opts, #{username := U} = _ClusterConf) ->
    Opts#{username => U};
with_user(Opts, _ClusterConf) ->
    Opts.

with_password(Opts, #{password := P} = _ClusterConf) ->
    Opts#{password => emqx_secret:unwrap(P)};
with_password(Opts, _ClusterConf) ->
    Opts.

start_pool(PoolName, ClientIdSuffix, #{pool_size := PoolSize} = ClusterConf) ->
    ClientOpts = emqtt_client_opts(ClientIdSuffix, ClusterConf),
    Opts = [
        {name, PoolName},
        {pool_size, PoolSize},
        {pool_type, hash},
        {client_opts, ClientOpts}
    ],
    ecpool:start_sup_pool(PoolName, ?MODULE, Opts).
