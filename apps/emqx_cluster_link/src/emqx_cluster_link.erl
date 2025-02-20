%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link).

-behaviour(emqx_external_broker).

-export([
    is_registered/0,
    register_external_broker/0,
    unregister_external_broker/0,
    add_route/1,
    delete_route/1,
    add_shared_route/2,
    delete_shared_route/2,
    add_persistent_route/2,
    delete_persistent_route/2,
    add_persistent_shared_route/3,
    delete_persistent_shared_route/3,
    forward/1
]).

%% emqx hooks
-export([
    put_hook/0,
    delete_hook/0,
    on_message_publish/1
]).

%% Internal exports
-export([do_handle_route_op_msg/1]).

-include("emqx_cluster_link.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%--------------------------------------------------------------------
%% emqx_external_broker API
%%--------------------------------------------------------------------

is_registered() ->
    emqx_external_broker:provider() =:= ?MODULE.

register_external_broker() ->
    case is_registered() of
        true -> ok;
        false -> emqx_external_broker:register_provider(?MODULE)
    end.

unregister_external_broker() ->
    emqx_external_broker:unregister_provider(?MODULE).

%% Using original Topic as Route ID in the most common scenario:
%% (non-shared, non-persistent routes).
%% Original Topic is used to identify the route and  be able
%% to delete it on a remote cluster.
%% There is no need to push Node name as this info can be derived from
%% agent state on the remote cluster.
add_route(Topic) ->
    emqx_cluster_link_router:push_update(add, Topic, Topic).

delete_route(Topic) ->
    emqx_cluster_link_router:push_update(delete, Topic, Topic).

add_shared_route(Topic, Group) ->
    RouteID = ?SHARED_ROUTE_ID(Topic, Group),
    emqx_cluster_link_router:push_update(add, Topic, RouteID).

delete_shared_route(Topic, Group) ->
    RouteID = ?SHARED_ROUTE_ID(Topic, Group),
    emqx_cluster_link_router:push_update(delete, Topic, RouteID).

add_persistent_route(Topic, ID) ->
    RouteID = ?PERSISTENT_ROUTE_ID(Topic, ID),
    emqx_cluster_link_router:push_update_persistent(add, Topic, RouteID).

delete_persistent_route(Topic, ID) ->
    RouteID = ?PERSISTENT_ROUTE_ID(Topic, ID),
    emqx_cluster_link_router:push_update_persistent(delete, Topic, RouteID).

add_persistent_shared_route(Topic, Group, ID) ->
    RouteID = ?PERSISTENT_SHARED_ROUTE_ID(Topic, Group, ID),
    emqx_cluster_link_router:push_update_persistent(add, Topic, RouteID).

delete_persistent_shared_route(Topic, Group, ID) ->
    RouteID = ?PERSISTENT_SHARED_ROUTE_ID(Topic, Group, ID),
    emqx_cluster_link_router:push_update_persistent(delete, Topic, RouteID).

forward(Delivery = #delivery{message = #message{topic = Topic, extra = Extra}}) ->
    case emqx_cluster_link_config:enabled_links() of
        [] ->
            %% Nowhere to forward, bail out right away.
            [];
        _Enabled when is_map_key(link_origin, Extra) ->
            %% Do not forward any external messages to other links.
            %% Only forward locally originated messages to all the relevant links, i.e. no gossip
            %% message forwarding.
            [];
        _Enabled ->
            Routes = emqx_cluster_link_extrouter:match_routes(Topic),
            forward(Routes, Delivery)
    end.

forward([], _Delivery) ->
    [];
forward(Routes, Delivery) ->
    lists:foldl(
        fun(#route{topic = To, dest = Cluster}, Acc) ->
            Result = emqx_cluster_link_mqtt:forward(Cluster, Delivery),
            [{Cluster, To, Result} | Acc]
        end,
        [],
        Routes
    ).

%%--------------------------------------------------------------------
%% EMQX Hooks
%%--------------------------------------------------------------------

on_message_publish(
    #message{topic = <<?ROUTE_TOPIC_PREFIX, _/binary>>} = Msg
) ->
    case handle_route_op_msg(Msg) of
        ok ->
            {stop, []};
        error ->
            %% Disconnect so that upstream agent starts anew
            Headers0 = Msg#message.headers,
            Headers = Headers0#{
                allow_publish => false,
                should_disconnect => true
            },
            StopMsg = emqx_message:set_headers(Headers, Msg),
            {stop, StopMsg}
    end;
on_message_publish(#message{topic = <<?MSG_TOPIC_PREFIX, ClusterName/binary>>, payload = Payload}) ->
    case emqx_cluster_link_mqtt:decode_forwarded_msg(Payload) of
        #message{} = ForwardedMsg ->
            {stop, maybe_filter_incoming_msg(ForwardedMsg, ClusterName)};
        _Err ->
            %% Just ignore it. It must be already logged by the decoder
            {stop, []}
    end;
on_message_publish(_Msg) ->
    ok.

put_hook() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_SYS_MSGS).

delete_hook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish, []}).

%%--------------------------------------------------------------------
%% Internal exports
%%--------------------------------------------------------------------

%% Exported only for mocking in tests
do_handle_route_op_msg(
    #message{topic = <<?ROUTE_TOPIC_PREFIX, ClusterName/binary>>, payload = Payload} = Msg
) ->
    case emqx_cluster_link_mqtt:decode_route_op(Payload) of
        {actor_init, Actor, InitInfo} ->
            Result = actor_init(ClusterName, Actor, InitInfo),
            _ = actor_init_ack(Actor, Result, Msg),
            ok;
        {route_updates, #{actor := Actor}, RouteOps} ->
            ok = update_routes(ClusterName, Actor, RouteOps);
        {heartbeat, #{actor := Actor}} ->
            ok = actor_heartbeat(ClusterName, Actor);
        {error, {unknown_payload, ParsedPayload}} ->
            ?SLOG(warning, #{
                msg => "unexpected_cluster_link_route_op_payload",
                payload => ParsedPayload
            })
    end,
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-define(PD_EXTROUTER_ACTOR, '$clink_extrouter_actor').
-define(PD_EXTROUTER_ACTOR_STATE, '$clink_extrouter_actor_state').

handle_route_op_msg(
    #message{topic = <<?ROUTE_TOPIC_PREFIX, ClusterName/binary>>} = Msg
) ->
    try
        ?MODULE:do_handle_route_op_msg(Msg)
    catch
        K:E:Stacktrace ->
            MyClusterName = emqx_cluster_link_config:cluster(),
            ?SLOG(error, #{
                msg => "cluster_link_routesync_protocol_error",
                kind => K,
                reason => E,
                stacktrace => Stacktrace,
                %% How this cluster names itself
                local_name => MyClusterName,
                %% How the remote cluster names itself
                received_from => ClusterName
            }),
            error
    end.

actor_init(
    ClusterName,
    #{actor := Actor, incarnation := Incr},
    #{
        target_cluster := TargetCluster,
        proto_ver := _
    }
) ->
    case emqx_cluster_link_config:link(ClusterName) of
        undefined ->
            ?SLOG(error, #{
                msg => "init_link_request_from_unknown_cluster",
                link_name => ClusterName
            }),
            %% Avoid atom error reasons, since they can be sent to the remote cluster,
            %% which will use safe binary_to_term decoding
            %% TODO: add error details?
            {error, <<"unknown_cluster">>};
        #{enable := true} = _LinkConf ->
            MyClusterName = emqx_cluster_link_config:cluster(),
            case MyClusterName of
                TargetCluster ->
                    Env = #{timestamp => erlang:system_time(millisecond)},
                    {ok, ActorSt} = emqx_cluster_link_extrouter:actor_init(
                        ClusterName, Actor, Incr, Env
                    ),
                    undefined = set_actor_state(ClusterName, Actor, ActorSt),
                    ok;
                _ ->
                    %% The remote cluster uses a different name to refer to this cluster
                    ?SLOG(error, #{
                        msg => "misconfigured_cluster_link_name",
                        %% How this cluster names itself
                        local_name => MyClusterName,
                        %% How the remote cluster names this local cluster
                        remote_name => TargetCluster,
                        %% How the remote cluster names itself
                        received_from => ClusterName
                    }),
                    {error, <<"bad_remote_cluster_link_name">>}
            end;
        #{enable := false} ->
            {error, <<"cluster_link_disabled">>}
    end.

actor_init_ack(#{actor := Actor}, Res, MsgIn) ->
    RespMsg = emqx_cluster_link_mqtt:actor_init_ack_resp_msg(Actor, Res, MsgIn),
    emqx_broker:publish(RespMsg).

update_routes(ClusterName, Actor, RouteOps) ->
    ActorSt = get_actor_state(ClusterName, Actor),
    lists:foreach(
        fun(RouteOp) ->
            _ = emqx_cluster_link_extrouter:actor_apply_operation(RouteOp, ActorSt)
        end,
        RouteOps
    ).

actor_heartbeat(ClusterName, Actor) ->
    case erlang:get(?PD_EXTROUTER_ACTOR_STATE) of
        undefined ->
            %% skip, only update when it is initialized
            %% otherwise will crash the init retires
            skip;
        _ ->
            Env = #{timestamp => erlang:system_time(millisecond)},
            ActorSt0 = get_actor_state(ClusterName, Actor),
            ActorSt = emqx_cluster_link_extrouter:actor_apply_operation(heartbeat, ActorSt0, Env),
            _ = update_actor_state(ActorSt)
    end,
    ok.

get_actor_state(ClusterName, Actor) ->
    {ClusterName, Actor} = erlang:get(?PD_EXTROUTER_ACTOR),
    erlang:get(?PD_EXTROUTER_ACTOR_STATE).

set_actor_state(ClusterName, Actor, ActorSt) ->
    _Undefined = erlang:put(?PD_EXTROUTER_ACTOR, {ClusterName, Actor}),
    update_actor_state(ActorSt).

update_actor_state(ActorSt) ->
    erlang:put(?PD_EXTROUTER_ACTOR_STATE, ActorSt).

%% let it crash if extra is not a map,
%% we don't expect the message to be forwarded from an older EMQX release,
%% that doesn't set extra = #{} by default.
with_sender_name(#message{extra = Extra} = Msg, ClusterName) when is_map(Extra) ->
    Msg#message{extra = Extra#{link_origin => ClusterName}}.

maybe_filter_incoming_msg(#message{topic = T} = Msg, ClusterName) ->
    %% Should prevent irrelevant messages from being dispatched in case
    %% the remote routing state lags behind the local config changes.
    #{enable := Enable, topics := Topics} = emqx_cluster_link_config:link(ClusterName),
    case Enable andalso emqx_topic:match_any(T, Topics) of
        true -> with_sender_name(Msg, ClusterName);
        false -> []
    end.
