%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link).

-behaviour(emqx_external_broker).

-export([
    register_external_broker/0,
    unregister_external_broker/0,
    add_route/1,
    delete_route/1,
    add_shared_route/2,
    delete_shared_route/2,
    add_persistent_route/2,
    delete_persistent_route/2,
    match_routes/1,
    forward/2,
    should_route_to_external_dests/1
]).

%% emqx hooks
-export([
    put_hook/0,
    delete_hook/0,
    on_message_publish/1
]).

-include("emqx_cluster_link.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% emqx_external_broker API
%%--------------------------------------------------------------------

register_external_broker() ->
    emqx_external_broker:register_provider(?MODULE).

unregister_external_broker() ->
    emqx_external_broker:unregister_provider(?MODULE).

%% Using original Topic as Route ID in the most common scenario:
%% (non-shared, non-persistent routes).
%% Original Topic is used to identify the route and  be able
%% to delete it on a remote cluster.
%% There is no need to push Node name as this info can be derived from
%% agent state on the remote cluster.
add_route(Topic) ->
    maybe_push_route_op(add, Topic, Topic).

delete_route(Topic) ->
    maybe_push_route_op(delete, Topic, Topic).

add_shared_route(Topic, Group) ->
    maybe_push_route_op(add, Topic, ?SHARED_ROUTE_ID(Topic, Group)).

delete_shared_route(Topic, Group) ->
    maybe_push_route_op(delete, Topic, ?SHARED_ROUTE_ID(Topic, Group)).

add_persistent_route(Topic, ID) ->
    maybe_push_route_op(add, Topic, ?PERSISTENT_ROUTE_ID(Topic, ID), push_persistent_route).

delete_persistent_route(Topic, ID) ->
    maybe_push_route_op(delete, Topic, ?PERSISTENT_ROUTE_ID(Topic, ID), push_persistent_route).

forward(DestCluster, Delivery) ->
    emqx_cluster_link_mqtt:forward(DestCluster, Delivery).

match_routes(Topic) ->
    emqx_cluster_link_extrouter:match_routes(Topic).

%% Do not forward any external messages to other links.
%% Only forward locally originated messages to all the relevant links, i.e. no gossip message forwarding.
should_route_to_external_dests(#message{extra = #{link_origin := _}}) ->
    false;
should_route_to_external_dests(_Msg) ->
    true.

%%--------------------------------------------------------------------
%% EMQX Hooks
%%--------------------------------------------------------------------

on_message_publish(
    #message{topic = <<?ROUTE_TOPIC_PREFIX, ClusterName/binary>>, payload = Payload} = Msg
) ->
    _ =
        case emqx_cluster_link_mqtt:decode_route_op(Payload) of
            {actor_init, InitInfoMap} ->
                actor_init(ClusterName, emqx_message:get_header(properties, Msg), InitInfoMap);
            {route_updates, #{actor := Actor, incarnation := Incr}, RouteOps} ->
                update_routes(ClusterName, Actor, Incr, RouteOps);
            {heartbeat, #{actor := Actor, incarnation := Incr}} ->
                actor_heartbeat(ClusterName, Actor, Incr)
        end,
    {stop, []};
on_message_publish(#message{topic = <<?MSG_TOPIC_PREFIX, ClusterName/binary>>, payload = Payload}) ->
    case emqx_cluster_link_mqtt:decode_forwarded_msg(Payload) of
        #message{} = ForwardedMsg ->
            {stop, with_sender_name(ForwardedMsg, ClusterName)};
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
%% Internal functions
%%--------------------------------------------------------------------

maybe_push_route_op(Op, Topic, RouteID) ->
    maybe_push_route_op(Op, Topic, RouteID, push).

maybe_push_route_op(Op, Topic, RouteID, PushFun) ->
    lists:foreach(
        fun(#{upstream := Cluster, topics := LinkFilters}) ->
            case topic_intersect_any(Topic, LinkFilters) of
                false ->
                    ok;
                TopicIntersection ->
                    emqx_cluster_link_router_syncer:PushFun(Cluster, Op, TopicIntersection, RouteID)
            end
        end,
        emqx_cluster_link_config:enabled_links()
    ).

topic_intersect_any(Topic, [LinkFilter | T]) ->
    case emqx_topic:intersection(Topic, LinkFilter) of
        false -> topic_intersect_any(Topic, T);
        TopicOrFilter -> TopicOrFilter
    end;
topic_intersect_any(_Topic, []) ->
    false.

actor_init(
    ClusterName,
    #{'Correlation-Data' := ReqId, 'Response-Topic' := RespTopic},
    #{
        actor := Actor,
        incarnation := Incr,
        cluster := TargetCluster,
        proto_ver := _
    }
) ->
    Res =
        case emqx_cluster_link_config:link(ClusterName) of
            undefined ->
                ?SLOG(
                    error,
                    #{
                        msg => "init_link_request_from_unknown_cluster",
                        link_name => ClusterName
                    }
                ),
                %% Avoid atom error reasons, since they can be sent to the remote cluster,
                %% which will use safe binary_to_term decoding
                %% TODO: add error details?
                {error, <<"unknown_cluster">>};
            LinkConf ->
                %% TODO: may be worth checking resource health and communicate it?
                _ = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(LinkConf),
                MyClusterName = emqx_cluster_link_config:cluster(),
                case MyClusterName of
                    TargetCluster ->
                        Env = #{timestamp => erlang:system_time(millisecond)},
                        emqx_cluster_link_extrouter:actor_init(ClusterName, Actor, Incr, Env);
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
                end
        end,
    _ = actor_init_ack(Actor, Res, ReqId, RespTopic),
    {stop, []}.

actor_init_ack(Actor, Res, ReqId, RespTopic) ->
    RespMsg = emqx_cluster_link_mqtt:actor_init_ack_resp_msg(Actor, Res, ReqId, RespTopic),
    emqx_broker:publish(RespMsg).

update_routes(ClusterName, Actor, Incarnation, RouteOps) ->
    ActorState = emqx_cluster_link_extrouter:actor_state(ClusterName, Actor, Incarnation),
    lists:foreach(
        fun(RouteOp) ->
            emqx_cluster_link_extrouter:actor_apply_operation(RouteOp, ActorState)
        end,
        RouteOps
    ).

actor_heartbeat(ClusterName, Actor, Incarnation) ->
    Env = #{timestamp => erlang:system_time(millisecond)},
    ActorState = emqx_cluster_link_extrouter:actor_state(ClusterName, Actor, Incarnation),
    _State = emqx_cluster_link_extrouter:actor_apply_operation(heartbeat, ActorState, Env).

%% let it crash if extra is not a map,
%% we don't expect the message to be forwarded from an older EMQX release,
%% that doesn't set extra = #{} by default.
with_sender_name(#message{extra = Extra} = Msg, ClusterName) when is_map(Extra) ->
    Msg#message{extra = Extra#{link_origin => ClusterName}}.
