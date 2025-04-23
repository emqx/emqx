%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Route replication across a cluster link.
%% This module implements "Route Replication Actor" and manages replication across
%% MQTT client channel. See also `emqx_cluster_link_extrouter` for details.
-module(emqx_cluster_link_routerepl).

-include("emqx_cluster_link.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Router API
-export([push/5]).

-export([start_link/3]).

%% Internal API / Route Syncer
-export([
    process_syncer_batch/4
]).

%% Internal API
-export([
    start_link_manager/3,
    start_link_syncer/4
]).

%% NOTE
%% This module runs both supervisor and `gen_statem`, and compiler does not like
%% overlapping behaviours.
%% -behaviour(supervisor).
-export([init/1]).

-behaviour(gen_statem).
-export([
    connecting/3,
    handshaking/3,
    bootstrapping/3,
    online/3,
    disconnected/3,
    callback_mode/0,
    terminate/2
]).

-define(REF(NAME), {via, gproc, NAME}).
-define(NAME(CLUSTER, ACTOR), {n, l, {?MODULE, CLUSTER, ACTOR}}).
-define(NAME(CLUSTER, ACTOR, WHAT), {n, l, {?MODULE, CLUSTER, ACTOR, WHAT}}).
-define(CLIENT_NAME(CLUSTER, ACTOR), ?NAME(CLUSTER, ACTOR, client)).
-define(SYNCER_NAME(CLUSTER, ACTOR), ?NAME(CLUSTER, ACTOR, syncer)).

-define(MAX_BATCH_SIZE, 4000).
-define(MIN_SYNC_INTERVAL, 10).
-define(ERROR_DELAY, 200).

-define(RECONNECT_TIMEOUT, 5_000).

-define(SAFE_MQTT_PUB(EXPR), ?SAFE_MQTT_PUB(EXPR, ok)).
-define(SAFE_MQTT_PUB(EXPR, ONSUCCESS),
    try EXPR of
        {ok, #{reason_code := __RC}} when __RC < ?RC_UNSPECIFIED_ERROR ->
            ONSUCCESS;
        {ok, #{reason_code_name := __RCN}} ->
            {error, {mqtt, __RCN}};
        {error, __Reason} ->
            {error, __Reason}
    catch
        exit:__Reason ->
            {error, {exit, __Reason}}
    end
).

%%

%% @doc Replicate a route addition or removal, where route is annotated with unique
%% route ID.
push(TargetCluster, Actor, OpName, Topic, ID) ->
    push_to(?SYNCER_NAME(TargetCluster, Actor), OpName, Topic, ID).

push_to(SyncerName, OpName, Topic, ID) ->
    case gproc:where(SyncerName) of
        SyncerPid when is_pid(SyncerPid) ->
            emqx_router_syncer:push(SyncerPid, OpName, Topic, ID, #{});
        undefined ->
            dropped
    end.

%% Supervisor

%% @doc Starts route replication supervisor.
%% This supervisor manages 2 processes:
%% 1. Route replication manager.
%%    Owns MQTT client used as a channel for route replication, manages its lifecycle,
%%    conducts bootstrapping.
%% 2. Route syncer.
%%    Handles accumulation / batching / deduplication of routes to be replicated, and
%%    communication across MQTT client channel according to Cluster Link Route
%%    Replication protocol, and basic error handling.
%% Configured with a "Route Replication Actor" identifier, an MFA supplying actor and
%% protocol details (e.g. "Incarnation"), and a Cluster Link.
start_link(Actor, ActorMF, #{name := TargetCluster} = LinkConf) ->
    SupName = ?NAME(TargetCluster, Actor),
    supervisor:start_link(?REF(SupName), ?MODULE, {sup, Actor, ActorMF, LinkConf}).

start_syncer(TargetCluster, Actor, Incarnation) ->
    SupName = ?NAME(TargetCluster, Actor),
    Spec = child_spec_syncer(TargetCluster, Actor, Incarnation),
    case supervisor:start_child(?REF(SupName), Spec) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

%% Manager process.
%% Owns MQTT Client process.
%% ClientID: `mycluster:emqx1@emqx.local:routesync`
%% Occasional TCP/MQTT-level disconnects are expected, and should be handled
%% gracefully.

start_link_manager(Actor, ActorMF, LinkConf) ->
    gen_statem:start_link(
        ?MODULE,
        {manager, mk_state(Actor, ActorMF, LinkConf)},
        []
    ).

%% Route syncer process.
%% Initially starts in a "closed" state. Manager decides when to open it, i.e.
%% when bootstrapping is done. Syncer crash means re-bootstrap is needed, so
%% we just restart the manager in this case.

start_link_syncer(TargetCluster, Actor, Incarnation, ClientName) ->
    SyncerName = ?SYNCER_NAME(TargetCluster, Actor),
    ActorName = atom_to_binary(Actor),
    emqx_router_syncer:start_link(?REF(SyncerName), #{
        max_batch_size => ?MAX_BATCH_SIZE,
        min_sync_interval => ?MIN_SYNC_INTERVAL,
        error_delay => ?ERROR_DELAY,
        initial_state => suspended,
        batch_handler => {?MODULE, process_syncer_batch, [ClientName, ActorName, Incarnation]}
        %% TODO: enable_replies => false
    }).

suspend_syncer(TargetCluster, Actor) ->
    emqx_router_syncer:suspend(?REF(?SYNCER_NAME(TargetCluster, Actor))).

activate_syncer(TargetCluster, Actor) ->
    emqx_router_syncer:activate(?REF(?SYNCER_NAME(TargetCluster, Actor))).

process_syncer_batch(Batch, ClientName, ActorName, Incarnation) ->
    Updates = maps:fold(
        fun(Route, Op, Acc) ->
            OpName = batch_get_opname(Op),
            Entry = emqx_cluster_link_mqtt:encode_field(route, {OpName, Route}),
            [Entry | Acc]
        end,
        [],
        Batch
    ),
    Result = publish_routes(gproc:where(ClientName), ActorName, Incarnation, Updates),
    ?tp(debug, "cluster_link_route_sync_complete", #{
        actor => ActorName,
        incarnation => Incarnation,
        batch => Batch
    }),
    Result.

batch_get_opname(Op) ->
    element(1, Op).

%%

init({sup, Actor, ActorMF, LinkConf}) ->
    init_supervisor(Actor, ActorMF, LinkConf);
init({manager, State}) ->
    init_manager(State).

init_supervisor(Actor, ActorMF, LinkConf) ->
    %% NOTE: Propagate crashes to the parent supervisor.
    SupFlags = #{
        %% TODO: one_for_one?
        strategy => one_for_all,
        intensity => 1,
        period => 1
    },
    Children = [
        child_spec_manager(Actor, ActorMF, LinkConf)
    ],
    {ok, {SupFlags, Children}}.

child_spec_manager(Actor, ActorMF, LinkConf) ->
    #{
        id => manager,
        start => {?MODULE, start_link_manager, [Actor, ActorMF, LinkConf]},
        restart => permanent,
        type => worker
    }.

child_spec_syncer(TargetCluster, Actor, Incarnation) ->
    ClientName = ?CLIENT_NAME(TargetCluster, Actor),
    #{
        id => syncer,
        start => {?MODULE, start_link_syncer, [TargetCluster, Actor, Incarnation, ClientName]},
        restart => permanent,
        type => worker
    }.

%% Manager implementation
%%
%% Manager is a state machine that moves between the following states:
%%                   .
%%                   │
%%                   ▼
%%   ╭─────────▶ Connecting ───────╮
%%   │               │             │
%%   │               ▼             │
%%   │   ╭───── Handshaking ───────┤
%%   │   │           │             │
%%   │   │           ▼             │
%%   │   │     Bootstrapping ──────┤
%%   │   │           │             │
%%   │   │           ▼             │
%%   │   ╰───────▶ Online ─────────┤
%%   │                             │
%%   │                             ▼
%%   ╰─────────────────────── Disconnected
%%

-record(st, {
    link :: emqx_cluster_link_schema:link(),
    actor :: atom(),
    incarnation :: non_neg_integer(),
    marker :: binary(),
    client :: undefined | pid(),
    handshake_reqid :: undefined | binary(),
    reconnect_at :: integer(),
    bootstrapped :: boolean(),
    heartbeat_timer :: undefined | reference()
}).

mk_state(Actor, ActorMF, LinkConf) ->
    Incarnation = apply_mf(ActorMF, incarnation),
    ClientMarker = apply_mf(ActorMF, marker),
    #st{
        link = LinkConf,
        actor = Actor,
        incarnation = Incarnation,
        marker = ClientMarker,
        reconnect_at = 0,
        bootstrapped = false
    }.

callback_mode() ->
    [state_functions].

init_manager(St = #st{}) ->
    ?tp("cluster_link_actor_init", #{actor => St#st.actor}),
    _ = erlang:process_flag(trap_exit, true),
    {ok, connecting, St, {next_event, internal, connect}}.

enter_connecting(St) ->
    {next_state, connecting, St, {next_event, internal, connect}}.

connecting(
    internal,
    connect,
    St0 = #st{link = Link, actor = Actor, marker = ClientMarker, client = undefined}
) ->
    TargetCluster = target_cluster(St0),
    ReconnectAt = erlang:system_time(millisecond) + ?RECONNECT_TIMEOUT,
    St = St0#st{reconnect_at = ReconnectAt},
    case start_link_client(Actor, ClientMarker, Link) of
        {ok, ClientPid} ->
            ok = announce_client(TargetCluster, Actor, ClientPid),
            enter_handshaking(St#st{client = ClientPid});
        {error, Reason} ->
            ?tp(error, "cluster_link_connection_failed", #{
                reason => Reason,
                target_cluster => TargetCluster,
                actor => Actor
            }),
            enter_disconnected(normal, St)
    end.

enter_handshaking(St) ->
    {next_state, handshaking, St, {next_event, internal, handshake}}.

handshaking(
    internal,
    handshake,
    St0 = #st{actor = Actor, incarnation = Incarnation, client = ClientPid}
) ->
    ReqId = gen_request_id(),
    Topic = ?RESP_TOPIC(local_cluster(), Actor),
    TargetCluster = target_cluster(St0),
    ActorName = atom_to_binary(Actor),
    Result = ?SAFE_MQTT_PUB(
        emqx_cluster_link_mqtt:publish_actor_init_sync(
            ClientPid, ReqId, Topic, TargetCluster, ActorName, Incarnation
        )
    ),
    case Result of
        ok ->
            St = St0#st{handshake_reqid = ReqId},
            Timeout = ?RECONNECT_TIMEOUT,
            {keep_state, St, {state_timeout, Timeout, abandon}};
        {error, Reason} ->
            ?tp(error, "cluster_link_handshake_failed", #{
                reason => Reason,
                target_cluster => TargetCluster,
                actor => Actor
            }),
            %% Ensure client is stopped, will move into `disconnected` once it is.
            ok = stop_link_client(St0),
            keep_state_and_data
    end;
handshaking(
    info,
    {publish, #{payload := Payload, properties := #{'Correlation-Data' := ReqId}}},
    St0 = #st{
        handshake_reqid = ReqId,
        actor = Actor,
        incarnation = Incarnation
    }
) ->
    TargetCluster = target_cluster(St0),
    {actor_init_ack, Ack} = emqx_cluster_link_mqtt:decode_resp(Payload),
    St = St0#st{handshake_reqid = undefined},
    case Ack of
        #{result := ok, need_bootstrap := NeedBootstrap} ->
            ok = start_syncer(TargetCluster, Actor, Incarnation),
            enter_bootstrap(NeedBootstrap, St);
        #{result := Error} ->
            ?tp(error, "cluster_link_handshake_rejected", #{
                reason => error_reason(Error),
                target_cluster => TargetCluster,
                actor => Actor,
                remote_link_proto_ver => maps:get(proto_ver, Ack, undefined)
            }),
            %% Stop client, will move into `disconnected` once it is.
            ok = stop_link_client(St),
            keep_state_and_data
    end;
handshaking(state_timeout, abandon, St = #st{actor = Actor}) ->
    ?tp(error, "cluster_link_handshake_timeout", #{
        target_cluster => target_cluster(St),
        actor => Actor
    }),
    %% Stop client, will move into `disconnected` once it is.
    ok = stop_link_client(St),
    keep_state_and_data;
handshaking(info, {'EXIT', ClientPid, Reason}, St = #st{client = ClientPid}) ->
    enter_disconnected(Reason, St);
handshaking(Type, Event, St) ->
    handle_event(Type, Event, St).

enter_bootstrap(_NeedBootstrap, St = #st{bootstrapped = false}) ->
    enter_bootstrapping(St);
enter_bootstrap(_NeedBootstrap = true, St = #st{bootstrapped = true}) ->
    enter_bootstrapping(St);
enter_bootstrap(_NeedBootstrap = false, St = #st{bootstrapped = true}) ->
    enter_bootstrapped(St).

enter_bootstrapped(St = #st{actor = Actor}) ->
    %% Bootstrap is complete.
    %% Activate route syncer: route ops accumulated since the start of bootstrap
    %% will start flowing across MQTT channel.
    ok = activate_syncer(target_cluster(St), Actor),
    enter_online(St#st{bootstrapped = true}).

enter_online(St) ->
    {next_state, online, schedule_heartbeat(St)}.

online(info, {timeout, _TRef, heartbeat}, St = #st{}) ->
    ok = heartbeat(St),
    {keep_state, schedule_heartbeat(St#st{heartbeat_timer = undefined})};
online(info, {'EXIT', ClientPid, Reason}, St = #st{client = ClientPid, actor = Actor}) ->
    %% Occasional client disconnects are expected.
    %% Suspend route syncer: it will start accumulating route ops in the state.
    %% Assuming this is transient condition. otherwise route syncer heap risks
    %% growing indefinitely.
    ok = suspend_syncer(target_cluster(St), Actor),
    enter_disconnected(Reason, cancel_heartbeat(St));
online(Type, Event, St) ->
    handle_event(Type, Event, St).

schedule_heartbeat(St = #st{heartbeat_timer = undefined}) ->
    Interval = emqx_cluster_link_config:actor_heartbeat_interval(),
    TRef = erlang:start_timer(Interval, self(), heartbeat),
    St#st{heartbeat_timer = TRef}.

cancel_heartbeat(St = #st{heartbeat_timer = undefined}) ->
    St;
cancel_heartbeat(St = #st{heartbeat_timer = TRef}) ->
    ok = emqx_utils:cancel_timer(TRef),
    St#st{heartbeat_timer = undefined}.

heartbeat(#st{client = ClientPid, actor = Actor, incarnation = Incarnation}) ->
    publish_heartbeat(ClientPid, Actor, Incarnation).

handle_disconnect(RC, St = #st{actor = Actor}) ->
    ?tp(info, "cluster_link_connection_disconnect", #{
        reason => emqx_reason_codes:name(RC),
        target_cluster => target_cluster(St),
        actor => Actor
    }),
    ok = stop_link_client(St),
    keep_state_and_data.

enter_disconnected(Reason, St0 = #st{actor = Actor, reconnect_at = ReconnectAt}) ->
    %% Reconnect no more than once every `?RECONNECT_TIMEOUT`.
    ReconnectIn = max(0, ReconnectAt - erlang:system_time(millisecond)),
    case Reason of
        %% Emit only debug message if stopped manually.
        normal -> Level = debug;
        %% Otherwise info message since interruptions are expected.
        _ -> Level = info
    end,
    ?tp(Level, "cluster_link_connection_down", #{
        reason => Reason,
        target_cluster => target_cluster(St0),
        actor => Actor,
        reconnect_in_ms => ReconnectIn
    }),
    St = St0#st{client = undefined},
    {next_state, disconnected, St, {next_event, internal, {retry, ReconnectIn}}}.

disconnected(internal, {retry, ReconnectIn}, St = #st{client = undefined}) when ReconnectIn > 0 ->
    {keep_state, St, {state_timeout, ReconnectIn, connect}};
disconnected(internal, {retry, _Now}, St = #st{client = undefined}) ->
    enter_connecting(St);
disconnected(state_timeout, connect, St) ->
    enter_connecting(St);
disconnected(info, {disconnected, _RC, _}, _St) ->
    keep_state_and_data;
disconnected(Type, Event, St) ->
    handle_event(Type, Event, St).

handle_event(info, {publish, _Uncorrelated = #{}}, _St) ->
    %% MQTT response not correlated to our handshake request.
    keep_state_and_data;
handle_event(info, {disconnected, RC, _}, St) ->
    %% MQTT disconnect packet.
    handle_disconnect(RC, St);
handle_event(Event, Payload, _St) ->
    ?tp(warning, "cluster_link_routerepl_unexpected_event", #{
        event => Event,
        payload => Payload
    }).

terminate(_Reason, _St) ->
    ok.

%% Bootstrapping.
%% Responsible for transferring local routing table snapshot to the target
%% cluster. Does so either during the initial startup or when MQTT connection
%% is re-established with a clean session. Once bootstrapping is done, it
%% activates the syncer.

enter_bootstrapping(St) ->
    {next_state, bootstrapping, St, {next_event, internal, go}}.

bootstrapping(internal, go, St = #st{}) ->
    bootstrap(St);
bootstrapping(info, {'EXIT', ClientPid, Reason}, St = #st{client = ClientPid}) ->
    enter_disconnected(Reason, St);
bootstrapping(Type, Event, St) ->
    handle_event(Type, Event, St).

bootstrap(St = #st{link = #{name := TargetCluster, topics := Topics}, actor = Actor}) ->
    Bootstrap = emqx_cluster_link_router_bootstrap:init(Actor, TargetCluster, Topics, #{}),
    HeartbeatTs = next_bootstrap_heartbeat(erlang:system_time(millisecond)),
    bootstrap(Bootstrap, HeartbeatTs, St).

bootstrap(Bootstrap, HeartbeatTs, St = #st{actor = Actor, incarnation = Incarnation}) ->
    TargetCluster = target_cluster(St),
    case emqx_cluster_link_router_bootstrap:next_batch(Bootstrap) of
        done ->
            ?tp(debug, "cluster_link_bootstrap_complete", #{
                target_cluster => TargetCluster,
                actor => Actor,
                incarnation => Incarnation
            }),
            enter_bootstrapped(St);
        {Batch, NBootstrap} ->
            %% TODO: Better error handling.
            case process_bootstrap_batch(Batch, St) of
                #{} ->
                    NHeartbeatTs = ensure_bootstrap_heartbeat(HeartbeatTs, St),
                    bootstrap(NBootstrap, NHeartbeatTs, St);
                {error, Reason} ->
                    ?tp(error, "cluster_link_bootstrap_failed", #{
                        reason => Reason,
                        target_cluster => TargetCluster,
                        actor => Actor
                    }),
                    %% Ensure client is stopped, will move into `disconnected` once it is.
                    ok = stop_link_client(St),
                    keep_state_and_data
            end
    end.

process_bootstrap_batch(
    Batch,
    #st{client = ClientPid, actor = Actor, incarnation = Incarnation}
) ->
    publish_routes(ClientPid, atom_to_binary(Actor), Incarnation, Batch).

ensure_bootstrap_heartbeat(HeartbeatTs, St) ->
    case erlang:system_time(millisecond) of
        Ts when Ts > HeartbeatTs ->
            ok = heartbeat(St),
            next_bootstrap_heartbeat(Ts);
        _TimeLeft ->
            St
    end.

next_bootstrap_heartbeat(Ts) ->
    Interval = emqx_cluster_link_config:actor_heartbeat_interval(),
    Ts + Interval.

%%

target_cluster(#st{link = #{name := TargetCluster}}) ->
    TargetCluster.

local_cluster() ->
    emqx_cluster_link_config:cluster().

%% MQTT Client

start_link_client(Actor, ClientMarker, Link) ->
    Options0 = emqx_cluster_link_config:mk_emqtt_options(Link),
    Options = refine_client_options(ClientMarker, Options0),
    case emqtt:start_link(Options) of
        {ok, Pid} ->
            try
                case emqtt:connect(Pid) of
                    {ok, _Props} ->
                        ?tp("cluster_link_actor_connected", #{actor => Actor}),
                        Topic = ?RESP_TOPIC(local_cluster(), Actor),
                        {ok, _, _} = emqtt:subscribe(Pid, Topic, ?QOS_1),
                        {ok, Pid};
                    Error ->
                        _ = flush_link_signal(Pid),
                        Error
                end
            catch
                exit:Reason ->
                    %% NOTE
                    %% For inexplicable reason exit signal may arrive earlier than
                    %% `disconnect` reply.
                    _ = flush_link_signal(Pid),
                    _ = flush_disconnect(),
                    {error, Reason}
            end;
        Error ->
            Error
    end.

stop_link_client(#st{client = ClientPid}) when is_pid(ClientPid) ->
    %% Stop the client, tolerate if it's dead / stopping right now.
    ?tp("cluster_link_stop_link_client", #{}),
    try
        emqtt:stop(ClientPid)
    catch
        exit:_ -> ok
    end.

flush_link_signal(Pid) ->
    receive
        {'EXIT', Pid, _} -> ok
    after 1 -> timeout
    end.

flush_disconnect() ->
    receive
        {disconnected, _RC, _} -> ok
    after 1 -> timeout
    end.

refine_client_options(ClientMarker, Options = #{clientid := ClientID}) ->
    %% TODO: Reconnect should help, but it looks broken right now.
    Options#{
        clientid => emqx_bridge_mqtt_lib:clientid_base([ClientID, ":", ClientMarker, ":"]),
        clean_start => true,
        retry_interval => 0
    }.

announce_client(TargetCluster, Actor, Pid) ->
    true = gproc:reg_other(?CLIENT_NAME(TargetCluster, Actor), Pid),
    ok.

publish_routes(ClientPid, ActorName, Incarnation, Updates) ->
    ?SAFE_MQTT_PUB(
        emqx_cluster_link_mqtt:publish_route_sync(ClientPid, ActorName, Incarnation, Updates),
        #{}
    ).

publish_heartbeat(ClientPid, Actor, Incarnation) ->
    %% NOTE: Fully asynchronous, no need for error handling.
    ActorName = atom_to_binary(Actor),
    emqx_cluster_link_mqtt:publish_heartbeat(ClientPid, ActorName, Incarnation).

%%

gen_request_id() ->
    emqx_utils_conv:bin(emqx_utils:gen_id(16)).

error_reason({error, Reason}) ->
    Reason;
error_reason(Other) ->
    Other.

apply_mf({Mod, Fun, Args}, Arg) ->
    apply(Mod, Fun, [Arg | Args]);
apply_mf(Fun, Arg) ->
    Fun(Arg).
