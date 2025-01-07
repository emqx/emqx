%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_router_syncer).

-include_lib("emqtt/include/emqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_cluster_link.hrl").

%% API
-export([start_link/1]).

-export([
    push/4,
    push_persistent_route/4
]).

%% debug/test helpers
-export([
    status/1,
    where/1,
    where/2
]).

-export([
    start_link_actor/4,
    start_link_syncer/4
]).

%% Internal API / Syncer
-export([
    process_syncer_batch/4
]).

%% silence warning
%% -behaviour(supervisor).
-export([init/1]).

-behaviour(gen_server).
-export([
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(NAME(Cluster), {n, l, {?MODULE, Cluster}}).
-define(REF(Cluster), {via, gproc, ?NAME(Cluster)}).

-define(NAME(Cluster, What), {n, l, {?MODULE, Cluster, What}}).
-define(CLIENT_NAME(Cluster), ?NAME(Cluster, client)).
-define(SYNCER_NAME(Cluster), ?NAME(Cluster, syncer)).
-define(SYNCER_REF(Cluster), {via, gproc, ?SYNCER_NAME(Cluster)}).
-define(ACTOR_NAME(Cluster), ?NAME(Cluster, actor)).
-define(ACTOR_REF(Cluster), {via, gproc, ?ACTOR_NAME(Cluster)}).

-define(MAX_BATCH_SIZE, 4000).
-define(MIN_SYNC_INTERVAL, 10).
-define(ERROR_DELAY, 200).

-define(RECONNECT_TIMEOUT, 5_000).
-define(ACTOR_INIT_TIMEOUT, 7000).

-define(CLIENT_SUFFIX, ":routesync:").
-define(PS_CLIENT_SUFFIX, ":routesync-ps:").

%% Special actor for persistent routes that has the same actor name on all nodes.
%% Node actors with the same name  nay race with each other (e.g. during bootstrap),
%% but it must be tolerable, since persistent route destination is a client ID,
%% which is unique cluster-wide.
-define(PS_ACTOR, <<"ps-routes-v1">>).
-define(PS_ACTOR_REF(Cluster), {via, gproc, ?NAME(Cluster, ps_actor)}).
-define(PS_ACTOR_NAME(Cluster), ?NAME(Cluster, ps_actor)).
-define(PS_CLIENT_NAME(Cluster), ?NAME(Cluster, ps_client)).
-define(PS_SYNCER_REF(Cluster), {via, gproc, ?PS_SYNCER_NAME(Cluster)}).
-define(PS_SYNCER_NAME(Cluster), ?NAME(Cluster, ps_syncer)).

-define(SAFE_MQTT_PUB(Expr, ClientPid), ?SAFE_MQTT_PUB(Expr, ClientPid, ok)).
-define(SAFE_MQTT_PUB(Expr, ClientPid, OnSuccess),
    try Expr of
        {ok, #{reason_code := __RC}} when __RC < ?RC_UNSPECIFIED_ERROR ->
            OnSuccess;
        {ok, #{reason_code_name := __RCN}} ->
            {error, {mqtt, __RCN}};
        {error, __Reason} ->
            {error, __Reason}
    catch
        exit:__Reason ->
            {error, {client, ClientPid, __Reason}}
    end
).

-record(st, {
    target :: binary(),
    actor :: binary(),
    incarnation :: non_neg_integer(),
    client :: undefined | pid(),
    bootstrapped :: boolean(),
    reconnect_timer :: undefined | reference(),
    heartbeat_timer :: undefined | reference(),
    actor_init_req_id :: undefined | binary(),
    actor_init_timer :: undefined | reference(),
    remote_actor_info :: undefined | map(),
    status :: connecting | connected | disconnected,
    error :: undefined | term(),
    link_conf :: map()
}).

push(TargetCluster, OpName, Topic, ID) ->
    do_push(?SYNCER_NAME(TargetCluster), OpName, Topic, ID).

push_persistent_route(TargetCluster, OpName, Topic, ID) ->
    do_push(?PS_SYNCER_NAME(TargetCluster), OpName, Topic, ID).

do_push(SyncerName, OpName, Topic, ID) ->
    case gproc:where(SyncerName) of
        SyncerPid when is_pid(SyncerPid) ->
            emqx_router_syncer:push(SyncerPid, OpName, Topic, ID, #{});
        undefined ->
            dropped
    end.

%% Debug/test helpers
where(Cluster) ->
    where(actor, Cluster).

where(actor, Cluster) ->
    gproc:where(?ACTOR_NAME(Cluster));
where(ps_actor, Cluster) ->
    gproc:where(?PS_ACTOR_NAME(Cluster)).

status(Cluster) ->
    case where(actor, Cluster) of
        Pid when is_pid(Pid) ->
            #st{error = Err, status = Status} = sys:get_state(Pid),
            #{error => Err, status => Status};
        undefined ->
            undefined
    end.

%% Supervisor:
%% 1. Actor + MQTT Client
%% 2. Syncer

start_link(#{name := TargetCluster} = LinkConf) ->
    supervisor:start_link(?REF(TargetCluster), ?MODULE, {sup, LinkConf}).

%% Actor

new_incarnation() ->
    %% TODO: Subject to clock skew, need something more robust.
    erlang:system_time(millisecond).

start_link_actor(ActorRef, Actor, Incarnation, LinkConf) ->
    gen_server:start_link(
        ActorRef,
        ?MODULE,
        {actor, mk_state(LinkConf, Actor, Incarnation)},
        []
    ).

get_actor_id() ->
    atom_to_binary(node()).

%% MQTT Client

start_link_client(Actor, LinkConf) ->
    Options = emqx_cluster_link_config:mk_emqtt_options(LinkConf),
    case emqtt:start_link(refine_client_options(Options, Actor)) of
        {ok, Pid} ->
            try emqtt:connect(Pid) of
                {ok, _Props} ->
                    {ok, Pid};
                Error ->
                    _ = flush_link_signal(Pid),
                    Error
            catch
                exit:Reason ->
                    ?SLOG(error, #{
                        msg => "failed_to_connect_to_cluster",
                        reason => Reason,
                        options => Options,
                        actor => Actor
                    }),
                    _ = flush_link_signal(Pid),
                    {error, failed_to_connect}
            end;
        Error ->
            Error
    end.

flush_link_signal(Pid) ->
    receive
        {'EXIT', Pid, _} -> ok
    after 1 -> timeout
    end.

refine_client_options(Options = #{clientid := ClientID}, Actor) ->
    Suffix =
        case Actor of
            ?PS_ACTOR -> ?PS_CLIENT_SUFFIX;
            _ -> ?CLIENT_SUFFIX
        end,
    %% TODO: Reconnect should help, but it looks broken right now.
    Options#{
        clientid => emqx_bridge_mqtt_lib:clientid_base([ClientID, Suffix]),
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 60},
        retry_interval => 0
    }.

announce_client(Actor, TargetCluster, Pid) ->
    Name =
        case Actor of
            ?PS_ACTOR -> ?PS_CLIENT_NAME(TargetCluster);
            _ -> ?CLIENT_NAME(TargetCluster)
        end,
    true = gproc:reg_other(Name, Pid),
    ok.

publish_routes(ClientPid, Actor, Incarnation, Updates) ->
    ?SAFE_MQTT_PUB(
        emqx_cluster_link_mqtt:publish_route_sync(ClientPid, Actor, Incarnation, Updates),
        ClientPid,
        #{}
    ).

publish_heartbeat(ClientPid, Actor, Incarnation) ->
    %% NOTE: Fully asynchronous, no need for error handling.
    emqx_cluster_link_mqtt:publish_heartbeat(ClientPid, Actor, Incarnation).

%% Route syncer

start_syncer(TargetCluster, Actor, Incr) ->
    Spec = child_spec(syncer, Actor, Incr, TargetCluster),
    case supervisor:start_child(?REF(TargetCluster), Spec) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

start_link_syncer(Actor, Incarnation, SyncerRef, ClientName) ->
    emqx_router_syncer:start_link(SyncerRef, #{
        max_batch_size => ?MAX_BATCH_SIZE,
        min_sync_interval => ?MIN_SYNC_INTERVAL,
        error_delay => ?ERROR_DELAY,
        initial_state => suspended,
        batch_handler => {?MODULE, process_syncer_batch, [ClientName, Actor, Incarnation]}
        %% TODO: enable_replies => false
    }).

suspend_syncer(TargetCluster, ?PS_ACTOR) ->
    emqx_router_syncer:suspend(?PS_SYNCER_REF(TargetCluster));
suspend_syncer(TargetCluster, _Actor) ->
    emqx_router_syncer:suspend(?SYNCER_REF(TargetCluster)).

activate_syncer(TargetCluster, ?PS_ACTOR) ->
    emqx_router_syncer:activate(?PS_SYNCER_REF(TargetCluster));
activate_syncer(TargetCluster, _Actor) ->
    emqx_router_syncer:activate(?SYNCER_REF(TargetCluster)).

process_syncer_batch(Batch, ClientName, Actor, Incarnation) ->
    Updates = maps:fold(
        fun(Route, Op, Acc) ->
            OpName = batch_get_opname(Op),
            Entry = emqx_cluster_link_mqtt:encode_field(route, {OpName, Route}),
            [Entry | Acc]
        end,
        [],
        Batch
    ),
    Result = publish_routes(gproc:where(ClientName), Actor, Incarnation, Updates),
    ?tp(debug, clink_route_sync_complete, #{
        actor => {Actor, Incarnation},
        batch => Batch
    }),
    Result.

batch_get_opname(Op) ->
    element(1, Op).

%%

init({sup, LinkConf}) ->
    %% FIXME: Intensity.
    SupFlags = #{
        %% TODO: one_for_one?
        strategy => one_for_all,
        intensity => 10,
        period => 60
    },
    Children = lists:append([
        [child_spec(actor, LinkConf)],
        [child_spec(ps_actor, LinkConf) || emqx_persistent_message:is_persistence_enabled()]
    ]),
    {ok, {SupFlags, Children}};
init({actor, State}) ->
    init_actor(State).

child_spec(actor, #{name := TargetCluster} = LinkConf) ->
    %% Actor process.
    %% Wraps MQTT Client process.
    %% ClientID: `mycluster:emqx1@emqx.local:routesync`
    %% Occasional TCP/MQTT-level disconnects are expected, and should be handled
    %% gracefully.
    Actor = get_actor_id(),
    Incarnation = new_incarnation(),
    actor_spec(actor, ?ACTOR_REF(TargetCluster), Actor, Incarnation, LinkConf);
child_spec(ps_actor, #{name := TargetCluster, ps_actor_incarnation := Incr} = LinkConf) ->
    actor_spec(ps_actor, ?PS_ACTOR_REF(TargetCluster), ?PS_ACTOR, Incr, LinkConf).

child_spec(syncer, ?PS_ACTOR, Incarnation, TargetCluster) ->
    SyncerRef = ?PS_SYNCER_REF(TargetCluster),
    ClientName = ?PS_CLIENT_NAME(TargetCluster),
    syncer_spec(ps_syncer, ?PS_ACTOR, Incarnation, SyncerRef, ClientName);
child_spec(syncer, Actor, Incarnation, TargetCluster) ->
    %% Route syncer process.
    %% Initially starts in a "closed" state. Actor decides when to open it, i.e.
    %% when bootstrapping is done. Syncer crash means re-bootstrap is needed, so
    %% we just restart the actor in this case.
    SyncerRef = ?SYNCER_REF(TargetCluster),
    ClientName = ?CLIENT_NAME(TargetCluster),
    syncer_spec(syncer, Actor, Incarnation, SyncerRef, ClientName).

actor_spec(ChildID, ActorRef, Actor, Incarnation, LinkConf) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link_actor, [ActorRef, Actor, Incarnation, LinkConf]},
        restart => permanent,
        type => worker
    }.

syncer_spec(ChildID, Actor, Incarnation, SyncerRef, ClientName) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link_syncer, [Actor, Incarnation, SyncerRef, ClientName]},
        restart => permanent,
        type => worker
    }.

mk_state(#{name := TargetCluster} = LinkConf, Actor, Incarnation) ->
    #st{
        target = TargetCluster,
        actor = Actor,
        incarnation = Incarnation,
        bootstrapped = false,
        status = connecting,
        link_conf = LinkConf
    }.

init_actor(State = #st{}) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, State, {continue, connect}}.

handle_continue(connect, St) ->
    {noreply, process_connect(St)}.
handle_call(_Request, _From, St) ->
    {reply, ignored, St}.

handle_cast(_Request, St) ->
    {noreply, St}.

handle_info({'EXIT', ClientPid, Reason}, St = #st{client = ClientPid}) ->
    {noreply, handle_client_down(Reason, St)};
handle_info(
    {publish, #{payload := Payload, properties := #{'Correlation-Data' := ReqId}}},
    St = #st{actor_init_req_id = ReqId}
) ->
    {actor_init_ack,
        #{
            result := Res,
            need_bootstrap := NeedBootstrap
        } = AckInfoMap} = emqx_cluster_link_mqtt:decode_resp(Payload),
    St1 = St#st{
        actor_init_req_id = undefined, actor_init_timer = undefined, remote_actor_info = AckInfoMap
    },
    case Res of
        ok ->
            _ = maybe_deactivate_alarm(St),
            {noreply,
                post_actor_init(St1#st{error = undefined, status = connected}, NeedBootstrap)};
        Error ->
            Reason = error_reason(Error),
            ?SLOG(error, #{
                msg => "failed_to_init_link",
                reason => Reason,
                target_cluster => St#st.target,
                actor => St#st.actor,
                remote_link_proto_ver => maps:get(proto_ver, AckInfoMap, undefined)
            }),
            _ = maybe_alarm(Reason, St1),
            ?tp(
                debug,
                clink_handshake_error,
                #{actor => {St1#st.actor, St1#st.incarnation}, reason => Reason}
            ),
            St2 = ensure_reconnect_timer(St1#st{error = Reason, status = disconnected}),
            {noreply, St2}
    end;
handle_info({timeout, TRef, reconnect}, St = #st{reconnect_timer = TRef}) ->
    {noreply, process_connect(St#st{reconnect_timer = undefined})};
handle_info({timeout, TRef, actor_init_timeout}, St0 = #st{actor_init_timer = TRef}) ->
    ?tp(error, "remote_actor_init_timeout", #{
        target_cluster => St0#st.target,
        actor => St0#st.actor
    }),
    Reason = init_timeout,
    _ = maybe_alarm(Reason, St0),
    St1 = St0#st{actor_init_timer = undefined, status = disconnected, error = Reason},
    St = stop_link_client(St1),
    {noreply, ensure_reconnect_timer(St)};
handle_info({timeout, TRef, _Heartbeat}, St = #st{heartbeat_timer = TRef}) ->
    {noreply, process_heartbeat(St#st{heartbeat_timer = undefined})};
%% Stale timeout.
handle_info({timeout, _, _}, St) ->
    {noreply, St};
handle_info(Info, St) ->
    ?SLOG(warning, #{msg => "unexpected_info", info => Info}),
    {noreply, St}.

terminate(_Reason, State) ->
    _ = maybe_deactivate_alarm(State),
    ok.

process_connect(St = #st{target = TargetCluster, actor = Actor, link_conf = Conf}) ->
    case start_link_client(Actor, Conf) of
        {ok, ClientPid} ->
            _ = maybe_deactivate_alarm(St),
            ok = announce_client(Actor, TargetCluster, ClientPid),
            init_remote_actor(St#st{client = ClientPid});
        {error, Reason} ->
            handle_connect_error(Reason, St)
    end.

init_remote_actor(
    St0 = #st{target = TargetCluster, client = ClientPid, actor = Actor, incarnation = Incr}
) ->
    ReqId = emqx_utils_conv:bin(emqx_utils:gen_id(16)),
    %% TODO: handle subscribe errors
    {ok, _, _} = emqtt:subscribe(ClientPid, ?RESP_TOPIC(Actor), ?QOS_1),
    Res = ?SAFE_MQTT_PUB(
        emqx_cluster_link_mqtt:publish_actor_init_sync(
            ClientPid, ReqId, ?RESP_TOPIC(Actor), TargetCluster, Actor, Incr
        ),
        ClientPid
    ),
    case Res of
        ok ->
            TRef = erlang:start_timer(?ACTOR_INIT_TIMEOUT, self(), actor_init_timeout),
            St0#st{status = connecting, actor_init_req_id = ReqId, actor_init_timer = TRef};
        {error, Reason} ->
            ?tp(error, "cluster_link_init_failed", #{
                reason => Reason,
                target_cluster => TargetCluster,
                actor => Actor
            }),
            _ = maybe_alarm(Reason, St0),
            St = stop_link_client(St0#st{error = Reason, status = disconnected}),
            ensure_reconnect_timer(St)
    end.

post_actor_init(
    St = #st{client = ClientPid, target = TargetCluster, actor = Actor, incarnation = Incr},
    NeedBootstrap
) ->
    ok = start_syncer(TargetCluster, Actor, Incr),
    NSt = schedule_heartbeat(St#st{client = ClientPid}),
    process_bootstrap(NSt, NeedBootstrap).

ensure_reconnect_timer(#st{reconnect_timer = undefined} = St) ->
    TRef = erlang:start_timer(?RECONNECT_TIMEOUT, self(), reconnect),
    St#st{reconnect_timer = TRef};
ensure_reconnect_timer(#st{reconnect_timer = TRef} = St) ->
    _ = erlang:cancel_timer(TRef),
    ensure_reconnect_timer(St#st{reconnect_timer = undefined}).

handle_connect_error(Reason, St) ->
    ?SLOG(error, #{
        msg => "cluster_link_connection_failed",
        reason => Reason,
        target_cluster => St#st.target,
        actor => St#st.actor
    }),
    _ = maybe_alarm(Reason, St),
    ensure_reconnect_timer(St#st{error = Reason, status = disconnected}).

handle_client_down(
    Reason,
    St = #st{target = TargetCluster, actor = Actor, bootstrapped = Bootstrapped}
) ->
    ?tp(error, "cluster_link_connection_failed", #{
        reason => Reason,
        target_cluster => St#st.target,
        actor => St#st.actor
    }),
    %% NOTE: There's no syncer yet if bootstrap haven't finished.
    %% TODO: Syncer may be already down due to one_for_all strategy.
    _ = Bootstrapped andalso suspend_syncer(TargetCluster, Actor),
    _ = maybe_alarm(Reason, St),
    NSt = cancel_heartbeat(St),
    process_connect(NSt#st{client = undefined, error = Reason, status = connecting}).

stop_link_client(#st{client = ClientPid} = St0) ->
    ?tp("clink_stop_link_client", #{}),
    ok = emqtt:stop(ClientPid),
    flush_link_signal(ClientPid),
    St0#st{client = undefined}.

process_bootstrap(St = #st{bootstrapped = false}, _NeedBootstrap) ->
    run_bootstrap(St);
process_bootstrap(St = #st{bootstrapped = true}, NeedBootstrap) ->
    case NeedBootstrap of
        true ->
            run_bootstrap(St);
        false ->
            process_bootstrapped(St)
    end.

process_heartbeat(St = #st{client = ClientPid, actor = Actor, incarnation = Incarnation}) ->
    ok = publish_heartbeat(ClientPid, Actor, Incarnation),
    schedule_heartbeat(St).

schedule_heartbeat(St = #st{heartbeat_timer = undefined}) ->
    Timeout = emqx_cluster_link_config:actor_heartbeat_interval(),
    TRef = erlang:start_timer(Timeout, self(), heartbeat),
    St#st{heartbeat_timer = TRef}.

cancel_heartbeat(St = #st{heartbeat_timer = undefined}) ->
    St;
cancel_heartbeat(St = #st{heartbeat_timer = TRef}) ->
    ok = emqx_utils:cancel_timer(TRef),
    St#st{heartbeat_timer = undefined}.

%% Bootstrapping.
%% Responsible for transferring local routing table snapshot to the target
%% cluster. Does so either during the initial startup or when MQTT connection
%% is re-established with a clean session. Once bootstrapping is done, it
%% activates the syncer.

run_bootstrap(St = #st{target = TargetCluster, actor = ?PS_ACTOR, link_conf = #{topics := Topics}}) ->
    case mria_config:whoami() of
        Role when Role /= replicant ->
            Opts = #{is_persistent_route => true},
            Bootstrap = emqx_cluster_link_router_bootstrap:init(TargetCluster, Topics, Opts),
            run_bootstrap(Bootstrap, St);
        _ ->
            process_bootstrapped(St)
    end;
run_bootstrap(St = #st{target = TargetCluster, link_conf = #{topics := Topics}}) ->
    Bootstrap = emqx_cluster_link_router_bootstrap:init(TargetCluster, Topics, #{}),
    run_bootstrap(Bootstrap, St).

run_bootstrap(Bootstrap, St) ->
    case emqx_cluster_link_router_bootstrap:next_batch(Bootstrap) of
        done ->
            ?tp(
                debug,
                clink_route_bootstrap_complete,
                #{actor => {St#st.actor, St#st.incarnation}, cluster => St#st.target}
            ),
            process_bootstrapped(St);
        {Batch, NBootstrap} ->
            %% TODO: Better error handling.
            case process_bootstrap_batch(Batch, St) of
                #{} ->
                    NSt = ensure_bootstrap_heartbeat(St),
                    run_bootstrap(NBootstrap, NSt);
                {error, {client, _, _}} ->
                    %% Client has exited, let `reconnect` codepath handle it.
                    St
            end
    end.

process_bootstrapped(
    St = #st{target = TargetCluster, actor = Actor}
) ->
    ok = activate_syncer(TargetCluster, Actor),
    St#st{bootstrapped = true}.

process_bootstrap_batch(Batch, #st{client = ClientPid, actor = Actor, incarnation = Incarnation}) ->
    publish_routes(ClientPid, Actor, Incarnation, Batch).

ensure_bootstrap_heartbeat(St = #st{heartbeat_timer = TRef}) ->
    case erlang:read_timer(TRef) of
        false ->
            ok = emqx_utils:cancel_timer(TRef),
            process_heartbeat(St#st{heartbeat_timer = undefined});
        _TimeLeft ->
            St
    end.

%%

error_reason({error, Reason}) ->
    Reason;
error_reason(OtherErr) ->
    OtherErr.

%% Assume that alarm is already active
maybe_alarm(Error, #st{error = Error}) ->
    ok;
maybe_alarm(Error, St) ->
    HrError = emqx_utils:readable_error_msg(error_reason(Error)),
    Name = link_name(St),
    emqx_alarm:safe_activate(
        Name,
        #{custer_link => Name, reason => cluster_link_down},
        <<"cluster link down: ", HrError/binary>>
    ).

maybe_deactivate_alarm(#st{error = undefined}) ->
    ok;
maybe_deactivate_alarm(St) ->
    emqx_alarm:safe_deactivate(link_name(St)).

link_name(#st{actor = ?PS_ACTOR = Actor, target = Target}) ->
    <<"cluster_link:", Target/binary, ":", (get_actor_id())/binary, ":", Actor/binary>>;
link_name(#st{actor = Actor, target = Target}) ->
    <<"cluster_link:", Target/binary, ":", Actor/binary>>.
