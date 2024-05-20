%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_router_syncer).

-include_lib("emqtt/include/emqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_cluster_link.hrl").

%% API
-export([start_link/1]).

-export([
    push/4,
    push_persistent_route/4
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
-define(ACTOR_REF(Cluster), {via, gproc, ?NAME(Cluster, actor)}).

-define(MAX_BATCH_SIZE, 4000).
-define(MIN_SYNC_INTERVAL, 10).
-define(ERROR_DELAY, 200).

-define(RECONNECT_TIMEOUT, 5_000).
-define(ACTOR_REINIT_TIMEOUT, 7000).
-define(HEARTBEAT_INTERVAL, 10_000).

-define(CLIENT_SUFFIX, ":routesync:").
-define(PS_CLIENT_SUFFIX, ":routesync-ps:").

%% Special actor for persistent routes that has the same actor name on all nodes.
%% Node actors with the same name  nay race with each other (e.g. during bootstrap),
%% but it must be tolerable, since persistent route destination is a client ID,
%% which is unique cluster-wide.
-define(PS_ACTOR, <<"ps-routes-v1">>).
-define(PS_INCARNATION, 0).
-define(PS_ACTOR_REF(Cluster), {via, gproc, ?NAME(Cluster, ps_actor)}).
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

%% Supervisor:
%% 1. Actor + MQTT Client
%% 2. Syncer

start_link(TargetCluster) ->
    supervisor:start_link(?REF(TargetCluster), ?MODULE, {sup, TargetCluster}).

%% Actor

start_link_actor(ActorRef, Actor, Incarnation, TargetCluster) ->
    gen_server:start_link(
        ActorRef,
        ?MODULE,
        {actor, mk_state(TargetCluster, Actor, Incarnation)},
        []
    ).

get_actor_id() ->
    atom_to_binary(node()).

get_actor_incarnation() ->
    persistent_term:get({?MODULE, incarnation}).

set_actor_incarnation(Incarnation) ->
    ok = persistent_term:put({?MODULE, incarnation}, Incarnation),
    Incarnation.

ensure_actor_incarnation() ->
    try
        get_actor_incarnation()
    catch
        error:badarg ->
            %% TODO: Subject to clock skew, need something more robust.
            Incarnation = erlang:system_time(millisecond),
            set_actor_incarnation(Incarnation)
    end.

%% MQTT Client

start_link_client(TargetCluster, Actor) ->
    Options = emqx_cluster_link_config:emqtt_options(TargetCluster),
    case emqtt:start_link(refine_client_options(Options, Actor)) of
        {ok, Pid} ->
            case emqtt:connect(Pid) of
                {ok, _Props} ->
                    {ok, Pid};
                Error ->
                    Error
            end;
        Error ->
            Error
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
        initial_state => closed,
        batch_handler => {?MODULE, process_syncer_batch, [ClientName, Actor, Incarnation]}
        %% TODO: enable_replies => false
    }).

close_syncer(TargetCluster, ?PS_ACTOR) ->
    emqx_router_syncer:close(?PS_SYNCER_REF(TargetCluster));
close_syncer(TargetCluster, _Actor) ->
    emqx_router_syncer:close(?SYNCER_REF(TargetCluster)).

open_syncer(TargetCluster, ?PS_ACTOR) ->
    emqx_router_syncer:open(?PS_SYNCER_REF(TargetCluster));
open_syncer(TargetCluster, _Actor) ->
    emqx_router_syncer:open(?SYNCER_REF(TargetCluster)).

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
    publish_routes(gproc:where(ClientName), Actor, Incarnation, Updates).

batch_get_opname(Op) ->
    element(1, Op).

%%

init({sup, TargetCluster}) ->
    %% FIXME: Intensity.
    SupFlags = #{
        %% TODO: one_for_one?
        strategy => one_for_all,
        intensity => 10,
        period => 60
    },
    Children = [
        child_spec(actor, TargetCluster),
        child_spec(ps_actor, TargetCluster)
    ],
    {ok, {SupFlags, Children}};
init({actor, State}) ->
    init_actor(State).

child_spec(actor, TargetCluster) ->
    %% Actor process.
    %% Wraps MQTT Client process.
    %% ClientID: `mycluster:emqx1@emqx.local:routesync`
    %% Occasional TCP/MQTT-level disconnects are expected, and should be handled
    %% gracefully.
    Actor = get_actor_id(),
    Incarnation = ensure_actor_incarnation(),
    actor_spec(actor, ?ACTOR_REF(TargetCluster), Actor, Incarnation, TargetCluster);
child_spec(ps_actor, TargetCluster) ->
    actor_spec(ps_actor, ?PS_ACTOR_REF(TargetCluster), ?PS_ACTOR, ?PS_INCARNATION, TargetCluster).

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

actor_spec(ChildID, ActorRef, Actor, Incarnation, TargetCluster) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link_actor, [ActorRef, Actor, Incarnation, TargetCluster]},
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

%%

-record(st, {
    target :: binary(),
    actor :: binary(),
    incarnation :: non_neg_integer(),
    client :: {pid(), reference()},
    bootstrapped :: boolean(),
    reconnect_timer :: reference(),
    heartbeat_timer :: reference(),
    actor_init_req_id :: binary(),
    actor_init_timer :: reference(),
    remote_actor_info :: undefined | map(),
    status :: connecting | connected | disconnected,
    error :: undefined | term()
}).

mk_state(TargetCluster, Actor, Incarnation) ->
    #st{
        target = TargetCluster,
        actor = Actor,
        incarnation = Incarnation,
        bootstrapped = false,
        status = connecting
    }.

init_actor(State = #st{}) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, State, {continue, connect}}.

handle_continue(connect, State) ->
    {noreply, process_connect(State)}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', ClientPid, Reason}, St = #st{client = ClientPid}) ->
    {noreply, handle_client_down(Reason, St)};
handle_info(
    {publish, #{payload := Payload, properties := #{'Correlation-Data' := ReqId}}},
    St = #st{actor_init_req_id = ReqId}
) ->
    {actor_init_ack, #{result := Res, need_bootstrap := NeedBootstrap} = AckInfoMap} = emqx_cluster_link_mqtt:decode_resp(
        Payload
    ),
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
            {noreply, St1#st{error = Reason, status = disconnected}}
    end;
handle_info({publish, #{}}, St) ->
    {noreply, St};
handle_info({timeout, TRef, reconnect}, St = #st{reconnect_timer = TRef}) ->
    {noreply, process_connect(St#st{reconnect_timer = undefined})};
handle_info({timeout, TRef, actor_reinit}, St = #st{actor_init_timer = TRef}) ->
    ?SLOG(error, #{
        msg => "remote_actor_init_timeout",
        target_cluster => St#st.target,
        actor => St#st.actor
    }),
    Reason = init_timeout,
    _ = maybe_alarm(Reason, St),
    {noreply,
        init_remote_actor(St#st{reconnect_timer = undefined, status = disconnected, error = Reason})};
handle_info({timeout, TRef, _Heartbeat}, St = #st{heartbeat_timer = TRef}) ->
    {noreply, process_heartbeat(St#st{heartbeat_timer = undefined})};
%% Stale timeout.
handle_info({timeout, _, _}, St) ->
    {noreply, St};
handle_info(Info, St) ->
    ?SLOG(warning, #{msg => "unexpected_info", info => Info}),
    {noreply, St}.

terminate(_Reason, _State) ->
    ok.

process_connect(St = #st{target = TargetCluster, actor = Actor}) ->
    case start_link_client(TargetCluster, Actor) of
        {ok, ClientPid} ->
            _ = maybe_deactivate_alarm(St),
            ok = announce_client(Actor, TargetCluster, ClientPid),
            init_remote_actor(St#st{client = ClientPid});
        {error, Reason} ->
            handle_connect_error(Reason, St)
    end.

init_remote_actor(
    St = #st{target = TargetCluster, client = ClientPid, actor = Actor, incarnation = Incr}
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
    St1 =
        case Res of
            ok ->
                St#st{status = connecting};
            {error, Reason} ->
                ?SLOG(error, #{
                    msg => "cluster_link_init_failed",
                    reason => Reason,
                    target_cluster => TargetCluster,
                    actor => Actor
                }),
                _ = maybe_alarm(Reason, St),
                St#st{error = Reason, status = disconnected}
        end,
    TRef = erlang:start_timer(?ACTOR_REINIT_TIMEOUT, self(), actor_reinit),
    St1#st{actor_init_req_id = ReqId, actor_init_timer = TRef}.

post_actor_init(
    St = #st{client = ClientPid, target = TargetCluster, actor = Actor, incarnation = Incr},
    NeedBootstrap
) ->
    ok = start_syncer(TargetCluster, Actor, Incr),
    NSt = schedule_heartbeat(St#st{client = ClientPid}),
    process_bootstrap(NSt, NeedBootstrap).

handle_connect_error(Reason, St) ->
    ?SLOG(error, #{
        msg => "cluster_link_connection_failed",
        reason => Reason,
        target_cluster => St#st.target,
        actor => St#st.actor
    }),
    TRef = erlang:start_timer(?RECONNECT_TIMEOUT, self(), reconnect),
    _ = maybe_alarm(Reason, St),
    St#st{reconnect_timer = TRef, error = Reason, status = disconnected}.

handle_client_down(Reason, St = #st{target = TargetCluster, actor = Actor}) ->
    ?SLOG(error, #{
        msg => "cluster_link_connection_failed",
        reason => Reason,
        target_cluster => St#st.target,
        actor => St#st.actor
    }),
    %% TODO: syncer may be already down due to one_for_all strategy
    ok = close_syncer(TargetCluster, Actor),
    _ = maybe_alarm(Reason, St),
    process_connect(St#st{client = undefined, error = Reason, status = connecting}).

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
    TRef = erlang:start_timer(?HEARTBEAT_INTERVAL, self(), heartbeat),
    St#st{heartbeat_timer = TRef}.

%% Bootstrapping.
%% Responsible for transferring local routing table snapshot to the target
%% cluster. Does so either during the initial startup or when MQTT connection
%% is re-established with a clean session. Once bootstrapping is done, it
%% opens the syncer.

run_bootstrap(St = #st{target = TargetCluster, actor = ?PS_ACTOR}) ->
    case mria_config:whoami() of
        Role when Role /= replicant ->
            Opts = #{is_persistent_route => true},
            Bootstrap = emqx_cluster_link_router_bootstrap:init(TargetCluster, Opts),
            run_bootstrap(Bootstrap, St);
        _ ->
            process_bootstrapped(St)
    end;
run_bootstrap(St = #st{target = TargetCluster}) ->
    Bootstrap = emqx_cluster_link_router_bootstrap:init(TargetCluster, #{}),
    run_bootstrap(Bootstrap, St).

run_bootstrap(Bootstrap, St) ->
    case emqx_cluster_link_router_bootstrap:next_batch(Bootstrap) of
        done ->
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

process_bootstrapped(St = #st{target = TargetCluster, actor = Actor}) ->
    ok = open_syncer(TargetCluster, Actor),
    St#st{bootstrapped = true}.

process_bootstrap_batch(Batch, #st{client = ClientPid, actor = Actor, incarnation = Incarnation}) ->
    publish_routes(ClientPid, Actor, Incarnation, Batch).

ensure_bootstrap_heartbeat(St = #st{heartbeat_timer = TRef}) ->
    case erlang:read_timer(TRef) of
        false ->
            ok = emqx_utils:cancel_timer(TRef),
            process_heartbeat(St);
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
