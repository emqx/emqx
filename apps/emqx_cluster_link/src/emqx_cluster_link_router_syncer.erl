%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_router_syncer).

-include_lib("emqtt/include/emqtt.hrl").

%% API
-export([start_link/1]).
-export([push/4]).

-export([
    start_link_actor/1,
    start_link_syncer/1
]).

%% Internal API / Syncer
-export([
    process_syncer_batch/4
]).

-behaviour(supervisor).
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

%%

push(TargetCluster, OpName, Topic, ID) ->
    case gproc:where(?SYNCER_NAME(TargetCluster)) of
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

start_link_actor(TargetCluster) ->
    Actor = get_actor_id(),
    Incarnation = ensure_actor_incarnation(),
    gen_server:start_link(
        ?ACTOR_REF(TargetCluster),
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

start_link_client(TargetCluster) ->
    Options = emqx_cluster_link_config:emqtt_options(TargetCluster),
    emqtt:start_link(refine_client_options(Options)).

refine_client_options(Options = #{clientid := ClientID}) ->
    %% TODO: Reconnect should help, but it looks broken right now.
    Options#{
        clientid => emqx_utils:format("~s:~s:routesync", [ClientID, node()]),
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 60},
        retry_interval => 0
    }.

client_session_present(ClientPid) ->
    Info = emqtt:info(ClientPid),
    proplists:get_value(session_present, Info, false).

announce_client(TargetCluster, Pid) ->
    true = gproc:reg_other(?CLIENT_NAME(TargetCluster), Pid),
    ok.

publish_routes(ClientPid, Actor, Incarnation, Updates) ->
    try emqx_cluster_link_mqtt:publish_route_sync(ClientPid, Actor, Incarnation, Updates) of
        {ok, #{reason_code := RC}} when RC < ?RC_UNSPECIFIED_ERROR ->
            #{};
        {ok, #{reason_code_name := RCN}} ->
            {error, {mqtt, RCN}};
        {error, Reason} ->
            {error, Reason}
    catch
        exit:Reason ->
            {error, {client, ClientPid, Reason}}
    end.

%% Route syncer

start_syncer(TargetCluster) ->
    case supervisor:start_child(?REF(TargetCluster), child_spec(syncer, TargetCluster)) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

start_link_syncer(TargetCluster) ->
    Actor = get_actor_id(),
    Incarnation = get_actor_incarnation(),
    ClientName = ?CLIENT_NAME(TargetCluster),
    emqx_router_syncer:start_link(?SYNCER_REF(TargetCluster), #{
        max_batch_size => ?MAX_BATCH_SIZE,
        min_sync_interval => ?MIN_SYNC_INTERVAL,
        error_delay => ?ERROR_DELAY,
        initial_state => closed,
        batch_handler => {?MODULE, process_syncer_batch, [ClientName, Actor, Incarnation]}
        %% TODO: enable_replies => false
    }).

close_syncer(TargetCluster) ->
    emqx_router_syncer:close(?SYNCER_REF(TargetCluster)).

open_syncer(TargetCluster) ->
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
        strategy => all_for_one,
        intensity => 10,
        period => 60
    },
    Children = [
        child_spec(actor, TargetCluster)
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
    #{
        id => actor,
        start => {?MODULE, start_link_actor, [TargetCluster]},
        restart => permanent,
        type => worker
    };
child_spec(syncer, TargetCluster) ->
    %% Route syncer process.
    %% Initially starts in a "closed" state. Actor decides when to open it, i.e.
    %% when bootstrapping is done. Syncer crash means re-bootstrap is needed, so
    %% we just restart the actor in this case.
    #{
        id => syncer,
        start => {?MODULE, start_link_syncer, [TargetCluster]},
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
    reconnect_timer :: reference()
}).

mk_state(TargetCluster, Actor, Incarnation) ->
    #st{
        target = TargetCluster,
        actor = Actor,
        incarnation = Incarnation,
        bootstrapped = false
    }.

init_actor(State = #st{}) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, State, {continue, connect}}.

handle_continue(connect, State) ->
    process_connect(State).

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', ClientPid, Reason}, St = #st{client = ClientPid}) ->
    handle_client_down(Reason, St);
handle_info({timeout, TRef, _Reconnect}, St = #st{reconnect_timer = TRef}) ->
    process_connect(St#st{reconnect_timer = undefined});
handle_info(_Info, St) ->
    %% TODO: log?
    {noreply, St}.

terminate(_Reason, _State) ->
    ok.

process_connect(St = #st{actor = TargetCluster}) ->
    case start_link_client(TargetCluster) of
        {ok, ClientPid} ->
            ok = start_syncer(TargetCluster),
            ok = announce_client(TargetCluster, ClientPid),
            process_bootstrap(St#st{client = ClientPid});
        {error, Reason} ->
            handle_connect_error(Reason, St)
    end.

handle_connect_error(Reason, St) ->
    %% TODO: logs
    TRef = erlang:start_timer(?RECONNECT_TIMEOUT, self(), reconnect),
    St#st{reconnect_timer = TRef}.

handle_client_down(Reason, St = #st{target = TargetCluster}) ->
    %% TODO: logs
    ok = close_syncer(TargetCluster),
    process_connect(St#st{client = undefined}).

process_bootstrap(St = #st{bootstrapped = false}) ->
    run_bootstrap(St);
process_bootstrap(St = #st{client = ClientPid, bootstrapped = true}) ->
    case client_session_present(ClientPid) of
        true ->
            process_bootstrapped(St);
        false ->
            run_bootstrap(St)
    end.

%% Bootstrapping.
%% Responsible for transferring local routing table snapshot to the target
%% cluster. Does so either during the initial startup or when MQTT connection
%% is re-established with a clean session. Once bootstrapping is done, it
%% opens the syncer.

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
                    run_bootstrap(NBootstrap, St);
                {error, {client, _, _}} ->
                    %% Client has exited, let `reconnect` codepath handle it.
                    St
            end
    end.

process_bootstrapped(St = #st{target = TargetCluster}) ->
    ok = open_syncer(TargetCluster),
    St#st{bootstrapped = true}.

process_bootstrap_batch(Batch, #st{client = ClientPid, actor = Actor, incarnation = Incarnation}) ->
    publish_routes(ClientPid, Actor, Incarnation, Batch).
