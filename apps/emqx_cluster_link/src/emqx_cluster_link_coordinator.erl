%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc experimental prototype implementation.
%% The idea is to add a sync point for all cluster route operations,
%% so that, routes can be batched/shrunk (via using emqx_route_syncer) before pushing them to linked clusters.
%% The expected result is reduced communication between linked clusters:
%% each nodes communicates with other clusters through coordinator.
%% The drawbacks are numerous though:
%%   - complexity/leader elections,
%%   - routes removal seems hard to implement unless remote cluster routes as stored per node,
%%     in that case global coordinator per cluster is not needed any more. - TBD
-module(emqx_cluster_link_coordinator).

-behaviour(gen_statem).

%% API
-export([
    route_op/2,
    on_link_ack/3
]).

-export([start_link/1]).

%% gen_statem
-export([
    callback_mode/0,
    init/1,
    terminate/3
]).

%% gen_statem state functions
-export([
    wait_for_coordinator/3,
    connecting/3,
    init_linking/3,
    bootstrapping/3,
    coordinating/3,
    following/3
]).

-export([select_routes/1]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("emqx/include/logger.hrl").

-define(COORDINATOR(UpstreamName), {?MODULE, UpstreamName}).
-define(SERVER, ?MODULE).
-define(WAIT_COORD_RETRY_INTERVAL, 100).
-define(CONN_RETRY_INTERVAL, 5000).
-define(INIT_LINK_RESP_TIMEOUT, 15_000).
-define(INIT_LINK_RETRIES, 5).
-define(UPSTREAM_DEST, {external, {link, _}}).
-define(IS_ROUTE_OP(Op), Op =:= <<"add">>; Op =:= <<"delete">>).

start_link(Conf) ->
    gen_statem:start_link(?MODULE, Conf, []).

route_op(Op, Topic) ->
    lists:foreach(
        fun(#{upstream := UpstreamName, topics := LinkFilters}) ->
            case topic_intersect_any(Topic, LinkFilters) of
                false -> ok;
                TopicOrFilter -> maybe_cast(UpstreamName, {Op, TopicOrFilter})
            end
        end,
        emqx:get_config([cluster, links])
    ).

on_link_ack(ClusterName, ReqId, Res) ->
    maybe_cast(ClusterName, {ack_link, ClusterName, ReqId, Res}).

callback_mode() ->
    [state_functions, state_enter].

init(LinkConf) ->
    process_flag(trap_exit, true),
    %% It helps to avoid unnecessary global name conflicts (and, as a result, coordinator re-election),
    %% e.g. when a down nodes comes back
    %% TODO: need to better understand `global` behaviour
    _ = global:sync(),
    Data = #{is_coordinator => false, link_conf => LinkConf},
    {ok, wait_for_coordinator, Data}.

wait_for_coordinator(enter, _OldState, _Data) ->
    {keep_state_and_data, [{state_timeout, 0, do_wait_for_coordinator}]};
wait_for_coordinator(_, do_wait_for_coordinator, Data) ->
    #{link_conf := #{upstream := Name}} = Data,
    case global:whereis_name(?COORDINATOR(Name)) of
        undefined ->
            case register_coordinator(Name) of
                yes ->
                    {next_state, connecting, Data#{is_coordinator => true}};
                no ->
                    %% TODO: this should not happen forever, if it does, we need to detect it
                    {keep_state_and_data, [
                        {state_timeout, ?WAIT_COORD_RETRY_INTERVAL, do_wait_for_coordinator}
                    ]}
            end;
        %% Can be a prev stale pid?
        %% Let it crash with case_clause if it happens...
        Pid when is_pid(Pid) andalso Pid =/= self() ->
            Data1 = Data#{coordinator_mon => erlang:monitor(process, Pid), coordinator_pid => Pid},
            {next_state, following, Data1}
    end;
wait_for_coordinator(cast, {Op, _Topic}, _Data) when ?IS_ROUTE_OP(Op) ->
    %% Ignore any route op, until bootstrapping is started.
    %% All ignored route ops are expected to be caught up during the bootstrap.
    keep_state_and_data;
wait_for_coordinator(EventType, Event, Data) ->
    handle_event_(?FUNCTION_NAME, EventType, Event, Data).

connecting(enter, _OldState, _Data) ->
    {keep_state_and_data, [{state_timeout, 0, reconnect}]};
connecting(cast, {Op, _Topic}, _Data) when ?IS_ROUTE_OP(Op) ->
    %% Ignore any route op, until bootstrapping is started.
    %% All ignored route ops are expected to be caught up during the bootstrap.
    keep_state_and_data;
connecting(_EventType, reconnect, Data) ->
    ensure_conn_pool(init_linking, Data);
connecting(EventType, Event, Data) ->
    handle_event_(?FUNCTION_NAME, EventType, Event, Data).

init_linking(enter, _OldState, Data) ->
    {keep_state, Data#{link_retries => ?INIT_LINK_RETRIES}, [{state_timeout, 0, init_link}]};
init_linking(cast, {ack_link, _ClusterName, ReqId, Res}, #{link_req_id := ReqId} = Data) ->
    case Res of
        %% This state machine is not suitable to bootstrap the upstream cluster conditionally,
        %% since it ignores any route ops received before bootstrapping...
        {ok, #{proto_ver := _, need_bootstrap := _}} ->
            {next_state, bootstrapping, maps:without([link_req_id, link_retries], Data)};
        {error, <<"bad_upstream_name">>} ->
            %% unrecoverable error that needs a user intervention,
            %% TODO: maybe need to transition to some error state
            {keep_state, maps:without([link_req_id, link_retries], Data), [{state_timeout, cancel}]}
    end;
init_linking(_, init_link, #{link_conf := #{upstream := Name}, link_retries := Retries} = Data) ->
    case Retries > 0 of
        true ->
            {ReqId, {ok, _}} = emqx_cluster_link_mqtt:init_link(Name),
            Data1 = Data#{link_req_id => ReqId, link_retries => Retries - 1},
            {keep_state, Data1, [{state_timeout, ?INIT_LINK_RESP_TIMEOUT, init_link}]};
        false ->
            ?SLOG(error, #{
                msg => "no_link_ack_response_received",
                link_name => Name
            }),
            %% unrecoverable error that needs a user intervention,
            %% TODO: maybe need to transition to some error state
            keep_state_and_data
    end;
init_linking(cast, {Op, _Topic}, _Data) when ?IS_ROUTE_OP(Op) ->
    %% Ignore any route op, until bootstrapping is started.
    %% All ignored route ops are expected to be caught up during the bootstrap.
    keep_state_and_data;
init_linking(EventType, Event, Data) ->
    handle_event_(?FUNCTION_NAME, EventType, Event, Data).

bootstrapping(enter, _OldState, #{link_conf := LinkConf} = Data) ->
    #{topics := LinkFilters, upstream := ClusterName} = LinkConf,
    %% TODO add timeout?
    {Pid, Ref} = erlang:spawn_monitor(fun() -> bootstrap(ClusterName, LinkFilters) end),
    {keep_state, Data#{bootstrap_pid => Pid, bootstrap_ref => Ref}};
bootstrapping(info, {'DOWN', Ref, process, _Pid, Reason}, #{bootstrap_ref := Ref} = Data) ->
    %% TODO: think about the best way to proceed if bootstrapping failed,
    %% perhaps just transition back to connecting state?
    normal = Reason,
    Data1 = maps:without([bootstrap_ref, bootstrap_pid], Data),
    {next_state, coordinating, Data1};
%% Accumulate new route ops, since there is no guarantee
%% they will be included in the bootstrapped data
bootstrapping(cast, {Op, _Topic}, _Data) when ?IS_ROUTE_OP(Op) ->
    {keep_state_and_data, [postpone]};
bootstrapping(EventType, Event, Data) ->
    handle_event_(?FUNCTION_NAME, EventType, Event, Data).

coordinating(enter, _OldState, _Data) ->
    keep_state_and_data;
coordinating(cast, {Op, Topic}, Data) when ?IS_ROUTE_OP(Op) ->
    #{link_conf := #{upstream := ClusterName}} = Data,
    %% TODO: batching
    case emqx_cluster_link_mqtt:publish_route_op(async, ClusterName, Op, Topic) of
        {error, _} ->
            %% Conn pool error, reconnect.
            {next_state, connecting, stop_conn_pool(Data)};
        _Ref ->
            keep_state_and_data
    end;
%% TODO: this can also be received in other states, move to generic handler?
coordinating(info, {global_name_conflict, CoordName}, Data) ->
    LogData = #{
        msg => "emqx_cluster_link_coordinator_name_conflict",
        coordinator_name => CoordName
    },
    LogData1 =
        %% TODO: this can be a previous (self) coordinator?
        case global:whereis_name(CoordName) of
            undefined -> LogData;
            Pid -> LogData#{new_coordinator => Pid, coordinator_node => node(Pid)}
        end,
    ?SLOG(warning, LogData1),
    Data1 = stop_conn_pool(Data),
    {next_state, wait_for_coordinator, Data1#{is_coordinator => false}};
%% only errors results are expected
%% TODO: a single error causes reconnection and re-bootstrapping,
%% it's worth considering some optimizations.
coordinating(info, {pub_result, _Ref, {error, Reason}}, #{link_conf := #{upstream := Name}} = Data) ->
    ?SLOG(error, #{
        msg => "failed_to_replicate_route_op_to_linked_cluster",
        link_name => Name,
        reason => Reason
    }),
    %% TODO: check errors, some may be not possible to correct by re-connecting
    Data1 = stop_conn_pool(Data),
    {next_state, connecting, Data1};
coordinating(EventType, Event, Data) ->
    handle_event_(?FUNCTION_NAME, EventType, Event, Data).

following(enter, _OldState, _Data) ->
    keep_state_and_data;
following(info, {'DOWN', MRef, process, _Pid, _Info}, #{coordinator_mon := MRef} = Data) ->
    {next_state, wait_for_coordinator, maps:without([coordinator_mon, coordinator_pid], Data)};
following(EventType, Event, Data) ->
    handle_event_(?FUNCTION_NAME, EventType, Event, Data).

handle_event_(_State, info, {'DOWN', Ref, process, _Pid, Reason}, Data) ->
    case Data of
        #{conn_pool_mons := #{Ref := WorkerName}, is_coordinator := true} ->
            ?SLOG(warning, #{
                msg => "cluster_link_route_connection_is_down",
                reason => Reason,
                worker => WorkerName
            }),
            {next_state, connecting, stop_conn_pool(Data)};
        _ ->
            %% Must be a stale 'DOWN' msg (e.g., from the next worker) which is already handled.
            keep_state_and_data
    end;
handle_event_(State, EventType, Event, Data) ->
    ?SLOG(warning, #{
        msg => "unexpected_event",
        event => Event,
        event_type => EventType,
        state => State,
        data => Data
    }),
    keep_state_and_data.

terminate(Reason, _State, #{link_conf := #{upstream := ClusterName}} = Data) ->
    %% TODO unregister coordinator?
    IsCoordinator = maps:get(is_coordinator, Data, false),
    case Reason of
        shutdown when IsCoordinator ->
            %% must be sync, since we are going to stop the pool
            %% NOTE: there is no guarantee that unlink op will arrive the last one
            %% (since there may be other route op sent over another pool worker)
            %% and clear everything, but it must be good enough to GC most of the routes.
            _ = emqx_cluster_link_mqtt:remove_link(ClusterName);
        _ ->
            ok
    end,
    _ = stop_conn_pool(Data),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

topic_intersect_any(Topic, [LinkFilter | T]) ->
    case emqx_topic:intersection(Topic, LinkFilter) of
        false -> topic_intersect_any(Topic, T);
        TopicOrFilter -> TopicOrFilter
    end;
topic_intersect_any(_Topic, []) ->
    false.

bootstrap(ClusterName, LinkFilters) ->
    %% TODO: do this in chunks
    Topics = select_routes(LinkFilters),
    {ok, _} = emqx_cluster_link_mqtt:publish_routes(sync, ClusterName, Topics).

%% TODO: if a local route matches link filter exactly,
%% it's enough to only select this matching filter itself and skip any other routes?
%% E.g., local routes: "t/global/#", "t/global/1/+", clsuter link topics = ["t/global/#"],
%% it's enough to replicate "t/global/#" only to the linked cluster.
%% What to do when "t/global/#" subscriber unsubscribers
%% and we start to get forwarded messages (e.g. "t/global/2/3") matching no subscribers?
%% How can we efficiently replace "t/global/#" route with "t/global/1/+"
%% (intersection of "t/global/#" and "t/global/#")?
%% So maybe better not to do it at all and replicate both "t/global/1/+" and "t/global/#" ?
select_routes(LinkFilters) ->
    {Wildcards, Topics} = lists:partition(fun emqx_topic:wildcard/1, LinkFilters),
    Routes = select_routes_by_topics(Topics),
    Routes1 = intersecting_routes(Wildcards),
    AllRoutes = Routes ++ Routes1,
    case emqx_router:get_schema_vsn() of
        v1 -> AllRoutes;
        %% v2 stores filters (Wildcard subscriptions routes) in a separate index,
        %% so WildcardRoutes contains only non-wildcard routes matching wildcard link filters.
        %% Thus, we need to select wildcard routes additionally
        v2 -> intersecting_routes_v2(Wildcards) ++ AllRoutes
    end.

select_routes_by_topics([]) ->
    [];
select_routes_by_topics([Topic | T]) ->
    case filter_out_upstream_routes(emqx_router:match_routes(Topic)) of
        [_ | _] ->
            %% These are non-wildcard link topics, so we don't care about actual
            %% routes as long as they are matched, and just need to replicate
            %% topic routes to the linked cluster
            [Topic | select_routes_by_topics(T)];
        _ ->
            select_routes_by_topics(T)
    end.

filter_out_upstream_routes(Routes) ->
    lists:filter(
        fun
            (#route{dest = ?UPSTREAM_DEST}) -> false;
            (_) -> true
        end,
        Routes
    ).

%% selects only non-wildcard routes that match wildcards (filters),
%% can only be done as a linear search over all routes
intersecting_routes([]) ->
    [];
intersecting_routes(Wildcards) ->
    Res = ets:foldl(
        fun
            (#route{dest = ?UPSTREAM_DEST}, Acc) ->
                Acc;
            (#route{topic = T}, Acc) ->
                %% TODO: probably nice to validate cluster link topic filters
                %% to have no intersections between each other?
                case topic_intersect_any(T, Wildcards) of
                    false -> Acc;
                    Intersection -> Acc#{Intersection => undefined}
                end
        end,
        #{},
        ?ROUTE_TAB
    ),
    maps:keys(Res).

intersecting_routes_v2([]) ->
    [];
intersecting_routes_v2(Wildcards) ->
    lists:foldl(
        fun(Wildcard, Acc) ->
            MatchedFilters = matched_filters_v2(Wildcard),
            all_intersections(Wildcard, MatchedFilters, Acc)
        end,
        [],
        Wildcards
    ).

matched_filters_v2(Wildcard) ->
    MatchesAcc = lists:foldl(
        fun(M, Acc) ->
            case emqx_topic_index:get_id(M) of
                ?UPSTREAM_DEST ->
                    Acc;
                _ ->
                    Acc#{emqx_topic_index:get_topic(M) => undefined}
            end
        end,
        #{},
        emqx_topic_index:matches_filter(Wildcard, ?ROUTE_TAB_FILTERS, [])
    ),
    maps:keys(MatchesAcc).

all_intersections(Wildcard, [W | Wildcards], Acc) ->
    case emqx_topic:intersection(Wildcard, W) of
        false -> all_intersections(Wildcard, Wildcards, Acc);
        Intersection -> all_intersections(Wildcard, Wildcards, [Intersection | Acc])
    end;
all_intersections(_, [], Acc) ->
    lists:usort(Acc).

maybe_cast(UpstreamName, Msg) ->
    case global:whereis_name(?COORDINATOR(UpstreamName)) of
        Pid when is_pid(Pid) ->
            gen_statem:cast(Pid, Msg);
        undefined ->
            %% Ignore and rely on coordinator bootstrapping once it's elected
            ok
    end.

register_coordinator(UpstreamName) ->
    case mria_config:role() of
        core ->
            global:register_name(
                ?COORDINATOR(UpstreamName), self(), fun global:random_notify_name/3
            );
        _ ->
            no
    end.

%% connecting state helper
ensure_conn_pool(NextState, #{link_conf := LinkConf} = Data) ->
    Res = start_conn_pool(LinkConf),
    Data1 = Data#{conn_pool => Res},
    case Res of
        {ok, _} ->
            Data2 = Data1#{conn_pool_mons => mon_pool_workers(LinkConf)},
            {next_state, NextState, Data2};
        _Err ->
            {keep_state, Data1, [{state_timeout, ?CONN_RETRY_INTERVAL, reconnect}]}
    end.

start_conn_pool(LinkConf) ->
    case emqx_cluster_link_mqtt:start_routing_pool(LinkConf) of
        {ok, _Pid} = Ok ->
            Ok;
        {error, Reason} = Err ->
            #{upstream := Name} = LinkConf,
            ?SLOG(error, #{
                msg => "failed_to_connect_to_linked_cluster",
                cluster_name => Name,
                reason => Reason
            }),
            Err
    end.

stop_conn_pool(#{link_conf := #{upstream := Name}} = Data) ->
    case Data of
        #{conn_pool := {ok, _}} ->
            Data1 = maybe_unmointor_workers(Data),
            Data1#{conn_pool => {stopped, emqx_cluster_link_mqtt:stop_routing_pool(Name)}};
        _ ->
            Data
    end.

maybe_unmointor_workers(#{conn_pool_mons := MonitorsMap} = Data) ->
    _ = maps:foreach(
        fun(Mref, _Name) ->
            erlang:demonitor(Mref)
        end,
        MonitorsMap
    ),
    maps:remove(conn_pool_mons, Data);
maybe_unmointor_workers(Data) ->
    Data.

mon_pool_workers(LinkConf) ->
    maps:from_list([
        {erlang:monitor(process, Pid), Name}
     || {Name, Pid} <- emqx_cluster_link_mqtt:routing_pool_workers(LinkConf)
    ]).
