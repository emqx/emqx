%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_resources).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-define(CHECK_INTERVAL, 5000).

-export([
    start_link/0,
    start_link/1,
    %% hot call
    cached_connection_count/0
]).

%% RPC
-export([
    local_connection_count/0,
    stats/1
]).

%% For testing
-export([update_now/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(OK(EXPR),
    (fun() ->
        try
            _ = EXPR,
            ok
        catch
            _:_ -> ok
        end
    end)()
).

-define(SAFE_CACHE_LOOKUP(Key, Default),
    try
        case ets:lookup(?MODULE, Key) of
            [{Key, Value}] -> Value;
            _ -> Default
        end
    catch
        _:_ -> Default
    end
).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(?CHECK_INTERVAL).

-spec start_link(timeout()) -> {ok, pid()}.
start_link(CheckInterval) when is_integer(CheckInterval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [CheckInterval], []).

%% @doc This function returns the total number of sessions, not connections,
%% including the ones which are disconnected.
%% Function name is not changed for RPC compatibility.
-spec local_connection_count() -> non_neg_integer().
local_connection_count() ->
    emqx_cm:get_sessions_count().

%% @doc This function returns the totoal number of sessions (not connections)
%% and the latest TPS of the local node in a map.
-spec stats(integer()) -> #{sessions := non_neg_integer(), tps := number()}.
stats(Time) ->
    #{sessions => emqx_cm:get_sessions_count(), tps => emqx_dashboard_monitor:local_tps(Time)}.

%% @doc For testing
-spec update_now() -> ok.
update_now() ->
    _ = erlang:send(whereis(?MODULE), update_resources),
    ok.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([CheckInterval]) ->
    _ = ets:new(?MODULE, [set, protected, named_table]),
    State = ensure_timer(#{check_peer_interval => CheckInterval}),
    {ok, State}.

handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(update_resources, State) ->
    ok = update_resources(),
    ok = maybe_alarms(),
    ?tp(emqx_license_resources_updated, #{}),
    {noreply, ensure_timer(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------
maybe_alarms() ->
    Limits = emqx_license_checker:limits(),
    ok = connection_quota_early_alarm(Limits),
    ok = max_tps_alarm(Limits),
    ok.

connection_quota_early_alarm({ok, #{max_sessions := Max}}) when is_integer(Max) ->
    Count = cached_connection_count(),
    Low = emqx_conf:get([license, connection_low_watermark], 0.75),
    High = emqx_conf:get([license, connection_high_watermark], 0.80),
    Count > Max * High andalso
        begin
            HighPercent = float_to_binary(High * 100, [{decimals, 0}]),
            Message = iolist_to_binary([
                "License: sessions quota exceeds ", HighPercent, "%"
            ]),
            ?OK(emqx_alarm:activate(license_quota, #{high_watermark => HighPercent}, Message))
        end,
    Count < Max * Low andalso ?OK(emqx_alarm:ensure_deactivated(license_quota)),
    ok;
connection_quota_early_alarm(_Limits) ->
    ok.

%% @private The cache table keeps track of the max TPS of the cluster (computed locally) over time.
%% However, the cache is ephemeral (ets), so we need to read from the alarm state too to compare
%% with the existing alarm (if any). The alarm is activated when the latest observed cluster TPS exceeds the limit.
%% The alarm is deactivated after a new license is loaded with higher TPS limit.
max_tps_alarm({ok, #{max_tps := Limit}}) ->
    LatestTps = cached_latest_cluster_tps(),
    HistMaxTps = cached_max_tps(),
    {Action, AlarmDetails} =
        case emqx_alarm:read_details(license_tps) of
            {ok, #{max_tps := AlarmTps} = Details} when LatestTps > AlarmTps ->
                {update, Details#{
                    max_tps => LatestTps, observed_at => now_rfc3339(), hist_max_tps => HistMaxTps
                }};
            {ok, Details} ->
                {ignore, Details};
            {error, not_found} ->
                {activate, new_tps_alarm_details(LatestTps, HistMaxTps)}
        end,
    MaxTps = maps:get(max_tps, AlarmDetails),
    case is_integer(Limit) andalso MaxTps > Limit of
        true when Action =:= update ->
            _ = emqx_alarm:update_details(license_tps, AlarmDetails),
            ok;
        true when Action =:= activate ->
            Message = iolist_to_binary(io_lib:format("License: TPS limit (~w) exceeded.", [Limit])),
            ?OK(emqx_alarm:activate(license_tps, AlarmDetails, Message));
        true when Action =:= ignore ->
            ok;
        false ->
            %% License has higher TPS limit, ensure the alarm is deactivated.
            ?OK(emqx_alarm:ensure_deactivated(license_tps))
    end.

new_tps_alarm_details(MaxTps, HistMaxTps) ->
    emqx_alarm:make_persistent_details(#{
        max_tps => MaxTps,
        hist_max_tps => HistMaxTps,
        observed_at => now_rfc3339()
    }).

now_rfc3339() ->
    emqx_utils_calendar:epoch_to_rfc3339(erlang:system_time(millisecond), millisecond).

cached_connection_count() ->
    ?SAFE_CACHE_LOOKUP(total_connection_count, 0).

cached_latest_cluster_tps() ->
    ?SAFE_CACHE_LOOKUP(latest_cluster_tps, 0).

cached_max_tps() ->
    ?SAFE_CACHE_LOOKUP(max_cluster_tps, 0).

update_resources() ->
    #{sessions := Sessions, tps := TPS} = stats(),
    ets:insert(?MODULE, {total_connection_count, Sessions}),
    Max0 = cached_max_tps(),
    Max = max(Max0, TPS),
    ets:insert(?MODULE, {latest_cluster_tps, TPS}),
    ets:insert(?MODULE, {max_cluster_tps, Max}),
    ok.

ensure_timer(#{check_peer_interval := CheckInterval} = State) ->
    _ =
        case State of
            #{timer := Timer} -> erlang:cancel_timer(Timer);
            _ -> ok
        end,
    State#{timer => erlang:send_after(CheckInterval, self(), update_resources)}.

total_sessions_v2(Nodes) ->
    Results = emqx_license_proto_v2:remote_connection_counts(Nodes),
    Counts = [Count || {ok, Count} <- Results],
    lists:sum(Counts).

stats() ->
    Nodes = mria:running_nodes(),
    %% Upgrade from v2 to v3 if any node in the cluster is before v3.
    Stats =
        case emqx_bpapi:supported_version(emqx_license) of
            2 ->
                SessionsV2 = total_sessions_v2(Nodes),
                #{sessions => SessionsV2, tps => 0};
            V when V > 2 ->
                Now = erlang:system_time(millisecond),
                stats(Nodes, Now)
        end,
    #{sessions := Sessions, tps := TPS} = Stats,
    %% Gateway registry is global, so take it from local node.
    GatewayConnections = emqx_gateway_cm_registry:get_connected_client_count(),
    #{sessions => Sessions + GatewayConnections, tps => erlang:round(TPS)}.

-spec stats(list(node()), integer()) -> #{sessions := non_neg_integer(), tps := number()}.
stats(Nodes, Now) ->
    Results = emqx_license_proto_v3:stats(Nodes, Now),
    lists:foldl(
        fun
            ({ok, #{sessions := Sessions, tps := TPS}}, Acc) ->
                Acc#{
                    sessions => Sessions + maps:get(sessions, Acc, 0),
                    tps => TPS + maps:get(tps, Acc, 0)
                };
            (_, Acc) ->
                Acc
        end,
        dummy_stats(),
        Results
    ).

dummy_stats() ->
    #{sessions => 0, tps => 0}.
