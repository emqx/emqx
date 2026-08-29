%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([update_now/0, backdate_tps_breach/1]).

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

%% @doc For testing: move the start of the current over-limit window back by
%% `Ms', so a test can reach the end of a long window without sleeping through
%% it. Does nothing when no window is open. Goes through the owning process
%% because the table is protected.
-spec backdate_tps_breach(non_neg_integer()) -> ok.
backdate_tps_breach(Ms) ->
    gen_server:call(?MODULE, {backdate_tps_breach, Ms}, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([CheckInterval]) ->
    _ = ets:new(?MODULE, [set, protected, named_table]),
    State = ensure_timer(#{check_peer_interval => CheckInterval}),
    {ok, State}.

handle_call({backdate_tps_breach, Ms}, _From, State) ->
    Reply =
        case cached_tps_over_limit_since() of
            undefined ->
                ok;
            Since ->
                true = ets:insert(?MODULE, {tps_over_limit_since, Since - Ms}),
                ok
        end,
    {reply, Reply, State};
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
    %% The window measures time until activation, so it only runs while there is
    %% no alarm to raise. Leaving it armed under an active alarm would matter as
    %% soon as an alarm can clear on its own: the first sample after it cleared
    %% would find a window opened long ago and re-raise at once, turning a clear
    %% duration into a flapping alarm.
    ok = track_tps_breach(
        Action =:= activate andalso is_integer(Limit) andalso LatestTps > Limit
    ),
    MaxTps = maps:get(max_tps, AlarmDetails),
    case is_integer(Limit) andalso MaxTps > Limit of
        true when Action =:= update ->
            _ = emqx_alarm:update_details(license_tps, AlarmDetails),
            ok;
        true when Action =:= activate ->
            case tps_breach_sustained() of
                true ->
                    Message = iolist_to_binary(
                        io_lib:format("License: TPS limit (~w) exceeded.", [Limit])
                    ),
                    ?OK(emqx_alarm:activate(license_tps, AlarmDetails, Message));
                false ->
                    %% Over the limit, but not for long enough yet.
                    ok
            end;
        true when Action =:= ignore ->
            ok;
        false ->
            %% License has higher TPS limit, ensure the alarm is deactivated.
            ?OK(emqx_alarm:ensure_deactivated(license_tps))
    end.

%% @private Remember when the current run of over-limit samples started, so the
%% alarm can require the breach to last. A sample at or below the limit ends the
%% run: `tps_alarm_sustain_duration' is a continuous window, not a total.
%%
%% Held in the same ephemeral table as the TPS cache, so a restart of this
%% process starts the window over. That only delays an alarm by the configured
%% duration, and the sampling itself restarts with the process anyway.
%%
%% The writes are deliberately not wrapped in `?OK/1': the table is created in
%% `init/1' and written from its owning process, so a failure is a bug rather
%% than an expected condition. Swallowed, it would freeze the window and the
%% alarm would never fire again with nothing in the log to say why.
track_tps_breach(_IsOverLimit = false) ->
    true = ets:delete(?MODULE, tps_over_limit_since),
    ok;
track_tps_breach(_IsOverLimit = true) ->
    case cached_tps_over_limit_since() of
        undefined ->
            true = ets:insert(?MODULE, {tps_over_limit_since, monotonic_ms()}),
            ok;
        _AlreadyTracking ->
            ok
    end.

%% @private Whether the current run of over-limit samples has lasted at least
%% `license.tps_alarm_sustain_duration'. The default of 0 keeps the alarm firing
%% on the first over-limit sample.
tps_breach_sustained() ->
    case emqx_conf:get([license, tps_alarm_sustain_duration], 0) of
        Duration when Duration =< 0 ->
            true;
        Duration ->
            case cached_tps_over_limit_since() of
                undefined ->
                    false;
                Since ->
                    monotonic_ms() - Since > Duration
            end
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

cached_tps_over_limit_since() ->
    ?SAFE_CACHE_LOOKUP(tps_over_limit_since, undefined).

%% The window is a duration, so it is measured with the monotonic clock.
%% `erlang:system_time/1' can step - an NTP correction, a manual clock change, a
%% suspended VM - and a step inside the window would delay the alarm or fire it
%% early, which is what this option exists to prevent. The `system_time' call in
%% `update_resources/0' stays as it is: that value is a timestamp to report, not
%% a difference to compare.
monotonic_ms() ->
    erlang:monotonic_time(millisecond).

update_resources() ->
    Now = erlang:system_time(millisecond),
    #{sessions := Sessions, tps := TPS} = stats(),
    ets:insert(?MODULE, {total_connection_count, Sessions}),
    Max0 = cached_max_tps(),
    Max = max(Max0, TPS),
    ets:insert(?MODULE, {latest_cluster_tps, TPS}),
    ets:insert(?MODULE, {max_cluster_tps, Max}),
    ok = emqx_license_session_hwm:observe(Now, Sessions),
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
    ExtraSessions = emqx_license_session_count:sum_callbacks(),
    #{sessions => Sessions + ExtraSessions, tps => erlang:round(TPS)}.

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
