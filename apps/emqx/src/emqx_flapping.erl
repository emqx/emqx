%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_flapping).

-behaviour(gen_server).

-include("emqx.hrl").
-include("types.hrl").
-include("logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/0, update_config/0, stop/0]).

%% API
-export([detect/1]).

-ifdef(TEST).
-export([get_policy/1, dimension_defaults/0, gc_interval/1]).
-endif.

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Tab
-define(FLAPPING_TAB, ?MODULE).

-type key() ::
    {emqx_types:zone(), clientid, emqx_types:clientid()}
    | {emqx_types:zone(), username, emqx_types:username()}
    | {emqx_types:zone(), peerhost, emqx_types:peerhost()}.

%% Must match the field defaults of the `flapping_detect_dimension`
%% struct in `emqx_schema`: zone overrides may hold partial dimension
%% configs whose unset fields are only filled by the global config when
%% the global dimension is a struct, not `none`.
-define(DIMENSION_DEFAULTS, #{
    window_time => 60000,
    max_count => 15,
    ban_time => 300000
}).

-record(flapping, {
    key :: key(),
    started_at :: pos_integer(),
    detect_cnt :: integer()
}).

-opaque flapping() :: #flapping{}.

-export_type([flapping/0]).

-spec start_link() -> emqx_types:startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

update_config() ->
    gen_server:cast(?MODULE, update_config).

stop() -> gen_server:stop(?MODULE).

-doc """
Count a connect event towards flapping detection.

Each enabled dimension (client ID, username, source IP address) is counted
independently against its own policy; the offending value is banned via
`emqx_banned` once its threshold is exceeded within its detection window.
Returns `true` if any dimension detected flapping.
""".
-spec detect(emqx_types:clientinfo()) -> boolean().
detect(#{clientid := ClientId, peerhost := PeerHost, zone := Zone} = ClientInfo) ->
    Policy = get_policy(Zone),
    Username = maps:get(username, ClientInfo, undefined),
    Detected = [
        detect({Zone, clientid, ClientId}, PeerHost, dimension_policy(by_clientid, Policy)),
        detect({Zone, username, Username}, PeerHost, dimension_policy(by_username, Policy)),
        detect({Zone, peerhost, PeerHost}, PeerHost, dimension_policy(by_peerhost, Policy))
    ],
    lists:member(true, Detected).

detect({_Zone, username, undefined}, _PeerHost, _Policy) ->
    false;
detect(_Key, _PeerHost, none) ->
    false;
detect(Key, PeerHost, #{max_count := Threshold} = Policy) ->
    %% The initial flapping record sets the detect_cnt to 0.
    InitVal = #flapping{
        key = Key,
        started_at = erlang:system_time(millisecond),
        detect_cnt = 0
    },
    case ets:update_counter(?FLAPPING_TAB, Key, {#flapping.detect_cnt, 1}, InitVal) of
        Cnt when Cnt < Threshold -> false;
        _Cnt ->
            case ets:take(?FLAPPING_TAB, Key) of
                [Flapping] ->
                    %% PeerHost is the source IP of the connect event
                    %% that tripped the threshold.
                    ok = gen_server:cast(?MODULE, {detected, Flapping, PeerHost, Policy}),
                    true;
                [] ->
                    false
            end
    end.

get_policy(Zone) ->
    Flapping = [flapping_detect],
    case emqx_config:get_zone_conf(Zone, Flapping, undefined) of
        undefined ->
            %% If zone has be deleted at running time,
            %% we don't crash the connection and disable flapping detect.
            Policy = emqx_config:get(Flapping),
            Policy#{by_clientid => none, by_username => none, by_peerhost => none};
        Policy ->
            Policy
    end.

dimension_policy(Name, Policy) ->
    case maps:get(Name, Policy, none) of
        none -> none;
        Dimension -> ensure_defaults(Dimension)
    end.

ensure_defaults(#{window_time := _, max_count := _, ban_time := _} = Dimension) ->
    Dimension;
ensure_defaults(Dimension) ->
    maps:merge(?DIMENSION_DEFAULTS, Dimension).

-ifdef(TEST).
dimension_defaults() -> ?DIMENSION_DEFAULTS.
-endif.

all_dimensions(Policy) ->
    [
        {clientid, dimension_policy(by_clientid, Policy)},
        {username, dimension_policy(by_username, Policy)},
        {peerhost, dimension_policy(by_peerhost, Policy)}
    ].

enabled_window_times(Policy) ->
    lists:filtermap(
        fun
            ({_Dimension, #{window_time := WindowTime}}) -> {true, WindowTime};
            ({_Dimension, none}) -> false
        end,
        all_dimensions(Policy)
    ).

now_diff(TS) -> erlang:system_time(millisecond) - TS.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_utils_ets:new(?FLAPPING_TAB, [
        public,
        set,
        {keypos, #flapping.key},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    Timers = start_timers(),
    {ok, Timers, hibernate}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(
    {detected,
        #flapping{
            key = {_Zone, Dimension, Value},
            started_at = StartedAt,
            detect_cnt = DetectCnt
        },
        PeerHost, #{window_time := WindowTime, ban_time := Interval}},
    State
) ->
    case now_diff(StartedAt) < WindowTime of
        %% Flapping happened:(
        true ->
            Now = erlang:system_time(second),
            Until = Now + (Interval div 1000),
            ok = emqx_banned:ensure(#banned{
                who = emqx_banned:who(Dimension, Value),
                by = <<"flapping detector">>,
                reason = <<"flapping is detected">>,
                at = Now,
                until = Until
            }),
            ok = emqx_metrics:inc_global(detected_metric(Dimension)),
            {Data, Meta} = log_info(Dimension, Value),
            ?SLOG(
                warning,
                Data#{
                    msg => "flapping_detected",
                    detected_by => Dimension,
                    peer_host => fmt_host(PeerHost),
                    detect_cnt => DetectCnt,
                    window_time_ms => WindowTime,
                    banned_until => emqx_utils_calendar:epoch_to_rfc3339(Until, second)
                },
                Meta
            );
        false ->
            ok
    end,
    {noreply, State};
handle_cast(update_config, State) ->
    NState = update_timer(State),
    {noreply, NState};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, TRef, {garbage_collect, Zone}}, State) ->
    case maps:get(Zone, State, undefined) of
        TRef ->
            Policy = get_policy(Zone),
            garbage_collect_zone(Zone, Policy),
            Timer = start_timer(Policy, Zone),
            {noreply, State#{Zone => Timer}, hibernate};
        _ ->
            {noreply, State}
    end;
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

detected_metric(clientid) -> 'flapping.detected.clientid';
detected_metric(username) -> 'flapping.detected.username';
detected_metric(peerhost) -> 'flapping.detected.peerhost'.

log_info(clientid, ClientId) ->
    {#{}, #{clientid => ClientId}};
log_info(username, Username) ->
    {#{username => Username}, #{}};
log_info(peerhost, _PeerHost) ->
    %% peer_host is always part of the log data.
    {#{}, #{}}.

garbage_collect_zone(Zone, Policy) ->
    Now = erlang:system_time(millisecond),
    MatchSpec = lists:append([
        expired_dimension_match_spec(Zone, Dimension, DimensionPolicy, Now)
     || {Dimension, DimensionPolicy} <- all_dimensions(Policy),
        is_map(DimensionPolicy)
    ]),
    select_delete(MatchSpec).

garbage_collect_after_config_update(Zones) ->
    Now = erlang:system_time(millisecond),
    maps:foreach(
        fun(Zone, #{flapping_detect := Policy}) ->
            MatchSpec = lists:append([
                config_update_dimension_match_spec(Zone, Dimension, DimensionPolicy, Now)
             || {Dimension, DimensionPolicy} <- all_dimensions(Policy)
            ]),
            select_delete(MatchSpec)
        end,
        Zones
    ).

config_update_dimension_match_spec(Zone, Dimension, none, _Now) ->
    ets:fun2ms(
        fun(#flapping{key = {EntryZone, EntryDimension, _}}) when
            EntryZone =:= Zone, EntryDimension =:= Dimension
        ->
            true
        end
    );
config_update_dimension_match_spec(Zone, Dimension, DimensionPolicy, Now) ->
    expired_dimension_match_spec(Zone, Dimension, DimensionPolicy, Now).

expired_dimension_match_spec(Zone, Dimension, #{window_time := WindowTime}, Now) ->
    Cutoff = Now - WindowTime,
    ets:fun2ms(
        fun(#flapping{key = {EntryZone, EntryDimension, _}, started_at = StartedAt}) when
            EntryZone =:= Zone,
            EntryDimension =:= Dimension,
            StartedAt =< Cutoff
        ->
            true
        end
    ).

select_delete([]) ->
    0;
select_delete(MatchSpec) ->
    ets:select_delete(?FLAPPING_TAB, MatchSpec).

delete_zone(Zone) ->
    MatchSpec = ets:fun2ms(
        fun(#flapping{key = {EntryZone, _, _}}) when EntryZone =:= Zone ->
            true
        end
    ),
    ets:select_delete(?FLAPPING_TAB, MatchSpec).

start_timer(Policy, Zone) ->
    case gc_interval(Policy) of
        undefined ->
            undefined;
        Interval ->
            emqx_utils:start_timer(Interval, {garbage_collect, Zone})
    end.

gc_interval(Policy) ->
    case enabled_window_times(Policy) of
        [] -> undefined;
        WindowTimes -> lists:min(WindowTimes)
    end.

start_timers() ->
    start_timers(emqx:get_config([zones], #{})).

start_timers(Zones) ->
    maps:map(
        fun(ZoneName, #{flapping_detect := FlappingDetect}) ->
            start_timer(FlappingDetect, ZoneName)
        end,
        Zones
    ).

update_timer(Timers) ->
    Zones = emqx:get_config([zones], #{}),
    OldZones = maps:keys(Timers),
    NewZones = maps:keys(Zones),
    lists:foreach(fun delete_zone/1, OldZones -- NewZones),
    garbage_collect_after_config_update(Zones),
    maps:foreach(fun(_ZoneName, TRef) -> emqx_utils:cancel_timer(TRef) end, Timers),
    start_timers(Zones).

fmt_host(PeerHost) ->
    try
        inet:ntoa(PeerHost)
    catch
        _:_ -> PeerHost
    end.
