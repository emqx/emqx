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
-export([get_policy/1]).
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

-type dimension() :: clientid | username | peerhost.

%% Client ID keys are bare binaries; the other dimensions are tagged.
%% The types cannot collide, and bare binaries keep the table shape
%% unchanged for the client ID dimension.
-type key() ::
    emqx_types:clientid()
    | {username, emqx_types:username()}
    | {peerhost, emqx_types:peerhost()}.

-record(flapping, {
    key :: key(),
    peerhost :: emqx_types:peerhost(),
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
        detect(ClientId, PeerHost, clientid_policy(Policy)),
        detect({username, Username}, PeerHost, dimension_policy(by_username, Policy)),
        detect({peerhost, PeerHost}, PeerHost, dimension_policy(by_peerhost, Policy))
    ],
    lists:member(true, Detected).

detect({username, undefined}, _PeerHost, _Policy) ->
    false;
detect(_Key, _PeerHost, none) ->
    false;
detect(Key, PeerHost, #{max_count := Threshold} = Policy) ->
    %% The initial flapping record sets the detect_cnt to 0.
    InitVal = #flapping{
        key = Key,
        peerhost = PeerHost,
        started_at = erlang:system_time(millisecond),
        detect_cnt = 0
    },
    case ets:update_counter(?FLAPPING_TAB, Key, {#flapping.detect_cnt, 1}, InitVal) of
        Cnt when Cnt < Threshold -> false;
        _Cnt ->
            case ets:take(?FLAPPING_TAB, Key) of
                [Flapping] ->
                    ok = gen_server:cast(?MODULE, {detected, Flapping, Policy}),
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
            Policy#{enable => false, by_username => none, by_peerhost => none};
        Policy ->
            Policy
    end.

%% The top-level policy is the client ID dimension.
clientid_policy(#{enable := true} = Policy) ->
    Policy;
clientid_policy(_Policy) ->
    none.

dimension_policy(Name, Policy) ->
    maps:get(Name, Policy, none).

all_dimensions(Policy) ->
    [
        clientid_policy(Policy),
        dimension_policy(by_username, Policy),
        dimension_policy(by_peerhost, Policy)
    ].

enabled_window_times(Policy) ->
    lists:filtermap(
        fun
            (#{window_time := WindowTime}) -> {true, WindowTime};
            (none) -> false
        end,
        all_dimensions(Policy)
    ).

%% Disabled dimensions produce no table entries, so only the enabled
%% ones are considered when deciding which entries are stale.
max_window_time(Policy) ->
    case enabled_window_times(Policy) of
        [] -> maps:get(window_time, Policy);
        WindowTimes -> lists:max(WindowTimes)
    end.

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
            key = Key,
            peerhost = PeerHost,
            started_at = StartedAt,
            detect_cnt = DetectCnt
        },
        #{window_time := WindowTime, ban_time := Interval}},
    State
) ->
    {Dimension, Value} = dimension_value(Key),
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

handle_info({timeout, _TRef, {garbage_collect, Zone}}, State) ->
    Policy = get_policy(Zone),
    Timestamp = erlang:system_time(millisecond) - max_window_time(Policy),
    MatchSpec = ets:fun2ms(fun(#flapping{started_at = StartedAt}) when StartedAt < Timestamp ->
        true
    end),
    ets:select_delete(?FLAPPING_TAB, MatchSpec),
    Timer = start_timer(Policy, Zone),
    {noreply, State#{Zone => Timer}, hibernate};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec dimension_value(key()) -> {dimension(), term()}.
dimension_value({username, Username}) -> {username, Username};
dimension_value({peerhost, PeerHost}) -> {peerhost, PeerHost};
dimension_value(ClientId) -> {clientid, ClientId}.

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

start_timer(Policy, Zone) ->
    case enabled_window_times(Policy) of
        [] ->
            undefined;
        WindowTimes ->
            emqx_utils:start_timer(lists:max(WindowTimes), {garbage_collect, Zone})
    end.

start_timers() ->
    maps:map(
        fun(ZoneName, #{flapping_detect := FlappingDetect}) ->
            start_timer(FlappingDetect, ZoneName)
        end,
        emqx:get_config([zones], #{})
    ).

update_timer(Timers) ->
    maps:map(
        fun(ZoneName, #{flapping_detect := FlappingDetect}) ->
            Enable = enabled_window_times(FlappingDetect) =/= [],
            case maps:get(ZoneName, Timers, undefined) of
                undefined ->
                    start_timer(FlappingDetect, ZoneName);
                TRef when Enable -> TRef;
                TRef ->
                    _ = erlang:cancel_timer(TRef),
                    undefined
            end
        end,
        emqx:get_config([zones], #{})
    ).

fmt_host(PeerHost) ->
    try
        inet:ntoa(PeerHost)
    catch
        _:_ -> PeerHost
    end.
