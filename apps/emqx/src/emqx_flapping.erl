%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_flapping).

-behaviour(gen_server).

-include("emqx.hrl").
-include("types.hrl").
-include("logger.hrl").

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

-record(flapping, {
    clientid :: emqx_types:clientid(),
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

%% @doc Detect flapping when a MQTT client disconnected.
-spec detect(emqx_types:clientinfo()) -> boolean().
detect(#{clientid := ClientId, peerhost := PeerHost, zone := Zone}) ->
    detect(ClientId, PeerHost, get_policy(Zone)).

detect(ClientId, PeerHost, #{enable := true, max_count := Threshold} = Policy) ->
    %% The initial flapping record sets the detect_cnt to 0.
    InitVal = #flapping{
        clientid = ClientId,
        peerhost = PeerHost,
        started_at = erlang:system_time(millisecond),
        detect_cnt = 0
    },
    case ets:update_counter(?FLAPPING_TAB, ClientId, {#flapping.detect_cnt, 1}, InitVal) of
        Cnt when Cnt < Threshold -> false;
        _Cnt ->
            case ets:take(?FLAPPING_TAB, ClientId) of
                [Flapping] ->
                    ok = gen_server:cast(?MODULE, {detected, Flapping, Policy}),
                    true;
                [] ->
                    false
            end
    end;
detect(_ClientId, _PeerHost, #{enable := false}) ->
    false.

get_policy(Zone) ->
    Flapping = [flapping_detect],
    case emqx_config:get_zone_conf(Zone, Flapping, undefined) of
        undefined ->
            %% If zone has be deleted at running time,
            %% we don't crash the connection and disable flapping detect.
            Policy = emqx_config:get(Flapping),
            Policy#{enable => false};
        Policy ->
            Policy
    end.

now_diff(TS) -> erlang:system_time(millisecond) - TS.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_utils_ets:new(?FLAPPING_TAB, [
        public,
        set,
        {keypos, #flapping.clientid},
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
            clientid = ClientId,
            peerhost = PeerHost,
            started_at = StartedAt,
            detect_cnt = DetectCnt
        },
        #{window_time := WindTime, ban_time := Interval}},
    State
) ->
    case now_diff(StartedAt) < WindTime of
        %% Flapping happened:(
        true ->
            ?SLOG(
                warning,
                #{
                    msg => "flapping_detected",
                    peer_host => fmt_host(PeerHost),
                    detect_cnt => DetectCnt,
                    wind_time_in_ms => WindTime
                },
                #{clientid => ClientId}
            ),
            Now = erlang:system_time(second),
            Banned = #banned{
                who = emqx_banned:who(clientid, ClientId),
                by = <<"flapping detector">>,
                reason = <<"flapping is detected">>,
                at = Now,
                until = Now + (Interval div 1000)
            },
            {ok, _} = emqx_banned:create(Banned),
            ok;
        false ->
            ?SLOG(
                warning,
                #{
                    msg => "client_disconnected",
                    peer_host => fmt_host(PeerHost),
                    detect_cnt => DetectCnt,
                    interval => Interval
                },
                #{clientid => ClientId}
            )
    end,
    {noreply, State};
handle_cast(update_config, State) ->
    NState = update_timer(State),
    {noreply, NState};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, _TRef, {garbage_collect, Zone}}, State) ->
    Policy = #{window_time := WindowTime} = get_policy(Zone),
    Timestamp = erlang:system_time(millisecond) - WindowTime,
    MatchSpec = [{{'_', '_', '_', '$1', '_'}, [{'<', '$1', Timestamp}], [true]}],
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

start_timer(#{enable := true, window_time := WindowTime}, Zone) ->
    emqx_utils:start_timer(WindowTime, {garbage_collect, Zone});
start_timer(_Policy, _Zone) ->
    undefined.

start_timers() ->
    maps:map(
        fun(ZoneName, #{flapping_detect := FlappingDetect}) ->
            start_timer(FlappingDetect, ZoneName)
        end,
        emqx:get_config([zones], #{})
    ).

update_timer(Timers) ->
    maps:map(
        fun(ZoneName, #{flapping_detect := FlappingDetect = #{enable := Enable}}) ->
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
