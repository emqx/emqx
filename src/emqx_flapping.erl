%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-logger_header("[Flapping]").

-export([start_link/0, stop/0]).

%% API
-export([check/1, detect/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% Tab
-define(FLAPPING_TAB, ?MODULE).
%% Default Policy
-define(FLAPPING_THRESHOLD, 30).
-define(FLAPPING_DURATION, 60000).
-define(FLAPPING_BANNED_INTERVAL, 300000).
-define(DEFAULT_DETECT_POLICY,
        #{threshold => ?FLAPPING_THRESHOLD,
          duration => ?FLAPPING_DURATION,
          banned_interval => ?FLAPPING_BANNED_INTERVAL
         }).

-record(flapping, {
          clientid   :: emqx_types:clientid(),
          peerhost   :: emqx_types:peerhost(),
          started_at :: pos_integer(),
          detect_cnt :: pos_integer(),
          banned_at  :: pos_integer()
         }).

-opaque(flapping() :: #flapping{}).

-export_type([flapping/0]).

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() -> gen_server:stop(?MODULE).

%% @doc Check flapping when a MQTT client connected.
-spec(check(emqx_types:clientinfo()) -> boolean()).
check(#{clientid := ClientId}) ->
    check(ClientId, get_policy()).

check(ClientId, #{banned_interval := Interval}) ->
    case ets:lookup(?FLAPPING_TAB, {banned, ClientId}) of
        [] -> false;
        [#flapping{banned_at = BannedAt}] ->
            now_diff(BannedAt) < Interval
    end.

%% @doc Detect flapping when a MQTT client disconnected.
-spec(detect(emqx_types:clientinfo()) -> boolean()).
detect(Client) -> detect(Client, get_policy()).

detect(#{clientid := ClientId, peerhost := PeerHost},
       Policy = #{threshold := Threshold}) ->
    try ets:update_counter(?FLAPPING_TAB, ClientId, {#flapping.detect_cnt, 1}) of
        Cnt when Cnt < Threshold -> false;
        _Cnt -> case ets:lookup(?FLAPPING_TAB, ClientId) of
                    [Flapping] ->
                        ok = gen_server:cast(?MODULE, {detected, Flapping, Policy}),
                        true;
                    [] -> false
                end
    catch
        error:badarg ->
            %% Create a flapping record.
            Flapping = #flapping{clientid   = ClientId,
                                 peerhost   = PeerHost,
                                 started_at = emqx_time:now_ms(),
                                 detect_cnt = 1
                                },
            true = ets:insert(?FLAPPING_TAB, Flapping),
            false
    end.

-compile({inline, [get_policy/0, now_diff/1]}).

get_policy() ->
    emqx:get_env(flapping_detect_policy, ?DEFAULT_DETECT_POLICY).

now_diff(TS) -> emqx_time:now_ms() - TS.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    #{duration := Duration, banned_interval := Interval} = get_policy(),
    ok = emqx_tables:new(?FLAPPING_TAB, [public, set,
                                         {keypos, 2},
                                         {read_concurrency, true},
                                         {write_concurrency, true}
                                        ]),
    State = #{time => max(Duration, Interval) + 1, tref => undefined},
    {ok, ensure_timer(State), hibernate}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({detected, Flapping = #flapping{clientid   = ClientId,
                                            peerhost   = PeerHost,
                                            started_at = StartedAt,
                                            detect_cnt = DetectCnt},
             #{duration := Duration}}, State) ->
    case (Interval = now_diff(StartedAt)) < Duration of
        true -> %% Flapping happened:(
            %% Log first
            ?LOG(error, "Flapping detected: ~s(~s) disconnected ~w times in ~wms",
                 [ClientId, esockd_net:ntoa(PeerHost), DetectCnt, Duration]),
            %% Banned.
            BannedFlapping = Flapping#flapping{clientid  = {banned, ClientId},
                                               banned_at = emqx_time:now_ms()
                                              },
            alarm_handler:set_alarm({{flapping_detected, ClientId}, BannedFlapping}),
            ets:insert(?FLAPPING_TAB, BannedFlapping);
        false ->
            ?LOG(warning, "~s(~s) disconnected ~w times in ~wms",
                 [ClientId, esockd_net:ntoa(PeerHost), DetectCnt, Interval])
    end,
    ets:delete_object(?FLAPPING_TAB, Flapping),
    {noreply, State};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, TRef, expire_flapping}, State = #{tref := TRef}) ->
    with_flapping_tab(fun expire_flapping/2,
                      [emqx_time:now_ms(), get_policy()]),
    {noreply, ensure_timer(State#{tref => undefined}), hibernate};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_timer(State = #{time := Time, tref := undefined}) ->
    State#{tref => emqx_misc:start_timer(Time, expire_flapping)};
ensure_timer(State) -> State.

with_flapping_tab(Fun, Args) ->
    case ets:info(?FLAPPING_TAB, size) of
        undefined -> ok;
        0         -> ok;
        _Size     -> erlang:apply(Fun, Args)
    end.

expire_flapping(NowTime, #{duration := Duration, banned_interval := Interval}) ->
    case ets:select(?FLAPPING_TAB,
                    [{#flapping{started_at = '$1', banned_at = undefined, _ = '_'},
                      [{'<', '$1', NowTime-Duration}], ['$_']},
                     {#flapping{clientid = {banned, '_'}, banned_at = '$1', _ = '_'},
                      [{'<', '$1', NowTime-Interval}], ['$_']}]) of
        [] -> ok;
        Flappings ->
            lists:foreach(fun(Flapping = #flapping{clientid = {banned, ClientId}}) ->
                              ets:delete_object(?FLAPPING_TAB, Flapping),
                              alarm_handler:clear_alarm({flapping_detected, ClientId});
                             (_) -> ok
                            end, Flappings)
    end.

