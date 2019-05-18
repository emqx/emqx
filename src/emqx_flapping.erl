%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_flapping).

-include("emqx.hrl").
-include("types.hrl").

-behaviour(gen_statem).

-export([start_link/1]).

%% This module is used to garbage clean the flapping records

%% gen_statem callbacks
-export([ terminate/3
        , code_change/4
        , init/1
        , initialized/3
        , callback_mode/0
        ]).

-define(FLAPPING_TAB, ?MODULE).

-export([check/3]).

-record(flapping,
        { client_id   :: binary()
        , check_count :: integer()
        , timestamp   :: integer()
        }).

-type(flapping_record() :: #flapping{}).
-type(flapping_state() :: flapping | ok).


%% @doc This function is used to initialize flapping records
%% the expiry time unit is minutes.
-spec(init_flapping(ClientId :: binary(), Interval :: integer()) -> flapping_record()).
init_flapping(ClientId, Interval) ->
    #flapping{client_id = ClientId,
              check_count = 1,
              timestamp = emqx_time:now_secs() + Interval}.

%% @doc This function is used to initialize flapping records
%% the expiry time unit is minutes.
-spec(check(Action :: atom(), ClientId :: binary(),
            Threshold :: {integer(), integer()}) -> flapping_state()).
check(Action, ClientId, Threshold = {_TimesThreshold, TimeInterval}) ->
    check(Action, ClientId, Threshold, init_flapping(ClientId, TimeInterval)).

-spec(check(Action :: atom(), ClientId :: binary(),
            Threshold :: {integer(), integer()},
            InitFlapping :: flapping_record()) -> flapping_state()).
check(Action, ClientId, Threshold, InitFlapping) ->
    case ets:update_counter(?FLAPPING_TAB, ClientId, {#flapping.check_count, 1}, InitFlapping) of
        1 -> ok;
        CheckCount ->
            case ets:lookup(?FLAPPING_TAB, ClientId) of
                [Flapping] ->
                    check_flapping(Action, CheckCount, Threshold, Flapping);
                _Flapping ->
                    ok
            end
    end.

check_flapping(Action, CheckCount, _Threshold = {TimesThreshold, TimeInterval},
               Flapping = #flapping{ client_id = ClientId
                                   , timestamp = Timestamp }) ->
    case emqx_time:now_secs() of
        NowTimestamp when NowTimestamp =< Timestamp,
                          CheckCount > TimesThreshold ->
            ets:delete(?FLAPPING_TAB, ClientId),
            flapping;
        NowTimestamp when NowTimestamp > Timestamp,
                          Action =:= disconnect ->
            ets:delete(?FLAPPING_TAB, ClientId),
            ok;
        NowTimestamp ->
            NewFlapping = Flapping#flapping{timestamp = NowTimestamp + TimeInterval},
            ets:insert(?FLAPPING_TAB, NewFlapping),
            ok
    end.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------
-spec(start_link(TimerInterval :: [integer()]) -> startlink_ret()).
start_link(TimerInterval) ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [TimerInterval], []).

init([TimerInterval]) ->
    TabOpts = [ public
              , set
              , {keypos, 2}
              , {write_concurrency, true}
              , {read_concurrency, true}],
    ok = emqx_tables:new(?FLAPPING_TAB, TabOpts),
    {ok, initialized, #{timer_interval => TimerInterval}}.

callback_mode() -> [state_functions, state_enter].

initialized(enter, _OldState, #{timer_interval := Time}) ->
    Action = {state_timeout, Time, clean_expired_records},
    {keep_state_and_data, Action};
initialized(state_timeout, clean_expired_records, #{}) ->
    clean_expired_records(),
    repeat_state_and_data.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _StateName, _State) ->
    emqx_tables:delete(?FLAPPING_TAB),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @doc clean expired records in ets
clean_expired_records() ->
    NowTime = emqx_time:now_secs(),
    MatchSpec = [{{'$1', '$2', '$3'},[{'<', '$3', NowTime}], [true]}],
    ets:select_delete(?FLAPPING_TAB, MatchSpec).
