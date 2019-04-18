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

%% @doc TODO:
%% 1. Flapping Detection
%% 2. Conflict Detection?


%% @doc flapping detect algorithm
%% * Storing the results of the last 21 checks of the host or service
%% * Analyzing the historical check results and determine where state
%%   changes/transitions occur
%% * Using the state transitions to determine a percent state change value
%%   (a measure of change) for the host or service
%% * Comparing the percent state change value against low and high flapping thresholds
-module(emqx_flapping).

-include("emqx.hrl").
-include("logger.hrl").
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

-export([check/4]).

-record(flapping,
        { client_id   :: binary()
        , check_times :: integer()
        , timestamp   :: integer()
        , expire_time :: integer()
        }).

-type(flapping_record() :: #flapping{}).
-type(flapping_state() :: flapping | ok).


%% @doc This function is used to initialize flapping records
%% the expiry time unit is minutes.
-spec(init_flapping(ClientId :: binary(), ExpiryInterval :: integer())
      -> flapping_record()).
init_flapping(ClientId, ExpiryInterval) ->
    #flapping{ client_id = ClientId
             , check_times = 1
             , timestamp = emqx_time:now_secs()
             , expire_time = emqx_time:now_secs() + ExpiryInterval
             }.

%% @doc This function is used to initialize flapping records
%% the expiry time unit is minutes.
-spec(check( Action :: atom()
           , ClientId :: binary()
           , ExpiryInterval :: integer()
           , Threshold :: {integer(), integer()})
      -> flapping_state()).
check(Action, ClientId, ExpiryInterval, Threshold) ->
    check(Action, ClientId, ExpiryInterval, Threshold, init_flapping(ClientId, ExpiryInterval)).

-spec(check( Action :: atom()
           , ClientId :: binary()
           , ExpiryInterval :: integer()
           , Threshold :: {integer(), integer()}
           , InitFlapping :: flapping_record())
      -> flapping_state()).
check(Action, ClientId, ExpiryInterval, Threshold, InitFlapping) ->
    Pos = #flapping.check_times,
    try ets:update_counter(?FLAPPING_TAB, ClientId, {Pos, 1}) of
        CheckTimes ->
            case ets:lookup(?FLAPPING_TAB, ClientId) of
                [Flapping] ->
                    check_flapping(Action, CheckTimes, ExpiryInterval, Threshold, Flapping);
                _Flapping ->
                    ok
            end
    catch
        error:badarg ->
            ets:insert_new(?FLAPPING_TAB, InitFlapping),
            ok
    end.

-spec(check_flapping( Action :: atom()
                    , CheckTimes :: integer()
                    , ExpiryInterval :: integer()
                    , Threshold :: {integer(), integer()}
                    , InitFlapping :: flapping_record())
      -> flapping_state()).
check_flapping(Action, CheckTimes, ExpiryInterval,
               _Threshold = {TimesThreshold, TimeInterval},
               Flapping = #flapping{ client_id = ClientId
                                   , timestamp = TimeStamp }) ->
    TimeDiff = emqx_time:now_secs() - TimeStamp,
    case TimeDiff of
        Minutes when Minutes < TimeInterval,
                     CheckTimes > TimesThreshold ->
            flapping;
        Minutes when Minutes =< ExpiryInterval ->
            Now = emqx_time:now_secs(),
            NewFlapping = Flapping#flapping{ timestamp = Now,
                                             expire_time = Now + ExpiryInterval},
            ets:insert(?FLAPPING_TAB, NewFlapping),
            ok;
        _Minutes when Action =:= offline ->
            ets:delete(?FLAPPING_TAB, ClientId),
            ok;
        _Minutes ->
            ok
    end.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------
-spec(start_link(Config :: list() | map()) -> startlink_ret()).
start_link(Config) when is_list(Config) ->
    start_link(maps:from_list(Config));
start_link(Config) ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, Config, []).

init(Config) ->
    TabOpts = [ public
              , set
              , {keypos, 2}
              , {write_concurrency, true}
              , {read_concurrency, true}],
    ok = emqx_tables:new(?FLAPPING_TAB, TabOpts),
    Timer = maps:get(timer, Config),
    {ok, initialized, #{timer => Timer}}.

callback_mode() -> [state_functions, state_enter].

initialized(enter, _OldState, #{timer := Time}) ->
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
    Records = ets:tab2list(?FLAPPING_TAB),
    traverse_records(Records).

traverse_records([]) ->
    ok;
traverse_records([#flapping{ client_id = ClientId,
                             expire_time = ExpiredTime} | LeftRecords]) ->
    case emqx_time:now_secs() > ExpiredTime of
        true ->
            ets:delete(?FLAPPING_TAB, ClientId);
        false ->
            true
    end,
    traverse_records(LeftRecords).
