%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_message_elapsed_stats).

%% API
-export([new/3, update/2, check_expire/3]).

-define(NOW, erlang:system_time(millisecond)).
-define(MINIMUM_INSERT_INTERVAL, 1000).
-define(MINIMUM_THRESHOLD, 100).

-type stats() :: #{ clientid := emqx_types:clientid()
                  , threshold := number()
                  , ema := emqx_moving_average:ema()
                  , last_update_time := timestamp()
                  , last_access_time := timestamp()  %% timestamp of last access top-k
                  , last_insert_value := float()
                  }.

-type timestamp() :: pos_integer().
-type timespan() :: pos_integer().

-type elapsed_type() :: average
                      | expire.

-export_type([stats/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec new(emqx_types:clientid(), non_neg_integer(), number()) -> stats().
new(ClientId, SamplesT, ThresholdT) ->
    Samples = erlang:max(1, SamplesT),
    Threshold = erlang:max(?MINIMUM_THRESHOLD, ThresholdT),
    #{ clientid => ClientId
     , ema => emqx_moving_average:new(exponential, #{period => Samples})
     , threshold => Threshold
     , last_updated_time => 0
     , last_access_time => 0
     , last_insert_value => 0
     }.

-spec update(float(), stats()) -> stats().
update(Val, #{ema := EMA} = Stats) ->
    Now = ?NOW,
    #{average := Elapsed} = EMA2 = emqx_moving_average:update(Val, EMA),
    Stats2 = call_hook(Now, average, Elapsed, Stats),
    Stats2#{ ema := EMA2
           , last_updated_time := ?NOW}.

-spec check_expire(timestamp(), timespan(), stats()) -> stats().
check_expire(Now, Interval, #{last_update_time := LUT} = S)
  when LUT >= Now - Interval ->
    S;

check_expire(Now, _Interval, #{last_update_time := LUT} = S) ->
    Elapsed = Now - LUT,
    call_hook(Now, expire, Elapsed, S).

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
-spec call_hook(timestamp(), elapsed_type(), timespan(), stats()) -> stats().
call_hook(Now, _, _, #{last_access_time := LIT} = S) when LIT >= Now - ?MINIMUM_INSERT_INTERVAL ->
    S;

call_hook(_, _, Elapsed, #{threshold := Threshold} = S)
  when Elapsed =< Threshold ->
    S;

call_hook(Now, Type, Elapsed, #{clientid := ClientId, last_insert_value := LIV} = Stats) ->
    Arg = #{clientid => ClientId,
            elapsed => Elapsed,
            type => Type,
            last_insert_value => LIV,
            timestamp => Now},
    emqx:run_hook('message.slow_subs_stats', [Arg]),
    Stats#{last_insert_value := Elapsed,
           last_access_time := Now}.
