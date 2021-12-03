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

-module(emqx_message_latency_stats).

%% API
-export([ new/1, update/3, check_expire/4, latency/1]).

-export([get_threshold/0, update_threshold/1]).

-define(NOW, erlang:system_time(millisecond)).
-define(MINIMUM_INSERT_INTERVAL, 1000).
-define(MINIMUM_THRESHOLD, 500).
-define(THRESHOLD_KEY, {?MODULE, threshold}).

-opaque stats() :: #{ ema := emqx_moving_average:ema()
                    , last_update_time := timestamp()
                    , last_access_time := timestamp()  %% timestamp of last try to call hook
                    , last_insert_value := non_neg_integer()
                    }.

-type timestamp() :: non_neg_integer().
-type timespan() :: number().

-type latency_type() :: average
                      | expire.

-import(emqx_zone, [get_env/3]).

-export_type([stats/0, latency_type/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(non_neg_integer() | emqx_types:zone()) -> stats().
new(SamplesT) when is_integer(SamplesT) ->
    Samples = erlang:max(1, SamplesT),
    #{ ema => emqx_moving_average:new(exponential, #{period => Samples})
     , last_update_time => 0
     , last_access_time => 0
     , last_insert_value => 0
     };

new(Zone) ->
    Samples = get_env(Zone, latency_samples, 1),
    new(Samples).

-spec update(emqx_types:clientid(), number(), stats()) -> stats().
update(ClientId, Val, #{ema := EMA} = Stats) ->
    Now = ?NOW,
    #{average := Latency} = EMA2 = emqx_moving_average:update(Val, EMA),
    Stats2 = call_hook(ClientId, Now, average, Latency, Stats),
    Stats2#{ ema := EMA2
           , last_update_time := ?NOW}.

-spec check_expire(emqx_types:clientid(), timestamp(), timespan(), stats()) -> stats().
check_expire(_, Now, Interval, #{last_update_time := LUT} = S)
  when LUT >= Now - Interval ->
    S;

check_expire(ClientId, Now, _Interval, #{last_update_time := LUT} = S) ->
    Latency = Now - LUT,
    call_hook(ClientId, Now, expire, Latency, S).

-spec latency(stats()) -> number().
latency(#{ema := #{average := Average}}) ->
    Average.

-spec update_threshold(pos_integer()) -> pos_integer().
update_threshold(Threshold) ->
    Val = erlang:max(Threshold, ?MINIMUM_THRESHOLD),
    persistent_term:put(?THRESHOLD_KEY, Val),
    Val.

get_threshold() ->
    persistent_term:get(?THRESHOLD_KEY, ?MINIMUM_THRESHOLD).

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
-spec call_hook(emqx_types:clientid(), timestamp(), latency_type(), timespan(), stats()) -> stats().
call_hook(_, _, _, Latency, S)
  when Latency =< ?MINIMUM_THRESHOLD ->
    S;

call_hook(ClientId, Now, Type, Latency, #{last_insert_value := LIV} = Stats) ->
    case get_threshold() >= Latency of
        true ->
            Stats#{last_access_time := Now};
        _ ->
            ToInsert = erlang:floor(Latency),
            Arg = #{clientid => ClientId,
                    latency => ToInsert,
                    type => Type,
                    last_insert_value => LIV,
                    update_time => Now},
            emqx:run_hook('message.slow_subs_stats', [Arg]),
            Stats#{last_insert_value := ToInsert,
                   last_access_time := Now}
    end.
