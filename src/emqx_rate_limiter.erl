%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%-------------------------------------------------------------------

-module(emqx_rate_limiter).

-export([new/3, check/2]).

-record(token_bucket, {
                       remain_tokens :: non_neg_integer(), % number of remained tokens in token bucket
                       interval      :: pos_integer(),     % specified interval time milliseconds
                       new_tokens    :: pos_integer(),     % number of new tokens
                       burst_size    :: non_neg_integer(), % max number of tokens required
                       last_time     :: pos_integer()      % milliseconds
                      }).

-type(token_bucket() :: #token_bucket{}).

-export_type([token_bucket/0]).

%%------------------------------------------------------------------
%% Note:
%%
%%   Four parameters :
%%       * enable.rate.limit
%%       * rate.limit.average
%%       * rate.limit.time.unit
%%       * rate.limit.burst.size
%%        b ->  bucket size
%%        R -> transmission rate
%%        r -> token added rate or limit rate
%%        burst_time = b / (R-r)
%%        burst_size = burst_time * R
%%        b = burst_size / R * (R - r)
%%        r =  tokens / TimeUnit or limit rate
%%
%%------------------------------------------------------------------

%% @doc Create rate limiter bucket.
-spec(new(non_neg_integer(), pos_integer(), pos_integer()) -> token_bucket()).
new(BurstSize, NewTokens, Interval) when BurstSize > NewTokens ->
    #token_bucket{ burst_size = BurstSize,
                   interval = Interval,
                   new_tokens = NewTokens,
                   remain_tokens = NewTokens,
                   last_time = emqx_time:now_ms()}.

%% @doc Check inflow data.
-spec(check(non_neg_integer(), token_bucket()) -> {non_neg_integer(), token_bucket()}).
check(Data, TokenBucket = #token_bucket{burst_size = BurstSize,
                                        remain_tokens = RemainTokens,
                                        interval = Interval,
                                        new_tokens = NewTokens ,
                                        last_time = LastTime}) ->
    Tokens = erlang:min(BurstSize, RemainTokens
                        + trunc(NewTokens
                                * (emqx_time:now_secs() - LastTime)
                                / Interval)),
    {Pause1, NewTokenBucket} =
        case Tokens >= Data of
            true  -> %% Tokens available
                {0, TokenBucket#token_bucket{remain_tokens = Tokens - Data,
                                  last_time = emqx_rate_limiter:now_ms()}};
            false -> %% Tokens not enough
                Pause = round((Data - Tokens) * Interval /NewTokens),
                {Pause, TokenBucket#token_bucket{remain_tokens = 0,
                                                 last_time = emqx_time:now_ms()
                                                 + Pause}}
        end,
    {Pause1, NewTokenBucket}.
