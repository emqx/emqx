%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_client).

%% API
-export([create/5, make_ref/3, consume/2]).
-export_type([limiter/0]).

%% tocket bucket algorithm
-record(limiter, { tokens :: non_neg_integer()
                 , rate :: float()
                 , capacity :: decimal()
                 , lasttime :: millisecond()
                 , ref :: ref_limiter()
                 }).

-record(ref, { counter :: counters:counters_ref()
             , index :: index()
             , rate :: decimal()
             , obtained :: non_neg_integer()
             }).

%% TODO
%% we should add a nop-limiter, when all the upper layers (global, zone, and buckets ) are infinity

-type limiter() :: #limiter{}.
-type ref_limiter() :: #ref{}.
-type client() :: limiter() | ref_limiter().
-type millisecond() :: non_neg_integer().
-type pause_result(Client) :: {pause, millisecond(), Client}.
-type consume_result(Client) :: {ok, Client}
                              | pause_result(Client).
-type decimal() :: emqx_limiter_decimal:decimal().
-type index() :: emqx_limiter_server:index().

-define(NOW, erlang:monotonic_time(millisecond)).
-define(MINIUMN_PAUSE, 100).

-import(emqx_limiter_decimal, [sub/2]).
%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
-spec create(float(),
             decimal(),
             counters:counters_ref(),
             index(),
             decimal()) -> limiter().
create(Rate, Capacity, Counter, Index, CounterRate) ->
    #limiter{ tokens = Capacity
            , rate = Rate
            , capacity = Capacity
            , lasttime = ?NOW
            , ref = make_ref(Counter, Index, CounterRate)
            }.

-spec make_ref(counters:counters_ref(), index(), decimal()) -> ref_limiter().
make_ref(Counter, Idx, Rate) ->
    #ref{counter = Counter, index = Idx, rate = Rate, obtained = 0}.

-spec consume(pos_integer(), Client) -> consume_result(Client)
              when Client :: client().
consume(Need, #limiter{tokens = Tokens,
                       capacity = Capacity} = Limiter) ->
    if Need =< Tokens ->
            try_consume_counter(Need, Limiter);
       Need > Capacity ->
            %% FIXME
            %% The client should be able to send 4kb data if the rate is configured to be 2kb/s, it just needs 2s to complete.
            throw("too big request"); %% FIXME how to deal this?
       true ->
            try_reset(Need, Limiter)
    end;

consume(Need, #ref{counter = Counter,
                   index = Index,
                   rate = Rate,
                   obtained = Obtained} = Ref) ->
    Tokens = counters:get(Counter, Index),
    if Tokens >= Need ->
            counters:sub(Counter, Index, Need),
            {ok, Ref#ref{obtained = Obtained + Need}};
       true ->
            return_pause(Need - Tokens, Rate, Ref)
    end.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
-spec try_consume_counter(pos_integer(), limiter()) -> consume_result(limiter()).
try_consume_counter(Need,
                    #limiter{tokens = Tokens,
                             ref = #ref{counter = Counter,
                                        index = Index,
                                        obtained = Obtained,
                                        rate = CounterRate} = Ref} = Limiter) ->
    CT = counters:get(Counter, Index),
    if CT >= Need ->
            counters:sub(Counter, Index, Need),
            {ok, Limiter#limiter{tokens = sub(Tokens, Need),
                                 ref = Ref#ref{obtained = Obtained + Need}}};
       true ->
            return_pause(Need - CT, CounterRate, Limiter)
    end.

-spec try_reset(pos_integer(), limiter()) -> consume_result(limiter()).
try_reset(Need,
          #limiter{tokens = Tokens,
                   rate = Rate,
                   lasttime = LastTime,
                   capacity = Capacity} = Limiter) ->
    Now = ?NOW,
    Inc = erlang:floor((Now - LastTime) * Rate / emqx_limiter_schema:minimum_period()),
    Tokens2 = erlang:min(Tokens + Inc, Capacity),
    if Need > Tokens2 ->
            return_pause(Need, Rate, Limiter);
       true ->
            Limiter2 = Limiter#limiter{tokens = Tokens2,
                                       lasttime = Now},
            try_consume_counter(Need, Limiter2)
    end.

-spec return_pause(pos_integer(), decimal(), Client) -> pause_result(Client)
              when Client :: client().
return_pause(_, infinity, Limiter) ->
    %% workaround when emqx_limiter_server's rate is infinity
    {pause, ?MINIUMN_PAUSE, Limiter};

return_pause(Diff, Rate, Limiter) ->
    Pause = erlang:round(Diff * emqx_limiter_schema:minimum_period() / Rate),
    {pause, erlang:max(Pause, ?MINIUMN_PAUSE), Limiter}.
