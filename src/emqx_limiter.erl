%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter).

-include("types.hrl").

-export([init/1, info/1, check/2]).

-import(emqx_misc, [maybe_apply/2]).

-record(limiter, {
          %% Publish Limit
          pub_limit :: maybe(esockd_rate_limit:bucket()),
          %% Rate Limit
          rate_limit :: maybe(esockd_rate_limit:bucket())
         }).

-type(limiter() :: #limiter{}).

-export_type([limiter/0]).

-define(ENABLED(Rl), (Rl =/= undefined)).

-spec(init(proplists:proplist()) -> maybe(limiter())).
init(Options) ->
    Pl = proplists:get_value(pub_limit, Options),
    Rl = proplists:get_value(rate_limit, Options),
    case ?ENABLED(Pl) or ?ENABLED(Rl) of
        true  -> #limiter{pub_limit  = init_limit(Pl),
                          rate_limit = init_limit(Rl)
                         };
        false -> undefined
    end.

init_limit(Rl) ->
    maybe_apply(fun esockd_rate_limit:new/1, Rl).

info(#limiter{pub_limit = Pl, rate_limit = Rl}) ->
    #{pub_limit => info(Pl), rate_limit => info(Rl)};

info(Rl) ->
    maybe_apply(fun esockd_rate_limit:info/1, Rl).

check(#{cnt := Cnt, oct := Oct}, Limiter = #limiter{pub_limit  = Pl,
                                                    rate_limit = Rl}) ->
    do_check([{#limiter.pub_limit, Cnt, Pl} || ?ENABLED(Pl)] ++
             [{#limiter.rate_limit, Oct, Rl} || ?ENABLED(Rl)], Limiter).

do_check([], Limiter) ->
    {ok, Limiter};
do_check([{Pos, Cnt, Rl}|More], Limiter) ->
    case esockd_rate_limit:check(Cnt, Rl) of
        {0, Rl1} ->
            do_check(More, setelement(Pos, Limiter, Rl1));
        {Pause, Rl1} ->
            {pause, Pause, setelement(Pos, Limiter, Rl1)}
    end.

