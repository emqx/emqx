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

-module(emqx_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(emqx_zone, [passthrough, no_history]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = meck:unload(emqx_zone).

t_info(_) ->
    meck:expect(emqx_zone, publish_limit, fun(_) -> {1, 10} end),
    Limiter = emqx_limiter:init([{rate_limit, {100, 1000}}]),
    #{pub_limit := #{rate   := 1,
                     burst  := 10,
                     tokens := 10
                    },
      rate_limit := #{rate   := 100,
                      burst  := 1000,
                      tokens := 1000
                     }
     } = emqx_limiter:info(Limiter).

t_check(_) ->
    meck:expect(emqx_zone, publish_limit, fun(_) -> {1, 10} end),
    Limiter = emqx_limiter:init([{rate_limit, {100, 1000}}]),
    lists:foreach(fun(I) ->
                          {ok, Limiter1} = emqx_limiter:check(#{cnt => I, oct => I*100}, Limiter),
                          #{pub_limit  := #{tokens := Cnt},
                            rate_limit := #{tokens := Oct}
                           } = emqx_limiter:info(Limiter1),
                          ?assertEqual({10 - I, 1000 - I*100}, {Cnt, Oct})
                  end, lists:seq(1, 10)).

t_check_pause(_) ->
    meck:expect(emqx_zone, publish_limit, fun(_) -> {1, 10} end),
    Limiter = emqx_limiter:init([{rate_limit, {100, 1000}}]),
    {pause, 1000, _} = emqx_limiter:check(#{cnt => 11, oct => 2000}, Limiter),
    {pause, 2000, _} = emqx_limiter:check(#{cnt => 10, oct => 1200}, Limiter).

