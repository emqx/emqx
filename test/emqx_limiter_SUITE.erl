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

-module(emqx_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

t_info(_) ->
    #{pub_limit := #{rate   := 1,
                     burst  := 10,
                     tokens := 10
                    },
      rate_limit := #{rate   := 100,
                      burst  := 1000,
                      tokens := 1000
                     }
     } = emqx_limiter:info(limiter()).

t_check(_) ->
    lists:foreach(fun(I) ->
                          {ok, Limiter} = emqx_limiter:check(#{cnt => I, oct => I*100}, limiter()),
                          #{pub_limit  := #{tokens := Cnt},
                            rate_limit := #{tokens := Oct}
                           } = emqx_limiter:info(Limiter),
                          ?assertEqual({10 - I, 1000 - I*100}, {Cnt, Oct})
                  end, lists:seq(1, 10)).

t_check_pause(_) ->
    {pause, 1000, _} = emqx_limiter:check(#{cnt => 11, oct => 2000}, limiter()),
    {pause, 2000, _} = emqx_limiter:check(#{cnt => 10, oct => 1200}, limiter()).

limiter() ->
    emqx_limiter:init([{pub_limit, {1, 10}}, {rate_limit, {100, 1000}}]).

