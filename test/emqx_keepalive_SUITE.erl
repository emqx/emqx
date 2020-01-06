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

-module(emqx_keepalive_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_check(_) ->
    Keepalive = emqx_keepalive:init(60),
    ?assertEqual(60, emqx_keepalive:info(interval, Keepalive)),
    ?assertEqual(0, emqx_keepalive:info(statval, Keepalive)),
    ?assertEqual(0, emqx_keepalive:info(repeat, Keepalive)),
    Info = emqx_keepalive:info(Keepalive),
    ?assertEqual(#{interval => 60,
                   statval  => 0,
                   repeat   => 0}, Info),
    {ok, Keepalive1} = emqx_keepalive:check(1, Keepalive),
    ?assertEqual(1, emqx_keepalive:info(statval, Keepalive1)),
    ?assertEqual(0, emqx_keepalive:info(repeat, Keepalive1)),
    {ok, Keepalive2} = emqx_keepalive:check(1, Keepalive1),
    ?assertEqual(1, emqx_keepalive:info(statval, Keepalive2)),
    ?assertEqual(1, emqx_keepalive:info(repeat, Keepalive2)),
    ?assertEqual({error, timeout}, emqx_keepalive:check(1, Keepalive2)).

