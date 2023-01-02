%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_misc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_sn]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_sn]).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------
t_sn_app(_) ->
    ?assertMatch({'EXIT', {_, _}}, catch emqx_sn_app:start_listeners()),
    ?assertMatch({error, _}, emqx_sn_app:stop_listener({udp, 9999, []})),
    ?assertMatch({error, _}, emqx_sn_app:stop_listener({udp, {{0,0,0,0}, 9999}, []})),
    ok.

t_sn_broadcast(_) ->
    ?assertEqual(ignored, gen_server:call(emqx_sn_broadcast, ignored)),
    ?assertEqual(ok, gen_server:cast(emqx_sn_broadcast, ignored)),
    ?assertEqual(ignored, erlang:send(emqx_sn_broadcast, ignored)),
    ?assertEqual(broadcast_advertise, erlang:send(emqx_sn_broadcast, broadcast_advertise)),
    ?assertEqual(ok, emqx_sn_broadcast:stop()).

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------
