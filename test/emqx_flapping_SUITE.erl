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

-module(emqx_flapping_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    prepare_for_test(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%% t_flapping(_Config) ->
%%     process_flag(trap_exit, true),
%%     flapping_connect(5),
%%     {ok, C} = emqtt:start_link([{client_id, <<"Client">>}]),
%%     {error, _} = emqtt:connect(C),
%%     receive
%%         {'EXIT', Client, _Reason} ->
%%             ct:log("receive exit signal, Client: ~p", [Client])
%%     after 1000 ->
%%             ct:log("timeout")
%%     end.

flapping_connect(Times) ->
    lists:foreach(fun do_connect/1, lists:seq(1, Times)).

do_connect(_I) ->
    {ok, C} = emqtt:start_link([{client_id, <<"Client">>}]),
    {ok, _} = emqtt:connect(C),
    ok = emqtt:disconnect(C).

prepare_for_test() ->
    ok = emqx_zone:set_env(external, enable_flapping_detect, true),
    ok = emqx_zone:set_env(external, flapping_threshold, {10, 60}),
    ok = emqx_zone:set_env(external, flapping_expiry_interval, 3600).
