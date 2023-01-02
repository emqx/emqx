%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_sup_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_restart_shared_sub({init, Config}) ->
    emqx:subscribe(<<"t/a">>, #{share => <<"groupa">>}),
    true = exit(whereis(emqx_shared_sub), kill),
    %% waiting for restart
    timer:sleep(200), Config;
t_restart_shared_sub(Config) when is_list(Config) ->
    ?assert(is_pid(whereis(emqx_shared_sub))),
    emqx:publish(emqx_message:make(<<"t/a">>, <<"Hi">>)),
    ?assert(
       receive
           {deliver, _Topic, #message{payload = <<"Hi">>}} -> true
       after 2000 ->
                 false
       end);
t_restart_shared_sub({'end', _Config}) ->
    emqx:unsubscribe(<<"$share/grpa/t/a">>).
