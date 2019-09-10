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
    prepare_env(),
    Config.

prepare_env() ->
    emqx_zone:set_env(external, enable_flapping_detect, true),
    application:set_env(emqx, flapping_detect_policy,
                        #{threshold => 3,
                          duration => 100,
                          banned_interval => 200
                         }).

end_per_suite(_Config) ->
    ok.

t_detect_check(_) ->
    {ok, _Pid} = emqx_flapping:start_link(),
    Client = #{zone => external,
               client_id => <<"clientid">>,
               peername => {{127,0,0,1}, 5000}
              },
    false = emqx_flapping:detect(Client),
    false = emqx_flapping:check(Client),
    false = emqx_flapping:detect(Client),
    false = emqx_flapping:check(Client),
    true = emqx_flapping:detect(Client),
    timer:sleep(50),
    true = emqx_flapping:check(Client),
    timer:sleep(300),
    false = emqx_flapping:check(Client),
    ok = emqx_flapping:stop().

