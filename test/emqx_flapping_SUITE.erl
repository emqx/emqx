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

-module(emqx_flapping_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([], fun set_special_configs/1),
    Config.

set_special_configs(emqx) ->
    emqx_zone:set_env(external, enable_flapping_detect, true),
    application:set_env(emqx, flapping_detect_policy,
                        #{threshold => 3,
                          duration => 100,
                          banned_interval => 2
                         });
set_special_configs(_App) -> ok.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]),
    ekka_mnesia:delete_schema(),    %% Clean emqx_banned table
    ok.

t_detect_check(_) ->
    ClientInfo = #{zone => external,
                   clientid => <<"clientid">>,
                   peerhost => {127,0,0,1}
                  },
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_banned:check(ClientInfo),
    false = emqx_flapping:detect(ClientInfo),
    false = emqx_banned:check(ClientInfo),
    true = emqx_flapping:detect(ClientInfo),
    timer:sleep(100),
    true = emqx_banned:check(ClientInfo),
    timer:sleep(3000),
    false = emqx_banned:check(ClientInfo),
    Childrens = supervisor:which_children(emqx_cm_sup),
    {flapping, Pid, _, _} = lists:keyfind(flapping, 1, Childrens),
    gen_server:call(Pid, unexpected_msg),
    gen_server:cast(Pid, unexpected_msg),
    Pid ! test,
    ok = emqx_flapping:stop().

