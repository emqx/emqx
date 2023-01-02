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

-module(emqx_mod_acl_internal_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_load_unload(_) ->
    ?assertEqual(ok, emqx_mod_acl_internal:unload([])),
    ?assertEqual(ok, emqx_mod_acl_internal:load([])),
    ?assertEqual({error,already_exists}, emqx_mod_acl_internal:load([])).

t_check_acl(_) ->
    emqx_mod_acl_internal:load([]),
    Rules=#{publish => [{allow,all}], subscribe => [{deny, all}]},
    ?assertEqual({ok, allow}, emqx_mod_acl_internal:check_acl(clientinfo(), publish,  <<"t">>, [], Rules)),
    ?assertEqual({ok, deny}, emqx_mod_acl_internal:check_acl(clientinfo(), subscribe,  <<"t">>, [], Rules)),
    ?assertEqual(ok, emqx_mod_acl_internal:check_acl(clientinfo(), connect,  <<"t">>, [], Rules)),
    emqx_mod_acl_internal:unload([]).

t_reload_acl(_) ->
    ?assertEqual(ok, emqx_mod_acl_internal:reload([])).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

clientinfo() -> clientinfo(#{}).
clientinfo(InitProps) ->
    maps:merge(#{zone       => zone,
                 protocol   => mqtt,
                 peerhost   => {127,0,0,1},
                 clientid   => <<"clientid">>,
                 username   => <<"username">>,
                 password   => <<"passwd">>,
                 is_superuser => false,
                 peercert   => undefined,
                 mountpoint => undefined
                }, InitProps).
