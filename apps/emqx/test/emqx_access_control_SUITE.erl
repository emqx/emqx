%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_control_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules([router, broker]),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_authenticate(_) ->
    emqx_zone:set_env(zone, allow_anonymous, false),
    ?assertMatch({error, _}, emqx_access_control:authenticate(clientinfo())),
    emqx_zone:set_env(zone, allow_anonymous, true),
    ?assertMatch({ok, _}, emqx_access_control:authenticate(clientinfo())).

t_authorize(_) ->
    Publish = ?PUBLISH_PACKET(?QOS_0, <<"t">>, 1, <<"payload">>),
    ?assertEqual(allow, emqx_access_control:authorize(clientinfo(), Publish, <<"t">>)).

t_bypass_auth_plugins(_) ->
    ClientInfo = clientinfo(),
    emqx_zone:set_env(bypass_zone, allow_anonymous, true),
    emqx_zone:set_env(zone, allow_anonymous, false),
    emqx_zone:set_env(bypass_zone, bypass_auth_plugins, true),
    emqx:hook('client.authenticate',{?MODULE, auth_fun, []}),
    ?assertMatch({ok, _}, emqx_access_control:authenticate(ClientInfo#{zone => bypass_zone})),
    ?assertMatch({ok, _}, emqx_access_control:authenticate(ClientInfo)).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

auth_fun(#{zone := bypass_zone}, AuthRes) ->
             {stop, AuthRes#{auth_result => password_error}};
auth_fun(#{zone := _}, AuthRes) ->
             {stop, AuthRes#{auth_result => success}}.

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
