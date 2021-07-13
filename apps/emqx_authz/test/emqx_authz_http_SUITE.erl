%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end ),
    ok = emqx_ct_helpers:start_apps([emqx_authz], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    file:delete(filename:join(emqx:get_env(plugins_etc_dir), 'authz.conf')),
    emqx_ct_helpers:stop_apps([emqx_authz, emqx_resource]),
    meck:unload(emqx_resource).

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, acl_nomatch, deny),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/loaded_plguins")),
    ok;
set_special_configs(emqx_authz) ->
    Rules = [#{config =>#{
                 url => #{host => "fake.com",
                          path => "/",
                          port => 443,
                          scheme => https},
                 headers => #{},
                 method => get,
                 request_timeout => 5000
                },
               principal => all,
               type => http}
            ],
    emqx_config:put([emqx_authz], #{rules => Rules}),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_) ->
    ClientInfo = #{clientid => <<"clientid">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1},
                   protocol => mqtt,
                   mountpoint => <<"fake">>
                   },

    meck:expect(emqx_resource, query, fun(_, _) -> {ok, 204, fake_headers} end),
    ?assertEqual(allow,
                 emqx_access_control:authorize(ClientInfo, subscribe, <<"#">>)),

    meck:expect(emqx_resource, query, fun(_, _) -> {ok, 200, fake_headers, fake_body} end),
    ?assertEqual(allow,
                 emqx_access_control:authorize(ClientInfo, publish, <<"#">>)),


    meck:expect(emqx_resource, query, fun(_, _) -> {error, other} end),
    ?assertEqual(deny,
        emqx_access_control:authorize(ClientInfo, subscribe, <<"+">>)),
    ?assertEqual(deny,
        emqx_access_control:authorize(ClientInfo, publish, <<"+">>)),
    ok.

