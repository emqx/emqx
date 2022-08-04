%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_file_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(RAW_SOURCE, #{
    <<"type">> => <<"file">>,
    <<"enable">> => true,
    <<"rules">> =>
        <<
            "{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}."
            "\n{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}."
        >>
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authz],
        fun set_special_configs/1
    ),
    %% meck after authz started
    meck:expect(
        emqx_authz,
        acl_conf_file,
        fun() ->
            emqx_common_test_helpers:deps_path(emqx_authz, "etc/acl.conf")
        end
    ),
    Config.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();
set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_ok(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = setup_config(?RAW_SOURCE#{
        <<"rules">> => <<"{allow, {user, \"username\"}, publish, [\"t\"]}.">>
    }),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, publish, <<"t">>)
    ),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, subscribe, <<"t">>)
    ).

t_superuser(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        is_superuser => true,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    %% no rules apply to superuser
    ok = setup_config(?RAW_SOURCE#{
        <<"rules">> => <<"{deny, {user, \"username\"}, publish, [\"t\"]}.">>
    }),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, publish, <<"t">>)
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, subscribe, <<"t">>)
    ).

t_invalid_file(_Config) ->
    ?assertMatch(
        {error, bad_acl_file_content},
        emqx_authz:update(?CMD_REPLACE, [?RAW_SOURCE#{<<"rules">> => <<"{{invalid term">>}])
    ).

t_update(_Config) ->
    ok = setup_config(?RAW_SOURCE#{
        <<"rules">> => <<"{allow, {user, \"username\"}, publish, [\"t\"]}.">>
    }),

    ?assertMatch(
        {error, _},
        emqx_authz:update(
            {?CMD_REPLACE, file},
            ?RAW_SOURCE#{<<"rules">> => <<"{{invalid term">>}
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authz:update(
            {?CMD_REPLACE, file}, ?RAW_SOURCE
        )
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
        ?RAW_SOURCE,
        SpecialParams
    ).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
