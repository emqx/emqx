%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_init_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CLIENTINFO, #{
    zone => default,
    listener => {tcp, default},
    protocol => mqtt,
    peerhost => {127, 0, 0, 1},
    clientid => <<"clientid">>,
    username => <<"username">>,
    password => <<"passwd">>,
    is_superuser => false,
    mountpoint => undefined
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_access_control:set_default_authn_restrictive(),
    Config.

end_per_suite(Config) ->
    emqx_access_control:set_default_authn_permissive(),
    Config.

init_per_testcase(_Case, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf,
                "authentication = [{mechanism = password_based, backend = built_in_database}]"}
        ],
        #{
            work_dir => ?config(priv_dir, Config)
        }
    ),
    ok = snabbkaffe:start_trace(),
    [{apps, Apps} | Config].

end_per_testcase(_Case, Config) ->
    ok = snabbkaffe:stop(),
    _ = application:stop(emqx_auth),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

t_initialize(_Config) ->
    ?assertMatch(
        {error, not_authorized},
        emqx_access_control:authenticate(?CLIENTINFO)
    ),
    ok = application:start(emqx_auth),
    ?assertMatch(
        {error, not_authorized},
        emqx_access_control:authenticate(?CLIENTINFO)
    ),
    %% call emqx_authn_chains:register_providers/1
    %% which triggers a handle_continue to store the chain in ets
    ok = emqx_authn_test_lib:register_fake_providers([{password_based, built_in_database}]),
    %% make another gen_server call to make sure the handle_continue is complete
    ?assertMatch(#{{password_based, built_in_database} := _}, emqx_authn_chains:get_providers()),
    ?assertMatch(
        {error, bad_username_or_password},
        emqx_access_control:authenticate(?CLIENTINFO)
    ),

    _ = emqx_authn_chains:add_user(
        'mqtt:global',
        <<"password_based:built_in_database">>,
        #{user_id => <<"username">>, password => <<"passwd">>}
    ),

    ?assertMatch(
        {ok, _},
        emqx_access_control:authenticate(?CLIENTINFO)
    ).
