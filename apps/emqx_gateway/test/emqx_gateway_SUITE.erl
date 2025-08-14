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

-module(emqx_gateway_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(GWNAME, mqttsn).
-define(CONF_DEFAULT, <<"gateway {}">>).

-define(AUTHN_CONF, #{
    enable => true,
    mechanism => password_based,
    backend => built_in_database,
    password_hash_algorithm => #{name => sha256, salt_position => suffix},
    user_id_type => username
}).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_gateway_test_utils:load_all_gateway_apps(),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_resource,
            emqx_gateway_lwm2m,
            emqx_gateway,
            emqx_auth,
            emqx_auth_redis,
            emqx_auth_mnesia
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

init_per_testcase(t_get_basic_usage_info_2, Config) ->
    DataDir = ?config(data_dir, Config),
    ok = setup_fake_usage_data(DataDir),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_get_basic_usage_info_2, _Config) ->
    emqx_gateway_cm:unregister_channel(lwm2m, <<"client_id">>),
    ok = emqx_gateway:unload(lwm2m),
    {ok, _} = emqx_conf:update([gateway], #{}, #{override_to => cluster}),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_registered_gateway(_) ->
    ?assertMatch(
        [{coap, #{cbkmod := emqx_gateway_coap}} | _],
        lists:sort(emqx_gateway:registered_gateway())
    ).

t_load_unload_list_lookup(_) ->
    {ok, _} = emqx_gateway:load(?GWNAME, #{idle_timeout => 1000}),
    ?assertEqual(
        {error, alredy_existed},
        emqx_gateway:load(?GWNAME, #{})
    ),
    ?assertEqual(
        {error, {unknown_gateway_name, bad_gw_name}},
        emqx_gateway:load(bad_gw_name, #{})
    ),

    ?assertEqual(1, length(emqx_gateway:list())),
    ?assertEqual(
        emqx_gateway:lookup(?GWNAME),
        lists:nth(1, emqx_gateway:list())
    ),

    ?assertEqual(ok, emqx_gateway:unload(?GWNAME)),
    ?assertEqual({error, not_found}, emqx_gateway:unload(?GWNAME)).

t_start_stop_update(_) ->
    {ok, _} = emqx_gateway:load(?GWNAME, #{idle_timeout => 1000}),

    #{status := running} = emqx_gateway:lookup(?GWNAME),

    ok = emqx_gateway:stop(?GWNAME),
    {error, already_stopped} = emqx_gateway:stop(?GWNAME),

    #{status := stopped} = emqx_gateway:lookup(?GWNAME),

    ok = emqx_gateway:update(
        ?GWNAME, #{enable => false, idle_timeout => 2000}
    ),
    #{
        status := stopped,
        config := #{idle_timeout := 2000}
    } = emqx_gateway:lookup(?GWNAME),

    ok = emqx_gateway:update(
        ?GWNAME, #{enable => true, idle_timeout => 3000}
    ),
    #{
        status := running,
        config := #{idle_timeout := 3000}
    } = emqx_gateway:lookup(?GWNAME),

    ok = emqx_gateway:update(
        ?GWNAME, #{enable => false, idle_timeout => 4000}
    ),
    #{
        status := stopped,
        config := #{idle_timeout := 4000}
    } = emqx_gateway:lookup(?GWNAME),

    ok = emqx_gateway:start(?GWNAME),
    #{
        status := running,
        config := #{idle_timeout := 4000}
    } = emqx_gateway:lookup(?GWNAME),

    {error, already_started} = emqx_gateway:start(?GWNAME),
    ok.

t_get_basic_usage_info_empty(_Config) ->
    ?assertEqual(
        #{},
        emqx_gateway:get_basic_usage_info()
    ).

t_get_basic_usage_info_1(_Config) ->
    {ok, _} = emqx_gateway:load(?GWNAME, #{idle_timeout => 1000}),
    try
        ?assertEqual(
            #{
                mqttsn =>
                    #{
                        authn => <<"undefined">>,
                        listeners => [],
                        num_clients => 0
                    }
            },
            emqx_gateway:get_basic_usage_info()
        )
    after
        ok = emqx_gateway:unload(?GWNAME)
    end.

t_get_basic_usage_info_2(_Config) ->
    ?assertMatch(
        #{
            lwm2m :=
                #{
                    authn := <<"password_based:redis">>,
                    listeners :=
                        [
                            #{
                                authn :=
                                    <<"password_based:built_in_database">>,
                                type := udp
                            }
                        ],
                    num_clients := 1
                }
        },
        emqx_gateway:get_basic_usage_info()
    ).

t_authn_data_not_be_cleared_when_gateway_app_restarted(_Config) ->
    GwName = stomp,
    ChainName = emqx_gateway_utils:global_chain(GwName),
    AssertUserExists = fun() ->
        ?assertMatch(
            #{
                data := [
                    #{
                        user_id := <<"test">>
                    }
                ]
            },
            emqx_authn_chains:list_users(
                ChainName,
                <<"password_based:built_in_database">>,
                #{like_user_id => <<"test">>}
            )
        )
    end,

    {ok, _} = emqx_gateway:load(GwName, #{authentication => ?AUTHN_CONF}),
    emqx_authn_chains:add_user(
        ChainName,
        <<"password_based:built_in_database">>,
        #{user_id => <<"test">>, password => <<"test">>}
    ),

    %% user added successfully
    AssertUserExists(),
    %% restart gateway app
    application:stop(emqx_gateway),
    application:start(emqx_gateway),
    %% assert: user should not be deleted if the gateway app is restarted
    AssertUserExists(),

    %% delete user
    emqx_authn_chains:delete_user(
        ChainName,
        <<"password_based:built_in_database">>,
        <<"test">>
    ),
    %% user deleted successfully
    ?assertMatch(
        #{
            data := []
        },
        emqx_authn_chains:list_users(
            ChainName,
            <<"password_based:built_in_database">>,
            #{like_user_id => <<"test">>}
        )
    ),
    ok.

%%--------------------------------------------------------------------
%% helper functions
%%--------------------------------------------------------------------

read_lwm2m_conf(DataDir) ->
    ConfPath = filename:join([DataDir, "lwm2m.conf"]),
    {ok, Conf} = file:read_file(ConfPath),
    Conf.

setup_fake_usage_data(Lwm2mDataDir) ->
    XmlDir = filename:join(
        [
            emqx_common_test_helpers:proj_root(),
            "apps",
            "emqx_gateway_lwm2m",
            "lwm2m_xml"
        ]
    ),
    Lwm2mConf = read_lwm2m_conf(Lwm2mDataDir),
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, Lwm2mConf),
    emqx_config:put([gateway, lwm2m, xml_dir], XmlDir),
    {ok, _} = emqx_gateway:load(lwm2m, emqx_config:get([gateway, lwm2m])),
    %% to simulate a connection
    FakeConnInfo = #{conn_mod => fake_conn_mod},
    FakeChanPid = self(),
    ok = emqx_gateway_cm:register_channel(lwm2m, <<"client_id">>, FakeChanPid, FakeConnInfo),
    ok.
