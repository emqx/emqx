%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_conf_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_conf.hrl").
-import(emqx_config_SUITE, [prepare_conf_file/3]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_authz]),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite([emqx_conf, emqx_authz]).

t_load_config(Config) ->
    Authz = authorization,
    Conf = emqx_conf:get_raw([Authz]),
    %% set sources to []
    ConfBin0 = hocon_pp:do(#{<<"authorization">> => Conf#{<<"sources">> => []}}, #{}),
    ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
    ok = emqx_conf_cli:conf(["load", ConfFile0]),
    ?assertEqual(Conf#{<<"sources">> => []}, emqx_conf:get_raw([Authz])),
    %% remove sources, it will reset to default file source.
    ConfBin1 = hocon_pp:do(#{<<"authorization">> => maps:remove(<<"sources">>, Conf)}, #{}),
    ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, Config),
    ok = emqx_conf_cli:conf(["load", ConfFile1]),
    Default = [emqx_authz_schema:default_authz()],
    ?assertEqual(Conf#{<<"sources">> => Default}, emqx_conf:get_raw([Authz])),
    %% reset
    ConfBin2 = hocon_pp:do(#{<<"authorization">> => Conf}, #{}),
    ConfFile2 = prepare_conf_file(?FUNCTION_NAME, ConfBin2, Config),
    ok = emqx_conf_cli:conf(["load", ConfFile2]),
    ?assertEqual(
        Conf#{<<"sources">> => [emqx_authz_schema:default_authz()]},
        emqx_conf:get_raw([Authz])
    ),
    ?assertEqual({error, empty_hocon_file}, emqx_conf_cli:conf(["load", "non-exist-file"])),
    ok.

t_load_readonly(Config) ->
    Base0 = base_conf(),
    Base1 = Base0#{<<"mqtt">> => emqx_conf:get_raw([mqtt])},
    lists:foreach(
        fun(Key) ->
            KeyBin = atom_to_binary(Key),
            Conf = emqx_conf:get_raw([Key]),
            ConfBin0 = hocon_pp:do(Base1#{KeyBin => Conf}, #{}),
            ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
            ?assertEqual(
                {error, "update_readonly_keys_prohibited"},
                emqx_conf_cli:conf(["load", ConfFile0])
            ),
            %% reload etc/emqx.conf changed readonly keys
            ConfBin1 = hocon_pp:do(Base1#{KeyBin => changed(Key)}, #{}),
            ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, Config),
            application:set_env(emqx, config_files, [ConfFile1]),
            ?assertMatch({error, [{Key, #{changed := _}}]}, emqx_conf_cli:conf(["reload"]))
        end,
        ?READONLY_KEYS
    ),
    ok.

t_error_schema_check(Config) ->
    Base = #{
        %% bad multiplier
        <<"mqtt">> => #{<<"keepalive_multiplier">> => -1},
        <<"zones">> => #{<<"my-zone">> => #{<<"mqtt">> => #{<<"keepalive_multiplier">> => 10}}}
    },
    ConfBin0 = hocon_pp:do(Base, #{}),
    ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
    ?assertMatch({error, _}, emqx_conf_cli:conf(["load", ConfFile0])),
    %% zones is not updated because of error
    ?assertEqual(#{}, emqx_config:get_raw([zones])),
    ok.

t_reload_etc_emqx_conf_not_persistent(Config) ->
    Mqtt = emqx_conf:get_raw([mqtt]),
    Base = base_conf(),
    Conf = Base#{<<"mqtt">> => Mqtt#{<<"keepalive_multiplier">> => 3}},
    ConfBin = hocon_pp:do(Conf, #{}),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    ok = emqx_conf_cli:conf(["reload"]),
    ?assertEqual(3, emqx:get_config([mqtt, keepalive_multiplier])),
    ?assertNotEqual(
        3,
        emqx_utils_maps:deep_get(
            [<<"mqtt">>, <<"keepalive_multiplier">>],
            emqx_config:read_override_conf(#{}),
            undefined
        )
    ),
    ok.

base_conf() ->
    #{
        <<"cluster">> => emqx_conf:get_raw([cluster]),
        <<"node">> => emqx_conf:get_raw([node])
    }.

changed(cluster) ->
    #{<<"name">> => <<"emqx-test">>};
changed(node) ->
    #{
        <<"name">> => <<"emqx-test@127.0.0.1">>,
        <<"cookie">> => <<"gokdfkdkf1122">>,
        <<"data_dir">> => <<"data">>
    };
changed(rpc) ->
    #{<<"mode">> => <<"sync">>}.
