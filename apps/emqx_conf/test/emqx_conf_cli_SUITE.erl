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
    ?assertEqual(Conf#{<<"sources">> := []}, emqx_conf:get_raw([Authz])),
    %% remove sources, it will reset to default file source.
    ConfBin1 = hocon_pp:do(#{<<"authorization">> => maps:remove(<<"sources">>, Conf)}, #{}),
    ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, Config),
    ok = emqx_conf_cli:conf(["load", ConfFile1]),
    Default = [emqx_authz_schema:default_authz()],
    ?assertEqual(Conf#{<<"sources">> := Default}, emqx_conf:get_raw([Authz])),
    %% reset
    ConfBin2 = hocon_pp:do(#{<<"authorization">> => Conf}, #{}),
    ConfFile2 = prepare_conf_file(?FUNCTION_NAME, ConfBin2, Config),
    ok = emqx_conf_cli:conf(["load", ConfFile2]),
    ?assertEqual(Conf, emqx_conf:get_raw([Authz])),
    ?assertEqual({error, empty_hocon_file}, emqx_conf_cli:conf(["load", "non-exist-file"])),
    ok.

t_load_readonly(Config) ->
    Base = #{<<"mqtt">> => emqx_conf:get_raw([mqtt])},
    lists:foreach(
        fun(Key) ->
            Conf = emqx_conf:get_raw([Key]),
            ConfBin0 = hocon_pp:do(Base#{Key => Conf}, #{}),
            ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
            ?assertEqual(
                {error, "update_readonly_keys_prohibited"},
                emqx_conf_cli:conf(["load", ConfFile0])
            )
        end,
        ?READONLY_KEYS
    ),
    ok.
