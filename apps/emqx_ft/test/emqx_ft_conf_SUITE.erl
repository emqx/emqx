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

-module(emqx_ft_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_ft]),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft, emqx_conf]),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_update_config(_Config) ->
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_conf:update(
            [file_transfer],
            #{<<"storage">> => #{<<"type">> => <<"unknown">>}},
            #{}
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer],
            #{<<"storage">> => #{<<"type">> => <<"local">>, <<"root">> => <<"/tmp/path">>}},
            #{}
        )
    ),
    ?assertEqual(
        <<"/tmp/path">>,
        emqx_config:get([file_transfer, storage, root])
    ).
