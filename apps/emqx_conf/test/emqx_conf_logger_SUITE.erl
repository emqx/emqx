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
-module(emqx_conf_logger_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% erlfmt-ignore
-define(BASE_CONF,
    """
             node {
                name = \"emqx1@127.0.0.1\"
                cookie = \"emqxsecretcookie\"
                data_dir = \"data\"
             }
             cluster {
                name = emqxcl
                discovery_strategy = static
                static.seeds = \"emqx1@127.0.0.1\"
                core_nodes = \"emqx1@127.0.0.1\"
             }
             log {
                console {
                enable = true
                level = debug
                }
                file {
                enable = true
                level = info
                to = \"log/emqx.log\"
                }
             }
    """).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:load_config(emqx_conf_schema, iolist_to_binary(?BASE_CONF)),
    emqx_mgmt_api_test_util:init_suite([emqx_conf]),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite([emqx_conf]).

t_log_conf(_Conf) ->
    FileExpect = #{
        <<"enable">> => true,
        <<"formatter">> => <<"text">>,
        <<"level">> => <<"info">>,
        <<"rotation_count">> => 10,
        <<"rotation_size">> => <<"50MB">>,
        <<"time_offset">> => <<"system">>,
        <<"to">> => <<"log/emqx.log">>
    },
    ExpectLog1 = #{
        <<"console">> =>
            #{
                <<"enable">> => true,
                <<"formatter">> => <<"text">>,
                <<"level">> => <<"debug">>,
                <<"time_offset">> => <<"system">>
            },
        <<"file">> =>
            #{<<"default">> => FileExpect}
    },
    ?assertEqual(ExpectLog1, emqx_conf:get_raw([<<"log">>])),
    UpdateLog0 = emqx_utils_maps:deep_remove([<<"file">>, <<"default">>], ExpectLog1),
    UpdateLog1 = emqx_utils_maps:deep_merge(
        UpdateLog0,
        #{
            <<"console">> => #{<<"level">> => <<"info">>},
            <<"file">> => FileExpect#{<<"level">> => <<"error">>}
        }
    ),
    ?assertMatch({ok, _}, emqx_conf:update([<<"log">>], UpdateLog1, #{})),
    ?assertMatch(
        {ok, #{config := #{file := "log/emqx.log"}, level := error}},
        logger:get_handler_config(default)
    ),
    ?assertMatch(
        {ok, #{config := #{type := standard_io}, level := info, module := logger_std_h}},
        logger:get_handler_config(console)
    ),
    UpdateLog2 = emqx_utils_maps:deep_merge(
        UpdateLog1,
        #{
            <<"console">> => #{<<"enable">> => false},
            <<"file">> => FileExpect#{<<"enable">> => false}
        }
    ),
    ?assertMatch({ok, _}, emqx_conf:update([<<"log">>], UpdateLog2, #{})),
    ?assertMatch({error, {not_found, default}}, logger:get_handler_config(default)),
    ?assertMatch({error, {not_found, console}}, logger:get_handler_config(console)),
    ok.
