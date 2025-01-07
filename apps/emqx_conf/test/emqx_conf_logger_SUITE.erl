%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% erlfmt-ignore
-define(BASE_CONF,
    "
    log {
       console {
         enable = true
         level = debug
       }
       file {
         enable = true
         level = info
         path = \"log/emqx.log\"
       }
      throttling {
         msgs = []
         time_window = 1m
      }
    }
    ").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, ?BASE_CONF}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    LogConfRaw = emqx_conf:get_raw([<<"log">>]),
    [{log_conf_raw, LogConfRaw} | Config].

end_per_testcase(_TestCase, Config) ->
    LogConfRaw = ?config(log_conf_raw, Config),
    {ok, _} = emqx_conf:update([<<"log">>], LogConfRaw, #{}),
    ok.

t_log_conf(_Conf) ->
    FileExpect = #{
        <<"enable">> => true,
        <<"formatter">> => <<"text">>,
        <<"level">> => <<"info">>,
        <<"rotation_count">> => 10,
        <<"rotation_size">> => <<"50MB">>,
        <<"time_offset">> => <<"system">>,
        <<"path">> => <<"log/emqx.log">>,
        <<"timestamp_format">> => <<"auto">>,
        <<"payload_encode">> => <<"text">>
    },
    ExpectLog1 = #{
        <<"console">> =>
            #{
                <<"enable">> => true,
                <<"formatter">> => <<"text">>,
                <<"level">> => <<"debug">>,
                <<"payload_encode">> => <<"text">>,
                <<"time_offset">> => <<"system">>,
                <<"timestamp_format">> => <<"auto">>
            },
        <<"file">> =>
            #{<<"default">> => FileExpect},
        <<"throttling">> =>
            #{<<"time_window">> => <<"1m">>, <<"msgs">> => []}
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

t_file_logger_infinity_rotation(_Config) ->
    ConfPath = [<<"log">>],
    FileConfPath = [<<"file">>, <<"default">>],
    ConfRaw = emqx_conf:get_raw(ConfPath),
    FileConfRaw = emqx_utils_maps:deep_get(FileConfPath, ConfRaw),
    %% inconsistent config: infinity rotation size, but finite rotation count
    BadFileConfRaw = maps:merge(
        FileConfRaw,
        #{
            <<"rotation_size">> => <<"infinity">>,
            <<"rotation_count">> => 10
        }
    ),
    BadConfRaw = emqx_utils_maps:deep_put(FileConfPath, ConfRaw, BadFileConfRaw),
    ?assertMatch({ok, _}, emqx_conf:update(ConfPath, BadConfRaw, #{})),
    HandlerIds = logger:get_handler_ids(),
    %% ensure that the handler is correctly added
    ?assert(lists:member(default, HandlerIds), #{handler_ids => HandlerIds}),
    ok.
