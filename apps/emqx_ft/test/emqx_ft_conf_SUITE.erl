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
-include_lib("snabbkaffe/include/test_macros.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = emqx_config:save_schema_mod_and_names(emqx_ft_schema),
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_ft], emqx_ft_test_helpers:env_handler(Config)
    ),
    {ok, _} = emqx:update_config([rpc, port_discovery], manual),
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
            #{
                <<"storage">> => #{
                    <<"type">> => <<"local">>,
                    <<"segments">> => #{
                        <<"root">> => <<"/tmp/path">>,
                        <<"gc">> => #{
                            <<"interval">> => <<"5m">>
                        }
                    },
                    <<"exporter">> => #{
                        <<"type">> => <<"local">>,
                        <<"root">> => <<"/tmp/exports">>
                    }
                }
            },
            #{}
        )
    ),
    ?assertEqual(
        <<"/tmp/path">>,
        emqx_config:get([file_transfer, storage, segments, root])
    ),
    ?assertEqual(
        5 * 60 * 1000,
        emqx_ft_conf:gc_interval(emqx_ft_conf:storage())
    ).

t_remove_restore_config(Config) ->
    ?assertMatch(
        {ok, _},
        emqx_conf:update([file_transfer, storage], #{<<"type">> => <<"local">>}, #{})
    ),
    ?assertEqual(
        60 * 60 * 1000,
        emqx_ft_conf:gc_interval(emqx_ft_conf:storage())
    ),
    % Verify that transfers work
    ok = emqx_ft_test_helpers:upload_file(gen_clientid(), <<"f1">>, "f1", <<?MODULE_STRING>>),
    ?assertMatch(
        {ok, _},
        emqx_conf:remove([file_transfer, storage], #{})
    ),
    ?assertEqual(
        undefined,
        emqx_ft_conf:storage()
    ),
    ?assertEqual(
        undefined,
        emqx_ft_conf:gc_interval(emqx_ft_conf:storage())
    ),
    ClientId = gen_clientid(),
    Client = emqx_ft_test_helpers:start_client(ClientId),
    % Verify that transfers fail cleanly when storage is disabled
    ?check_trace(
        ?assertMatch(
            {ok, #{reason_code_name := unspecified_error}},
            emqtt:publish(
                Client,
                <<"$file/f2/init">>,
                emqx_utils_json:encode(emqx_ft:encode_filemeta(#{name => "f2", size => 42})),
                1
            )
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{transfer := {ClientId, <<"f2">>}, reason := {error, disabled}}],
                ?of_kind("store_filemeta_failed", Trace)
            )
        end
    ),
    % Restore local storage backend
    Root = iolist_to_binary(emqx_ft_test_helpers:root(Config, node(), [segments])),
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer, storage],
            #{
                <<"type">> => <<"local">>,
                <<"segments">> => #{
                    <<"root">> => Root,
                    <<"gc">> => #{<<"interval">> => <<"1s">>}
                }
            },
            #{}
        )
    ),
    % Verify that GC is getting triggered eventually
    ?check_trace(
        ?block_until(#{?snk_kind := garbage_collection}, 5000, 0),
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        ?snk_kind := garbage_collection,
                        storage := #{
                            type := local,
                            segments := #{root := Root}
                        }
                    }
                ],
                ?of_kind(garbage_collection, Trace)
            )
        end
    ),
    % Verify that transfers work again
    ok = emqx_ft_test_helpers:upload_file(gen_clientid(), <<"f1">>, "f1", <<?MODULE_STRING>>).

gen_clientid() ->
    emqx_base62:encode(emqx_guid:gen()).
