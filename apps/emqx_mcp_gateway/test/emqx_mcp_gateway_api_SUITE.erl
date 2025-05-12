%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mcp_gateway_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

-define(SERVER_NAME, <<"test/calculator">>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%========================================================================
%% Init
%%========================================================================
init_per_suite(Config) ->
    DataDir = filename:join([?config(data_dir, Config), "..", "SUITE_data"]),
    ct:pal("------ DataDir: ~p", [DataDir]),
    Apps1 = emqx_cth_suite:start([emqx, emqx_conf, emqx_ctl], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    emqx_config:put(
        [mcp],
        #{
            enable => true,
            broker_suggested_server_name => #{
                enable => true,
                bootstrap_file => filename:join([DataDir, <<"server_names.csv">>])
            },
            servers => #{}
        }
    ),
    application:ensure_all_started(emqx_mcp_gateway),
    ct:sleep(500),
    ct:pal("mcp configs0: ~p", [emqx_config:get([mcp])]),
    [{apps, [emqx_mcp_gateway | Apps1]} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

%%========================================================================
%% Test Cases
%%========================================================================
t_mcp_server_names(_) ->
    ?assertEqual({ok, <<"demo_mcp_server/a">>}, get_server_name(<<"a">>)),
    ?assertEqual({ok, <<"demo_mcp_server/b">>}, get_server_name(<<"b">>)),
    ?assertEqual({ok, <<"demo_mcp_server/c">>}, get_server_name(<<"c">>)),
    ?assertEqual({ok, <<"demo_mcp_server/d">>}, get_server_name(<<"d">>)),
    ?assertEqual({ok, <<"demo_mcp_server/e">>}, get_server_name(<<"e">>)),
    ?OK(#{data := Servers, meta := _}) = server_names_api(get, #{query_string => #{}}),
    ?assertEqual(5, length(Servers)),
    %% get server name "e"
    ?assertMatch(
        ?OK(#{
            data := [
                #{
                    server_name := <<"demo_mcp_server/e">>,
                    username := <<"e">>
                }
            ],
            meta := _
        }),
        server_names_api(get, #{query_string => #{<<"like_username">> => <<"e">>}})
    ),
    ?assertMatch(
        ?OK(#{
            server_name := <<"demo_mcp_server/e">>,
            username := <<"e">>
        }),
        single_server_name_api(get, #{bindings => #{username => <<"e">>}})
    ),
    %% delete server name "e"
    ?assertMatch(
        ?NO_CONTENT, single_server_name_api(delete, #{bindings => #{username => <<"e">>}})
    ),
    ?assertMatch({error, not_found}, get_server_name(<<"e">>)),
    ?assertMatch({404, _}, single_server_name_api(get, #{bindings => #{username => <<"e">>}})),
    ?OK(#{data := Servers1, meta := _}) = server_names_api(get, #{query_string => #{}}),
    ?assertEqual(4, length(Servers1)),
    %% add server name "e"
    ?assertMatch(
        ?CREATED(#{
            server_name := <<"demo_mcp_server/e">>,
            username := <<"e">>
        }),
        server_names_api(post, #{
            body => #{<<"server_name">> => <<"demo_mcp_server/e">>, <<"username">> => <<"e">>}
        })
    ),
    ?assertMatch(
        ?OK(#{
            server_name := <<"demo_mcp_server/e">>,
            username := <<"e">>
        }),
        single_server_name_api(get, #{bindings => #{username => <<"e">>}})
    ),
    %% update server name "e"
    ?assertMatch(
        ?OK(#{
            server_name := <<"updated/e">>,
            username := <<"e">>
        }),
        single_server_name_api(put, #{
            bindings => #{username => <<"e">>},
            body => #{<<"server_name">> => <<"updated/e">>, <<"username">> => <<"e">>}
        })
    ),
    ct:pal("mcp configs001: ~p", [emqx_config:get([mcp])]),
    ?assertMatch(
        ?OK(#{
            server_name := <<"updated/e">>,
            username := <<"e">>
        }),
        single_server_name_api(get, #{bindings => #{username => <<"e">>}})
    ).

t_mcp_servers(Config) ->
    ct:pal("mcp configs1: ~p", [emqx_config:get([mcp])]),
    DataDir = filename:join([?config(data_dir, Config), "..", "SUITE_data"]),
    ?assertMatch(?OK([]), mcp_servers_api(get, #{})),
    ?assertMatch(
        ?CREATED(_),
        mcp_servers_api(post, #{
            body => #{
                <<"id">> => <<"calc">>,
                <<"enable">> => true,
                <<"args">> => [
                    filename:join([DataDir, <<"calculator.py">>])
                ],
                <<"command">> => <<"/tmp/venv-mcp/bin/python3">>,
                <<"env">> => #{<<"VAR1">> => <<"1">>},
                <<"server_desc">> => <<>>,
                <<"server_name">> => ?SERVER_NAME,
                <<"server_type">> => <<"stdio">>
            }
        })
    ),
    ct:sleep(1000),
    ct:pal("mcp configs2: ~p", [emqx_config:get([mcp])]),
    #{listening_mcp_servers := Servers} = sys:get_state(emqx_mcp_server_dispatcher),
    ?assert(map_size(Servers) > 0),
    emqx_mcp_gateway_SUITE:t_mcp_normal_msg_flow([]).

%%==================================================================
%% Helper Functions
%%==================================================================
get_server_name(Name) ->
    emqx_mcp_server_name_manager:get_server_name(Name).

server_names_api(Method, Params) ->
    emqx_mcp_gateway_api:'/mcp/server_names'(Method, Params).

single_server_name_api(Method, Params) ->
    emqx_mcp_gateway_api:'/mcp/server_names/:username'(Method, Params).

mcp_servers_api(Method, Params) ->
    emqx_mcp_gateway_api:'/mcp/servers'(Method, Params).

single_mcp_server_api(Method, Params) ->
    emqx_mcp_gateway_api:'/mcp/servers/:id'(Method, Params).
