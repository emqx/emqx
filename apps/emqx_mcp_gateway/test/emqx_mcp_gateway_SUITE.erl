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

-module(emqx_mcp_gateway_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SERVER_NAME, <<"test/calculator">>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%========================================================================
%% Init
%%========================================================================
init_per_suite(Config) ->
    DataDir = ?config(data_dir, Config),
    emqx_config:put(
        [mcp],
        #{
            enable => true,
            broker_suggested_server_name => #{
                enable => true,
                bootstrap_file => filename:join([DataDir, <<"server_names.csv">>])
            },
            servers => #{
                calc => #{
                    args => [
                        filename:join([DataDir, <<"calculator.py">>])
                    ],
                    env => #{},
                    command => <<"/tmp/venv-mcp/bin/python3">>,
                    enable => true,
                    server_name => ?SERVER_NAME,
                    server_type => stdio
                }
            }
        }
    ),
    Apps = emqx_cth_suite:start([emqx, emqx_ctl, emqx_mcp_gateway], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

%%========================================================================
%% Test cases
%%========================================================================
t_mcp_normal_msg_flow(_) ->
    true = emqx:is_running(node()),
    McpClientId = <<"myclient">>,
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {clientid, McpClientId},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C),

    %% Subscribe to the server presence topic
    {ok, _, [1]} = emqtt:subscribe(C, <<"$mcp-server/presence/#">>, qos1),
    ct:sleep(100),
    {ServerId, ServerName} =
        receive
            {publish, Msg} ->
                verify_server_presence_payload(maps:get(payload, Msg)),
                <<"$mcp-server/presence/", SidName/binary>> = maps:get(topic, Msg),
                split_id_and_server_name(SidName);
            Msg ->
                ct:fail({got_unexpected_message, Msg})
        after 100 ->
            ct:fail(no_message)
        end,
    ?assertEqual(?SERVER_NAME, ServerName),
    RpcTopic = <<"$mcp-rpc-endpoint/", McpClientId/binary, "/", ServerName/binary>>,

    %% Subscribe to the RPC topic
    % dbg:tracer(),
    % dbg:p(all, c),
    % dbg:tpl(emqtt, parse_subopt, 1, cx),
    {ok, _, [1]} = emqtt:subscribe(C, #{}, [{RpcTopic, [{qos, qos1}, {nl, true}]}]),
    %% Initialize with the server
    InitRequest = emqx_mcp_message:initialize_request(
        #{
            <<"name">> => <<"testClient">>,
            <<"version">> => <<"1.0.0">>
        },
        #{
            sampling => #{}, roots => #{listChanged => true}
        }
    ),
    InitTopic = <<"$mcp-server/", ServerId/binary, "/", ServerName/binary>>,
    emqtt:publish(C, InitTopic, InitRequest, qos1),
    receive
        {publish, Msg1} ->
            ?assertEqual(RpcTopic, maps:get(topic, Msg1)),
            verify_initialize_response(maps:get(payload, Msg1));
        Msg1 ->
            ct:fail({got_unexpected_message, Msg1})
    after 100 ->
        ct:fail(no_message)
    end,

    %% Send the initialized notification
    InitNotif = emqx_mcp_message:initialized_notification(),
    emqtt:publish(C, RpcTopic, InitNotif, qos1),

    %% List tools
    ListToolsRequest = emqx_mcp_message:json_rpc_request(2, <<"tools/list">>, #{}),
    emqtt:publish(C, RpcTopic, ListToolsRequest, qos1),
    receive
        {publish, Msg2} ->
            ?assertEqual(RpcTopic, maps:get(topic, Msg2)),
            ?assertMatch(
                {ok, #{
                    id := 2,
                    type := json_rpc_response,
                    result := #{
                        <<"tools">> := [
                            #{
                                <<"name">> := <<"add">>,
                                <<"description">> := _,
                                <<"inputSchema">> := #{}
                            }
                        ]
                    }
                }},
                emqx_mcp_message:decode_rpc_msg(maps:get(payload, Msg2))
            );
        Msg2 ->
            ct:fail({got_unexpected_message, Msg2})
    after 100 ->
        ct:fail(no_message)
    end,

    emqtt:disconnect(C).

%%========================================================================
%% Helper functions
%%========================================================================
split_id_and_server_name(Str) ->
    %% Split the server ID and name from the topic
    case string:split(Str, <<"/">>) of
        [Id, ServerName] -> {Id, ServerName};
        _ -> throw({error, {invalid_id_and_server_name, Str}})
    end.

verify_server_presence_payload(Payload) ->
    Msg = emqx_mcp_message:decode_rpc_msg(Payload),
    ?assertMatch(
        {ok, #{type := json_rpc_notification, method := <<"notifications/server/online">>}}, Msg
    ).

verify_initialize_response(Payload) ->
    Msg = emqx_mcp_message:decode_rpc_msg(Payload),
    ?assertMatch(
        {ok, #{
            type := json_rpc_response,
            result := #{
                <<"capabilities">> := #{
                    <<"experimental">> := #{},
                    <<"prompts">> := #{<<"listChanged">> := false},
                    <<"resources">> :=
                        #{
                            <<"listChanged">> := false,
                            <<"subscribe">> := false
                        },
                    <<"tools">> := #{<<"listChanged">> := false}
                },
                <<"protocolVersion">> := <<"2024-11-05">>,
                <<"serverInfo">> := #{
                    <<"name">> := <<"Calculator">>,
                    <<"version">> := <<"1.7.1">>
                }
            }
        }},
        Msg
    ).
