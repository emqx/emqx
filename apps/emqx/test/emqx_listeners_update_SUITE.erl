%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_listeners_update_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_schema.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-import(emqx_listeners, [current_conns/2, is_running/1]).

-define(LISTENERS, [listeners]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Init = emqx:get_raw_config(?LISTENERS),
    [{init_conf, Init} | Config].

end_per_testcase(_TestCase, Config) ->
    Conf = ?config(init_conf, Config),
    {ok, _} = emqx:update_config(?LISTENERS, Conf),
    ok.

t_default_conf(_Config) ->
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:1883">>}},
            <<"ssl">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8883">>}},
            <<"ws">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8083">>}},
            <<"wss">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8084">>}}
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    ?assertMatch(
        #{
            tcp := #{default := #{bind := {{0, 0, 0, 0}, 1883}}},
            ssl := #{default := #{bind := {{0, 0, 0, 0}, 8883}}},
            ws := #{default := #{bind := {{0, 0, 0, 0}, 8083}}},
            wss := #{default := #{bind := {{0, 0, 0, 0}, 8084}}}
        },
        emqx:get_config(?LISTENERS)
    ),
    ok.

t_update_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"bind">>], Raw, <<"127.0.0.1:1883">>
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"bind">>], Raw1, <<"127.0.0.1:8883">>
    ),
    Raw3 = emqx_utils_maps:deep_put(
        [<<"ws">>, <<"default">>, <<"bind">>], Raw2, <<"0.0.0.0:8083">>
    ),
    Raw4 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"bind">>], Raw3, <<"127.0.0.1:8084">>
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw4)),
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:1883">>}},
            <<"ssl">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:8883">>}},
            <<"ws">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8083">>}},
            <<"wss">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:8084">>}}
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    BindTcp = {{127, 0, 0, 1}, 1883},
    BindSsl = {{127, 0, 0, 1}, 8883},
    BindWs = {{0, 0, 0, 0}, 8083},
    BindWss = {{127, 0, 0, 1}, 8084},
    ?assertMatch(
        #{
            tcp := #{default := #{bind := BindTcp}},
            ssl := #{default := #{bind := BindSsl}},
            ws := #{default := #{bind := BindWs}},
            wss := #{default := #{bind := BindWss}}
        },
        emqx:get_config(?LISTENERS)
    ),
    ?assertError(not_found, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),

    ?assertEqual(0, current_conns(<<"tcp:default">>, BindTcp)),
    ?assertEqual(0, current_conns(<<"ssl:default">>, BindSsl)),

    ?assertEqual({0, 0, 0, 0}, proplists:get_value(ip, ranch:info('ws:default'))),
    ?assertEqual({127, 0, 0, 1}, proplists:get_value(ip, ranch:info('wss:default'))),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),
    ok.

t_update_conf_validate_access_rules(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    RawCorrectConf1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["allow all"]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, RawCorrectConf1)),
    RawCorrectConf2 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["deny all"]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, RawCorrectConf2)),
    RawCorrectConf3 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["allow 10.0.1.0/24"]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, RawCorrectConf3)),
    RawIncorrectConf1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["xxx all"]
    ),
    ?assertMatch(
        {error, #{
            reason := <<"invalid_rule(s): xxx all">>,
            value := ["xxx all"],
            path := "listeners.tcp.default.access_rules",
            kind := validation_error,
            matched_type := "emqx:mqtt_tcp_listener"
        }},
        emqx:update_config(?LISTENERS, RawIncorrectConf1)
    ),
    RawIncorrectConf2 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["allow xxx"]
    ),
    ?assertMatch(
        {error, #{
            reason := <<"invalid_rule(s): allow xxx">>,
            value := ["allow xxx"],
            path := "listeners.tcp.default.access_rules",
            kind := validation_error,
            matched_type := "emqx:mqtt_tcp_listener"
        }},
        emqx:update_config(?LISTENERS, RawIncorrectConf2)
    ),
    ok.

t_update_conf_access_rules_split(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>],
        Raw,
        ["  allow all , deny all  , allow 10.0.1.0/24   "]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw1)),
    ?assertMatch(
        #{
            tcp := #{
                default := #{
                    access_rules := ["allow all", "deny all", "allow 10.0.1.0/24"]
                }
            }
        },
        emqx:get_config(?LISTENERS)
    ),
    ok.

t_update_tcp_keepalive_conf(_Conf) ->
    Keepalive = <<"240,30,5">>,
    KeepaliveStr = binary_to_list(Keepalive),
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"bind">>], Raw, <<"127.0.0.1:1883">>
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"tcp_options">>, <<"keepalive">>], Raw1, Keepalive
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw2)),
    ?assertMatch(
        #{
            <<"tcp">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:1883">>,
                    <<"tcp_options">> := #{<<"keepalive">> := Keepalive}
                }
            }
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    ?assertMatch(
        #{tcp := #{default := #{tcp_options := #{keepalive := KeepaliveStr}}}},
        emqx:get_config(?LISTENERS)
    ),
    Keepalive2 = <<" 241, 31, 6 ">>,
    KeepaliveStr2 = binary_to_list(Keepalive2),
    Raw3 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"tcp_options">>, <<"keepalive">>], Raw1, Keepalive2
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw3)),
    ?assertMatch(
        #{
            <<"tcp">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:1883">>,
                    <<"tcp_options">> := #{<<"keepalive">> := Keepalive2}
                }
            }
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    ?assertMatch(
        #{tcp := #{default := #{tcp_options := #{keepalive := KeepaliveStr2}}}},
        emqx:get_config(?LISTENERS)
    ),
    ok.

t_tcp_change_parse_unit(_Conf) ->
    test_change_parse_unit(?LISTENERS ++ [tcp, default], #{
        hosts => [{{127, 0, 0, 1}, 1883}]
    }).

t_ssl_change_parse_unit(_Conf) ->
    test_change_parse_unit(?LISTENERS ++ [ssl, default], #{
        hosts => [{{127, 0, 0, 1}, 8883}],
        ssl => true,
        ssl_opts => [{verify, verify_none}]
    }).

test_change_parse_unit(ConfPath, ClientOpts) ->
    ListenerRawConf0 = #{<<"parse_unit">> := <<"chunk">>} = emqx:get_raw_config(ConfPath),
    ListenerRawConf1 = ListenerRawConf0#{
        <<"parse_unit">> := <<"frame">>
    },
    %% Update listener and verify `parse_unit` came into effect:
    ?assertMatch({ok, _}, emqx:update_config(ConfPath, {update, ListenerRawConf1})),
    Client1 = emqtt_connect(ClientOpts),
    pong = emqtt:ping(Client1),
    CState1 = get_conn_state(Client1),
    emqx_listeners:is_packet_parser_available(mqtt) andalso
        ?assertMatch(
            #{parser := {frame, _Options}},
            CState1
        ),
    %% Restore original config and verify original `parse_unit` came into effect as well:
    ?assertMatch({ok, _}, emqx:update_config(ConfPath, {update, ListenerRawConf0})),
    Client2 = emqtt_connect(ClientOpts),
    pong = emqtt:ping(Client2),
    CState2 = get_conn_state(Client2),
    emqx_listeners:is_packet_parser_available(mqtt) andalso
        ?assertMatch(
            #{parser := Parser} when Parser =/= map_get(parser, CState1),
            CState2
        ),
    %% Existing connections should be preserved:
    pong = emqtt:ping(Client1),
    ok = emqtt:disconnect(Client1),
    pong = emqtt:ping(Client2),
    ok = emqtt:disconnect(Client2).

t_update_empty_ssl_options_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"bind">>], Raw, <<"127.0.0.1:1883">>
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"bind">>], Raw1, <<"127.0.0.1:8883">>
    ),
    Raw3 = emqx_utils_maps:deep_put(
        [<<"ws">>, <<"default">>, <<"bind">>], Raw2, <<"0.0.0.0:8083">>
    ),
    Raw4 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"bind">>], Raw3, <<"127.0.0.1:8084">>
    ),
    Raw5 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"cacertfile">>], Raw4, <<"">>
    ),
    Raw6 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"ssl_options">>, <<"cacertfile">>], Raw5, <<"">>
    ),
    Raw7 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"ssl_options">>, <<"ciphers">>], Raw6, <<"">>
    ),
    Ciphers = <<"TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256 ">>,
    Raw8 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"ciphers">>],
        Raw7,
        Ciphers
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw8)),
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:1883">>}},
            <<"ssl">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:8883">>,
                    <<"ssl_options">> := #{
                        <<"cacertfile">> := <<"">>,
                        <<"ciphers">> := Ciphers
                    }
                }
            },
            <<"ws">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8083">>}},
            <<"wss">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:8084">>,
                    <<"ssl_options">> := #{
                        <<"cacertfile">> := <<"">>,
                        <<"ciphers">> := <<"">>
                    }
                }
            }
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    BindTcp = {{127, 0, 0, 1}, 1883},
    BindSsl = {{127, 0, 0, 1}, 8883},
    BindWs = {{0, 0, 0, 0}, 8083},
    BindWss = {{127, 0, 0, 1}, 8084},
    ?assertMatch(
        #{
            tcp := #{default := #{bind := BindTcp}},
            ssl := #{
                default := #{
                    bind := BindSsl,
                    ssl_options := #{
                        cacertfile := <<"">>,
                        ciphers := ["TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"]
                    }
                }
            },
            ws := #{default := #{bind := BindWs}},
            wss := #{
                default := #{
                    bind := BindWss,
                    ssl_options := #{
                        cacertfile := <<"">>,
                        ciphers := []
                    }
                }
            }
        },
        emqx:get_config(?LISTENERS)
    ),
    ?assertError(not_found, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),

    ?assertEqual(0, current_conns(<<"tcp:default">>, BindTcp)),
    ?assertEqual(0, current_conns(<<"ssl:default">>, BindSsl)),

    ?assertEqual({0, 0, 0, 0}, proplists:get_value(ip, ranch:info('ws:default'))),
    ?assertEqual({127, 0, 0, 1}, proplists:get_value(ip, ranch:info('wss:default'))),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),

    Raw9 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"ciphers">>], Raw7, [
            "TLS_AES_256_GCM_SHA384",
            "TLS_AES_128_GCM_SHA256",
            "TLS_CHACHA20_POLY1305_SHA256"
        ]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw9)),

    BadRaw = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"keyfile">>], Raw4, <<"">>
    ),
    ?assertMatch(
        {error,
            {bad_ssl_config, #{
                reason := pem_file_path_or_string_is_required,
                which_option := <<"keyfile">>
            }}},
        emqx:update_config(?LISTENERS, BadRaw)
    ),
    ok.

t_add_delete_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    %% add
    #{<<"tcp">> := #{<<"default">> := Tcp}} = Raw,
    NewBind = <<"127.0.0.1:1987">>,
    Raw1 = emqx_utils_maps:deep_put([<<"tcp">>, <<"new">>], Raw, Tcp#{<<"bind">> => NewBind}),
    Raw2 = emqx_utils_maps:deep_put([<<"ssl">>, <<"default">>], Raw1, ?TOMBSTONE_VALUE),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw2)),
    ?assertEqual(0, current_conns(<<"tcp:new">>, {{127, 0, 0, 1}, 1987})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    %% deleted
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw)),
    ?assertError(not_found, current_conns(<<"tcp:new">>, {{127, 0, 0, 1}, 1987})),
    ?assertEqual(0, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    ok.

t_delete_default_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    %% delete default listeners
    Raw1 = emqx_utils_maps:deep_put([<<"tcp">>, <<"default">>], Raw, ?TOMBSTONE_VALUE),
    Raw2 = emqx_utils_maps:deep_put([<<"ssl">>, <<"default">>], Raw1, ?TOMBSTONE_VALUE),
    Raw3 = emqx_utils_maps:deep_put([<<"ws">>, <<"default">>], Raw2, ?TOMBSTONE_VALUE),
    Raw4 = emqx_utils_maps:deep_put([<<"wss">>, <<"default">>], Raw3, ?TOMBSTONE_VALUE),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw4)),
    ?assertError(not_found, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    ?assertMatch({error, not_found}, is_running('ws:default')),
    ?assertMatch({error, not_found}, is_running('wss:default')),

    %% reset
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw)),
    ?assertEqual(0, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertEqual(0, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),
    ok.

%%

emqtt_connect(Opts) ->
    case emqtt:start_link(Opts) of
        {ok, Client} ->
            true = erlang:unlink(Client),
            case emqtt:connect(Client) of
                {ok, _} -> Client;
                {error, Reason} -> error(Reason, [Opts])
            end;
        {error, Reason} ->
            error(Reason, [Opts])
    end.

get_conn_state(Client) ->
    ClientId = proplists:get_value(clientid, emqtt:info(Client)),
    [CPid | _] = emqx_cm:lookup_channels(ClientId),
    emqx_connection:get_state(CPid).
