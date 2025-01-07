%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_compatible_config_tests).

-include_lib("eunit/include/eunit.hrl").

empty_config_test() ->
    Conf1 = #{<<"bridges">> => #{}},
    Conf2 = #{<<"bridges">> => #{<<"webhook">> => #{}}},
    ?assertEqual(Conf1, check(Conf1)),
    ?assertEqual(#{<<"bridges">> => #{<<"webhook">> => #{}}}, check(Conf2)),
    ok.

%% ensure webhook config can be checked
webhook_config_test() ->
    Conf1 = parse(webhook_v5011_hocon()),
    Conf2 = parse(full_webhook_v5011_hocon()),
    Conf3 = parse(full_webhook_v5019_hocon()),

    ?assertMatch(
        #{
            <<"bridges">> := #{
                <<"webhook">> := #{
                    <<"the_name">> :=
                        #{
                            <<"method">> := get,
                            <<"body">> := <<"${payload}">>
                        }
                }
            }
        },
        check(Conf1)
    ),

    ?assertMatch(
        #{
            <<"bridges">> := #{
                <<"webhook">> := #{
                    <<"the_name">> :=
                        #{
                            <<"method">> := get,
                            <<"body">> := <<"${payload}">>
                        }
                }
            }
        },
        check(Conf2)
    ),
    #{
        <<"bridges">> := #{
            <<"webhook">> := #{
                <<"the_name">> :=
                    #{
                        <<"method">> := get,
                        <<"resource_opts">> := ResourceOpts,
                        <<"body">> := <<"${payload}">>
                    }
            }
        }
    } = check(Conf3),
    ?assertMatch(#{<<"request_ttl">> := infinity}, ResourceOpts),
    ok.

up(#{<<"bridges">> := Bridges0} = Conf0) ->
    Bridges = up(Bridges0),
    Conf0#{<<"bridges">> := Bridges};
up(#{<<"mqtt">> := MqttBridges0} = Bridges) ->
    MqttBridges = emqx_bridge_compatible_config:upgrade_pre_ee(
        MqttBridges0, fun emqx_bridge_compatible_config:maybe_upgrade/1
    ),
    Bridges#{<<"mqtt">> := MqttBridges};
up(#{<<"webhook">> := WebhookBridges0} = Bridges) ->
    WebhookBridges = emqx_bridge_compatible_config:upgrade_pre_ee(
        WebhookBridges0, fun emqx_bridge_compatible_config:http_maybe_upgrade/1
    ),
    Bridges#{<<"webhook">> := WebhookBridges}.

parse(HOCON) ->
    {ok, Conf} = hocon:binary(HOCON),
    Conf.

mqtt_config_test_() ->
    Conf0 = mqtt_v5011_hocon(),
    Conf1 = mqtt_v5011_full_hocon(),
    [
        {Tag, fun() ->
            Parsed = parse(Conf),
            Upgraded = up(Parsed),
            Checked = check(Upgraded),
            assert_upgraded(Checked)
        end}
     || {Tag, Conf} <- [{"minimum", Conf0}, {"full", Conf1}]
    ].

assert_upgraded(#{<<"bridges">> := Bridges}) ->
    assert_upgraded(Bridges);
assert_upgraded(#{<<"mqtt">> := Mqtt}) ->
    assert_upgraded(Mqtt);
assert_upgraded(#{<<"bridge_one">> := Map}) ->
    assert_upgraded1(Map);
assert_upgraded(#{<<"bridge_two">> := Map}) ->
    assert_upgraded1(Map).

assert_upgraded1(Map) ->
    ?assertNot(maps:is_key(<<"connector">>, Map)),
    ?assertNot(maps:is_key(<<"direction">>, Map)),
    ?assert(maps:is_key(<<"server">>, Map)),
    ?assert(maps:is_key(<<"ssl">>, Map)).

check(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf, #{required => false}).

%% erlfmt-ignore
%% this is config generated from v5.0.11
webhook_v5011_hocon() ->
"
bridges{
  webhook {
    the_name{
      body = \"${payload}\"
      connect_timeout = \"5s\"
      enable_pipelining = 100
      headers {\"content-type\" = \"application/json\"}
      max_retries = 3
      method = \"get\"
      pool_size = 4
      request_timeout = \"15s\"
      ssl {enable = false, verify = \"verify_peer\"}
      url = \"http://localhost:8080\"
    }
  }
}
".

full_webhook_v5011_hocon() ->
    ""
    "\n"
    "bridges{\n"
    "  webhook {\n"
    "    the_name{\n"
    "      body = \"${payload}\"\n"
    "      connect_timeout = \"5s\"\n"
    "      direction = \"egress\"\n"
    "      enable_pipelining = 100\n"
    "      headers {\"content-type\" = \"application/json\"}\n"
    "      max_retries = 3\n"
    "      method = \"get\"\n"
    "      pool_size = 4\n"
    "      pool_type = \"random\"\n"
    "      request_timeout = \"5s\"\n"
    "      ssl {\n"
    "        ciphers = \"\"\n"
    "        depth = 10\n"
    "        enable = false\n"
    "        reuse_sessions = true\n"
    "        secure_renegotiate = true\n"
    "        user_lookup_fun = \"emqx_tls_psk:lookup\"\n"
    "        verify = \"verify_peer\"\n"
    "        versions = [\"tlsv1.3\", \"tlsv1.2\", \"tlsv1.1\", \"tlsv1\"]\n"
    "      }\n"
    "      url = \"http://localhost:8080\"\n"
    "    }\n"
    "  }\n"
    "}\n"
    "".

%% does not contain direction
full_webhook_v5019_hocon() ->
    ""
    "\n"
    "bridges{\n"
    "  webhook {\n"
    "    the_name{\n"
    "      body = \"${payload}\"\n"
    "      connect_timeout = \"5s\"\n"
    "      enable_pipelining = 100\n"
    "      headers {\"content-type\" = \"application/json\"}\n"
    "      max_retries = 3\n"
    "      method = \"get\"\n"
    "      pool_size = 4\n"
    "      pool_type = \"random\"\n"
    "      request_timeout = \"1m\"\n"
    "      resource_opts = {\n"
    "        request_timeout = \"infinity\"\n"
    "      }\n"
    "      ssl {\n"
    "        ciphers = \"\"\n"
    "        depth = 10\n"
    "        enable = false\n"
    "        reuse_sessions = true\n"
    "        secure_renegotiate = true\n"
    "        user_lookup_fun = \"emqx_tls_psk:lookup\"\n"
    "        verify = \"verify_peer\"\n"
    "        versions = [\"tlsv1.3\", \"tlsv1.2\", \"tlsv1.1\", \"tlsv1\"]\n"
    "      }\n"
    "      url = \"http://localhost:8080\"\n"
    "    }\n"
    "  }\n"
    "}\n"
    "".

%% erlfmt-ignore
%% this is a generated from v5.0.11
mqtt_v5011_hocon() ->
"
bridges {
  mqtt {
    bridge_one {
      connector {
        bridge_mode = false
        clean_start = true
        keepalive = \"60s\"
        mode = cluster_shareload
        proto_ver = \"v4\"
        reconnect_interval = \"15s\"
        server = \"localhost:1883\"
        ssl {enable = false, verify = \"verify_peer\"}
      }
      direction = egress
      enable = true
      payload = \"${payload}\"
      remote_qos = 1
      remote_topic = \"tttttttttt\"
      retain = false
    }
    bridge_two {
      connector {
        bridge_mode = false
        clean_start = true
        keepalive = \"60s\"
        mode = \"cluster_shareload\"
        proto_ver = \"v4\"
        reconnect_interval = \"15s\"
        server = \"localhost:1883\"
        ssl {enable = false, verify = \"verify_peer\"}
      }
      direction = ingress
      enable = true
      local_qos = 1
      payload = \"${payload}\"
      remote_qos = 1
      remote_topic = \"tttttttt/#\"
      retain = false
    }
  }
}
".

%% erlfmt-ignore
%% a more complete version
mqtt_v5011_full_hocon() ->
"
bridges {
  mqtt {
    bridge_one {
      connector {
        bridge_mode = false
        clean_start = true
        keepalive = \"60s\"
        max_inflight = 32
        mode = \"cluster_shareload\"
        password = \"\"
        proto_ver = \"v5\"
        replayq {offload = false, seg_bytes = \"100MB\"}
        retry_interval = \"12s\"
        server = \"localhost:1883\"
        ssl {
          ciphers = \"\"
          depth = 10
          enable = false
          reuse_sessions = true
          secure_renegotiate = true
          user_lookup_fun = \"emqx_tls_psk:lookup\"
          verify = \"verify_peer\"
          versions = [\"tlsv1.3\", \"tlsv1.2\", \"tlsv1.1\", \"tlsv1\"]
        }
        username = \"\"
      }
      direction = \"ingress\"
      enable = true
      local_qos = 1
      payload = \"${payload}\"
      remote_qos = 1
      remote_topic = \"tttt/a\"
      retain = false
    }
    bridge_two {
      connector {
        bridge_mode = false
        clean_start = true
        keepalive = \"60s\"
        max_inflight = 32
        mode = \"cluster_shareload\"
        password = \"\"
        proto_ver = \"v4\"
        replayq {offload = false, seg_bytes = \"100MB\"}
        retry_interval = \"44s\"
        server = \"localhost:1883\"
        ssl {
          ciphers = \"\"
          depth = 10
          enable = false
          reuse_sessions = true
          secure_renegotiate = true
          user_lookup_fun = \"emqx_tls_psk:lookup\"
          verify = verify_peer
          versions = [\"tlsv1.3\", \"tlsv1.2\", \"tlsv1.1\", \"tlsv1\"]
        }
        username = \"\"
      }
      direction = egress
      enable = true
      payload = \"${payload.x}\"
      remote_qos = 1
      remote_topic = \"remotetopic/1\"
      retain = false
    }
  }
}
".
