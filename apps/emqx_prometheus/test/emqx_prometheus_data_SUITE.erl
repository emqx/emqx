%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_prometheus_data_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_prometheus.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% erlfmt-ignore
-define(EMQX_CONF, <<"
authentication = [
  {
    backend = built_in_database
    enable = true
    mechanism = password_based
    password_hash_algorithm {name = sha256, salt_position = suffix}
    user_id_type = username
  },
  {
    algorithm = sha256
    backend = built_in_database
    enable = true
    iteration_count = 4096
    mechanism = scram
  }
]
authorization {
  cache {
    enable = true
  }
  deny_action = ignore
  no_match = allow
  sources = [
    {path = \"${EMQX_ETC_DIR}/acl.conf\", type = file}
  ]
}
connectors {
  http {
    test_http_connector {
      ssl {enable = false, verify = verify_peer}
      url = \"http://127.0.0.1:3000\"
    }
  }
}
rule_engine {
  ignore_sys_message = true
  jq_function_default_timeout = 10s
  rules {
    rule_xbmw {
      actions = [\"mqtt:action1\"]
      description = \"\"
      enable = true
      metadata {created_at = 1707244896918}
      sql = \"SELECT * FROM \\\"t/#\\\"\"
    }
  }
}
">>).

all() ->
    lists:flatten([
        {group, '/prometheus/stats'},
        {group, '/prometheus/auth'},
        {group, '/prometheus/data_integration'},
        [{group, '/prometheus/schema_validation'} || emqx_release:edition() == ee],
        [{group, '/prometheus/message_transformation'} || emqx_release:edition() == ee]
    ]).

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    AcceptGroups = [
        {group, 'text/plain'},
        {group, 'application/json'}
    ],
    ModeGroups = [
        {group, ?PROM_DATA_MODE__NODE},
        {group, ?PROM_DATA_MODE__ALL_NODES_AGGREGATED},
        {group, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED}
    ],
    [
        {'/prometheus/stats', ModeGroups},
        {'/prometheus/auth', ModeGroups},
        {'/prometheus/data_integration', ModeGroups},
        {'/prometheus/schema_validation', ModeGroups},
        {'/prometheus/message_transformation', ModeGroups},
        {?PROM_DATA_MODE__NODE, AcceptGroups},
        {?PROM_DATA_MODE__ALL_NODES_AGGREGATED, AcceptGroups},
        {?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, AcceptGroups},
        {'text/plain', TCs},
        {'application/json', TCs}
    ].

init_per_suite(Config) ->
    meck:new(emqx_retainer, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_retainer, retained_count, fun() -> 0 end),
    meck:expect(
        emqx_authz_file,
        acl_conf_file,
        fun() ->
            emqx_common_test_helpers:deps_path(emqx_auth, "etc/acl.conf")
        end
    ),
    ok = emqx_prometheus_SUITE:maybe_meck_license(),
    emqx_prometheus_SUITE:start_mock_pushgateway(9091),

    Apps = emqx_cth_suite:start(
        lists:flatten([
            emqx,
            {emqx_conf, ?EMQX_CONF},
            emqx_auth,
            emqx_auth_mnesia,
            emqx_rule_engine,
            emqx_bridge_http,
            emqx_connector,
            [
                {emqx_schema_validation, #{config => schema_validation_config()}}
             || emqx_release:edition() == ee
            ],
            [
                {emqx_message_transformation, #{config => message_transformation_config()}}
             || emqx_release:edition() == ee
            ],
            {emqx_prometheus, emqx_prometheus_SUITE:legacy_conf_default()},
            emqx_management
        ]),
        #{
            work_dir => filename:join(?config(priv_dir, Config), ?MODULE)
        }
    ),

    [{apps, Apps} | Config].

end_per_suite(Config) ->
    meck:unload([emqx_retainer]),
    emqx_prometheus_SUITE:maybe_unmeck_license(),
    emqx_prometheus_SUITE:stop_mock_pushgateway(),
    emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_group('/prometheus/stats', Config) ->
    [{module, emqx_prometheus} | Config];
init_per_group('/prometheus/auth', Config) ->
    [{module, emqx_prometheus_auth} | Config];
init_per_group('/prometheus/data_integration', Config) ->
    [{module, emqx_prometheus_data_integration} | Config];
init_per_group('/prometheus/schema_validation', Config) ->
    [{module, emqx_prometheus_schema_validation} | Config];
init_per_group('/prometheus/message_transformation', Config) ->
    [{module, emqx_prometheus_message_transformation} | Config];
init_per_group(?PROM_DATA_MODE__NODE, Config) ->
    [{mode, ?PROM_DATA_MODE__NODE} | Config];
init_per_group(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Config) ->
    [{mode, ?PROM_DATA_MODE__ALL_NODES_AGGREGATED} | Config];
init_per_group(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Config) ->
    [{mode, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED} | Config];
init_per_group('text/plain', Config) ->
    [{accept, 'text/plain'} | Config];
init_per_group('application/json', Config) ->
    [{accept, 'application/json'} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(t_collect_prom_data, Config) ->
    meck:new(emqx_utils, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_utils, gen_id, fun() -> "fake" end),

    meck:new(emqx, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx,
        data_dir,
        fun() ->
            {data_dir, Data} = lists:keyfind(data_dir, 1, Config),
            Data
        end
    ),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_collect_prom_data, _Config) ->
    meck:unload(emqx_utils),
    meck:unload(emqx),
    ok;
end_per_testcase(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_collect_prom_data(Config) ->
    CollectOpts = collect_opts(Config),
    Module = ?config(module, Config),
    Response = emqx_prometheus_api:collect(Module, CollectOpts),
    assert_data(Module, Response, CollectOpts).

%%--------------------------------------------------------------------
%% Helper fns
%%--------------------------------------------------------------------

assert_data(_Module, {Code, Header, RawDataBinary}, #{type := <<"prometheus">>, mode := Mode}) ->
    ?assertEqual(Code, 200),
    ?assertMatch(#{<<"content-type">> := <<"text/plain">>}, Header),
    DataL = lists:filter(
        fun(B) ->
            case re:run(B, <<"^[^#]">>, [global]) of
                {match, _} ->
                    true;
                nomatch ->
                    false
            end
        end,
        binary:split(RawDataBinary, [<<"\n">>], [global])
    ),
    assert_prom_data(DataL, Mode);
assert_data(Module, {Code, JsonData}, #{type := <<"json">>, mode := Mode}) ->
    ?assertEqual(Code, 200),
    ?assertMatch(#{}, JsonData),
    assert_json_data(Module, JsonData, Mode).

%%%%%%%%%%%%%%%%%%%%
%% assert text/plain format
assert_prom_data(DataL, Mode) ->
    NDataL = lists:map(
        fun(Line) ->
            binary:split(Line, [<<"{">>, <<",">>, <<"} ">>, <<" ">>], [global])
        end,
        DataL
    ),
    do_assert_prom_data(NDataL, Mode).

-define(MGU(K, MAP), maps:get(K, MAP, undefined)).

assert_json_data(_, Data, Mode) ->
    lists:foreach(
        fun(FunSeed) ->
            erlang:apply(?MODULE, fun_name(FunSeed), [?MGU(FunSeed, Data), Mode]),
            ok
        end,
        maps:keys(Data)
    ),
    ok.

fun_name(Seed) ->
    binary_to_atom(<<"assert_json_data__", (atom_to_binary(Seed))/binary>>).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

collect_opts(Config) ->
    #{
        type => accept(?config(accept, Config)),
        mode => ?config(mode, Config)
    }.

accept('text/plain') ->
    <<"prometheus">>;
accept('application/json') ->
    <<"json">>.

do_assert_prom_data([], _Mode) ->
    ok;
do_assert_prom_data([Metric | RestDataL], Mode) ->
    [_MetricName | _] = Metric,
    assert_stats_metric_labels(Metric, Mode),
    do_assert_prom_data(RestDataL, Mode).

assert_stats_metric_labels([MetricName | R] = _Metric, Mode) ->
    case maps:get(Mode, metric_meta(MetricName), undefined) of
        %% for uncatched metrics (by prometheus.erl)
        undefined ->
            ok;
        N when is_integer(N) ->
            case N =:= length(lists:droplast(R)) of
                true ->
                    ok;
                false ->
                    ct:print(
                        "====================~n"
                        "%% Metric: ~p~n"
                        "%% Expect labels count: ~p in Mode: ~p~n"
                        "%% But got labels: ~p~n",
                        [_Metric, N, Mode, length(lists:droplast(R))]
                    )
            end,
            ?assertEqual(N, length(lists:droplast(R)))
    end.

-define(meta(NODE, AGGRE, UNAGGRE), #{
    ?PROM_DATA_MODE__NODE => NODE,
    ?PROM_DATA_MODE__ALL_NODES_AGGREGATED => AGGRE,
    ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED => UNAGGRE
}).

%% `/prometheus/stats`
%% BEGIN always no label
metric_meta(<<"emqx_cluster_sessions_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_cluster_sessions_max">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_topics_max">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_topics_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_retained_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_retained_max">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_subscriptions_shared_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_subscriptions_shared_max">>) -> ?meta(0, 0, 0);
%% END
%% BEGIN no label in mode `node`
metric_meta(<<"emqx_vm_cpu_use">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_cpu_idle">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_run_queue">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_process_messages_in_queues">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_total_memory">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_used_memory">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_mnesia_tm_mailbox_size">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_broker_pool_max_mailbox_size">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_cluster_nodes_running">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_cluster_nodes_stopped">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_conf_sync_txid">>) -> ?meta(0, 1, 1);
%% END
metric_meta(<<"emqx_cert_expiry_at">>) -> ?meta(2, 2, 2);
metric_meta(<<"emqx_license_expiry_at">>) -> ?meta(0, 0, 0);
%% mria metric with label `shard` and `node` when not in mode `node`
metric_meta(<<"emqx_mria_", _Tail/binary>>) -> ?meta(1, 2, 2);
%% `/prometheus/auth`
metric_meta(<<"emqx_authn_users_count">>) -> ?meta(1, 1, 1);
metric_meta(<<"emqx_authn_", _Tail/binary>>) -> ?meta(1, 1, 2);
metric_meta(<<"emqx_authz_rules_count">>) -> ?meta(1, 1, 1);
metric_meta(<<"emqx_authz_", _Tail/binary>>) -> ?meta(1, 1, 2);
metric_meta(<<"emqx_banned_count">>) -> ?meta(0, 0, 0);
%% `/prometheus/data_integration`
metric_meta(<<"emqx_rules_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_actions_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_connectors_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_schema_registrys_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_rule_", _Tail/binary>>) -> ?meta(1, 1, 2);
metric_meta(<<"emqx_action_", _Tail/binary>>) -> ?meta(1, 1, 2);
metric_meta(<<"emqx_connector_", _Tail/binary>>) -> ?meta(1, 1, 2);
%% `/prometheus/schema_validation`
metric_meta(<<"emqx_schema_validation_", _Tail/binary>>) -> ?meta(1, 1, 2);
%% `/prometheus/message_transformation`
metric_meta(<<"emqx_message_transformation_", _Tail/binary>>) -> ?meta(1, 1, 2);
%% normal emqx metrics
metric_meta(<<"emqx_", _Tail/binary>>) -> ?meta(0, 0, 1);
metric_meta(_) -> #{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Assert Json Data Structure

assert_json_data__messages(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_messages_received := _,
            emqx_messages_sent := _,
            emqx_messages_qos0_received := _,
            emqx_messages_qos0_sent := _,
            emqx_messages_qos1_received := _,
            emqx_messages_qos1_sent := _,
            emqx_messages_qos2_received := _,
            emqx_messages_qos2_sent := _,
            emqx_messages_publish := _,
            emqx_messages_dropped := _,
            emqx_messages_dropped_expired := _,
            emqx_messages_dropped_no_subscribers := _,
            emqx_messages_forward := _,
            emqx_messages_retained := _,
            emqx_messages_delayed := _,
            emqx_messages_delivered := _,
            emqx_messages_acked := _
        },
        M
    ),
    ok;
assert_json_data__messages(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__stats(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_connections_count := _,
            emqx_connections_max := _,
            emqx_durable_subscriptions_count := _,
            emqx_durable_subscriptions_max := _,
            emqx_live_connections_count := _,
            emqx_live_connections_max := _,
            emqx_sessions_count := _,
            emqx_sessions_max := _,
            emqx_channels_count := _,
            emqx_channels_max := _,
            emqx_topics_count := _,
            emqx_topics_max := _,
            emqx_suboptions_count := _,
            emqx_suboptions_max := _,
            emqx_subscribers_count := _,
            emqx_subscribers_max := _,
            emqx_subscriptions_count := _,
            emqx_subscriptions_max := _,
            emqx_subscriptions_shared_count := _,
            emqx_subscriptions_shared_max := _,
            emqx_retained_count := _,
            emqx_retained_max := _,
            emqx_delayed_count := _,
            emqx_delayed_max := _
        },
        M
    );
assert_json_data__stats(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__olp(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(#{}, M);
assert_json_data__olp(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    ok.

assert_json_data__client(Ms, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED) andalso
        is_list(Ms)
->
    ?assertMatch(
        [
            #{
                emqx_client_connect := _,
                emqx_client_connack := _,
                emqx_client_connected := _,
                emqx_client_authenticate := _,
                emqx_client_auth_anonymous := _,
                emqx_client_authorize := _,
                emqx_client_subscribe := _,
                emqx_client_unsubscribe := _,
                emqx_client_disconnected := _
            }
        ],
        Ms
    );
assert_json_data__client(#{} = M, ?PROM_DATA_MODE__NODE) ->
    ?assertMatch(
        #{
            emqx_client_connect := _,
            emqx_client_connack := _,
            emqx_client_connected := _,
            emqx_client_authenticate := _,
            emqx_client_auth_anonymous := _,
            emqx_client_authorize := _,
            emqx_client_subscribe := _,
            emqx_client_unsubscribe := _,
            emqx_client_disconnected := _
        },
        M
    );
assert_json_data__client(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__session(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_session_created := _,
            emqx_session_resumed := _,
            emqx_session_takenover := _,
            emqx_session_discarded := _,
            emqx_session_terminated := _
        },
        M
    );
assert_json_data__session(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__metrics(M, ?PROM_DATA_MODE__NODE) ->
    ?assertMatch(
        #{
            emqx_vm_cpu_use := _,
            emqx_vm_cpu_idle := _,
            emqx_vm_run_queue := _,
            emqx_vm_process_messages_in_queues := _,
            emqx_vm_total_memory := _,
            emqx_vm_used_memory := _
        },
        M
    );
assert_json_data__metrics(Ms, Mode) when
    is_list(Ms) andalso
        (Mode =:= ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED orelse
            Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__delivery(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_delivery_dropped := _,
            emqx_delivery_dropped_no_local := _,
            emqx_delivery_dropped_too_large := _,
            emqx_delivery_dropped_qos0_msg := _,
            emqx_delivery_dropped_queue_full := _,
            emqx_delivery_dropped_expired := _
        },
        M
    );
assert_json_data__delivery(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__cluster(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{emqx_cluster_nodes_running := _, emqx_cluster_nodes_stopped := _},
        M
    );
assert_json_data__cluster(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__acl(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_authorization_allow := _,
            emqx_authorization_deny := _,
            emqx_authorization_cache_hit := _,
            emqx_authorization_cache_miss := _,
            emqx_authorization_superuser := _,
            emqx_authorization_nomatch := _,
            emqx_authorization_matched_allow := _,
            emqx_authorization_matched_deny := _
        },
        M
    );
assert_json_data__acl(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__authn(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_authentication_success := _,
            emqx_authentication_success_anonymous := _,
            emqx_authentication_failure := _
        },
        M
    );
assert_json_data__authn(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data__packets(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_packets_publish_auth_error := _,
            emqx_packets_puback_received := _,
            emqx_packets_pubcomp_inuse := _,
            emqx_packets_pubcomp_sent := _,
            emqx_packets_suback_sent := _,
            emqx_packets_pubrel_missed := _,
            emqx_packets_publish_inuse := _,
            emqx_packets_pingresp_sent := _,
            emqx_packets_subscribe_received := _,
            emqx_bytes_received := _,
            emqx_packets_publish_dropped := _,
            emqx_packets_publish_received := _,
            emqx_packets_connack_sent := _,
            emqx_packets_connack_auth_error := _,
            emqx_packets_pubrec_inuse := _,
            emqx_packets_sent := _,
            emqx_packets_puback_sent := _,
            emqx_packets_received := _,
            emqx_packets_pubrec_missed := _,
            emqx_packets_unsubscribe_received := _,
            emqx_packets_puback_inuse := _,
            emqx_packets_publish_sent := _,
            emqx_packets_pubrec_sent := _,
            emqx_packets_pubcomp_received := _,
            emqx_packets_disconnect_sent := _,
            emqx_packets_unsuback_sent := _,
            emqx_bytes_sent := _,
            emqx_packets_unsubscribe_error := _,
            emqx_packets_auth_received := _,
            emqx_packets_subscribe_auth_error := _,
            emqx_packets_puback_missed := _,
            emqx_packets_publish_error := _,
            emqx_packets_subscribe_error := _,
            emqx_packets_disconnect_received := _,
            emqx_packets_pingreq_received := _,
            emqx_packets_pubrel_received := _,
            emqx_packets_pubcomp_missed := _,
            emqx_packets_pubrec_received := _,
            emqx_packets_connack_error := _,
            emqx_packets_auth_sent := _,
            emqx_packets_pubrel_sent := _,
            emqx_packets_connect := _
        },
        M
    );
assert_json_data__packets(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

%% certs always return json list
assert_json_data__certs(Ms, _) ->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    emqx_cert_expiry_at := _,
                    listener_type := _,
                    listener_name := _
                },
                M
            )
        end,
        Ms
    ).

assert_json_data__cluster_rpc(Ms, Mode) when
    Mode =:= ?PROM_DATA_MODE__NODE;
    Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED
->
    ?assertMatch(
        #{
            emqx_conf_sync_txid := _
        },
        Ms
    );
assert_json_data__cluster_rpc(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) ->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    emqx_conf_sync_txid := _
                },
                M
            )
        end,
        Ms
    ).

eval_foreach_assert(FunctionName, Ms) ->
    Fun = fun() ->
        ok = lists:foreach(
            fun(M) -> erlang:apply(?MODULE, FunctionName, [M, ?PROM_DATA_MODE__NODE]) end, Ms
        ),
        ok = lists:foreach(fun(M) -> ?assertMatch(#{node := _}, M) end, Ms)
    end,
    Fun().

%% license always map
assert_json_data__license(M, _) ->
    case emqx_release:edition() of
        ce -> ok;
        ee -> ?assertMatch(#{emqx_license_expiry_at := _}, M)
    end.

-define(assert_node_foreach(Ms), lists:foreach(fun(M) -> ?assertMatch(#{node := _}, M) end, Ms)).

assert_json_data__emqx_banned(M, _) ->
    ?assertMatch(#{emqx_banned_count := _}, M).

assert_json_data__emqx_authn(Ms, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    id := _,
                    emqx_authn_enable := _,
                    emqx_authn_failed := _,
                    emqx_authn_nomatch := _,
                    emqx_authn_status := _,
                    emqx_authn_success := _,
                    emqx_authn_total := _,
                    emqx_authn_users_count := _
                },
                M
            )
        end,
        Ms
    );
assert_json_data__emqx_authn(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) ->
    ?assert_node_foreach(Ms).

assert_json_data__emqx_authz(Ms, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    type := _,
                    emqx_authz_allow := _,
                    emqx_authz_deny := _,
                    emqx_authz_enable := _,
                    emqx_authz_nomatch := _,
                    emqx_authz_rules_count := _,
                    emqx_authz_status := _,
                    emqx_authz_total := _
                },
                M
            )
        end,
        Ms
    );
assert_json_data__emqx_authz(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) ->
    ?assert_node_foreach(Ms).

assert_json_data__rules(Ms, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    id := _,
                    emqx_rule_actions_failed := _,
                    emqx_rule_actions_failed_out_of_service := _,
                    emqx_rule_actions_failed_unknown := _,
                    emqx_rule_actions_success := _,
                    emqx_rule_actions_total := _,
                    emqx_rule_enable := _,
                    emqx_rule_failed := _,
                    emqx_rule_failed_exception := _,
                    emqx_rule_failed_no_result := _,
                    emqx_rule_matched := _,
                    emqx_rule_passed := _
                },
                M
            )
        end,
        Ms
    );
assert_json_data__rules(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    ?assert_node_foreach(Ms).

assert_json_data__actions(Ms, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    id := _,
                    emqx_action_dropped := _,
                    emqx_action_dropped_expired := _,
                    emqx_action_dropped_other := _,
                    emqx_action_dropped_queue_full := _,
                    emqx_action_dropped_resource_not_found := _,
                    emqx_action_dropped_resource_stopped := _,
                    emqx_action_enable := _,
                    emqx_action_failed := _,
                    emqx_action_inflight := _,
                    emqx_action_late_reply := _,
                    emqx_action_matched := _,
                    emqx_action_queuing := _,
                    emqx_action_received := _,
                    emqx_action_retried := _,
                    emqx_action_retried_failed := _,
                    emqx_action_retried_success := _,
                    emqx_action_status := _,
                    emqx_action_success := _
                },
                M
            )
        end,
        Ms
    );
assert_json_data__actions(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    ?assert_node_foreach(Ms).

assert_json_data__connectors(Ms, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    id := _,
                    emqx_connector_enable := _,
                    emqx_connector_status := _
                },
                M
            )
        end,
        Ms
    );
assert_json_data__connectors(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    ?assert_node_foreach(Ms).

assert_json_data__data_integration_overview(M, _) ->
    case emqx_release:edition() of
        ee ->
            ?assertMatch(
                #{
                    emqx_connectors_count := _,
                    emqx_rules_count := _,
                    emqx_actions_count := _,
                    emqx_schema_registrys_count := _
                },
                M
            );
        ce ->
            ?assertMatch(
                #{
                    emqx_connectors_count := _,
                    emqx_rules_count := _,
                    emqx_actions_count := _
                },
                M
            )
    end.

assert_json_data__schema_validations(Ms, _) ->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    validation_name := _,
                    emqx_schema_validation_enable := _,
                    emqx_schema_validation_matched := _,
                    emqx_schema_validation_failed := _,
                    emqx_schema_validation_succeeded := _
                },
                M
            )
        end,
        Ms
    ).

assert_json_data__message_transformations(Ms, _) ->
    lists:foreach(
        fun(M) ->
            ?assertMatch(
                #{
                    validation_name := _,
                    emqx_message_transformation_enable := _,
                    emqx_message_transformation_matched := _,
                    emqx_message_transformation_failed := _,
                    emqx_message_transformation_succeeded := _
                },
                M
            )
        end,
        Ms
    ).

schema_validation_config() ->
    Validation = #{
        <<"enable">> => true,
        <<"name">> => <<"my_validation">>,
        <<"topics">> => [<<"t/#">>],
        <<"strategy">> => <<"all_pass">>,
        <<"failure_action">> => <<"drop">>,
        <<"checks">> => [
            #{
                <<"type">> => <<"sql">>,
                <<"sql">> => <<"select * where true">>
            }
        ]
    },
    #{
        <<"schema_validation">> => #{
            <<"validations">> => [Validation]
        }
    }.

message_transformation_config() ->
    Transformation = #{
        <<"enable">> => true,
        <<"name">> => <<"my_transformation">>,
        <<"topics">> => [<<"t/#">>],
        <<"failure_action">> => <<"drop">>,
        <<"operations">> => [
            #{
                <<"key">> => <<"topic">>,
                <<"value">> => <<"concat([topic, '/', payload.t])">>
            }
        ]
    },
    #{
        <<"message_transformation">> => #{
            <<"transformations">> => [Transformation]
        }
    }.

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
