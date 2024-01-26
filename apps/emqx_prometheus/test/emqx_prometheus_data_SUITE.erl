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

-module(emqx_prometheus_data_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_prometheus.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

all() ->
    [
        {group, stats},
        {group, auth},
        {group, data_integration}
    ].

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
        {stats, ModeGroups},
        {auth, ModeGroups},
        {data_integration, ModeGroups},
        {?PROM_DATA_MODE__NODE, AcceptGroups},
        {?PROM_DATA_MODE__ALL_NODES_AGGREGATED, AcceptGroups},
        {?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, AcceptGroups},
        {'text/plain', TCs},
        {'application/json', TCs}
    ].

init_per_suite(Config) ->
    meck:new(emqx_retainer, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_retainer, retained_count, fun() -> 0 end),
    emqx_prometheus_SUITE:init_group(),
    ok = emqx_common_test_helpers:start_apps(
        [emqx, emqx_conf, emqx_auth, emqx_rule_engine, emqx_prometheus],
        fun set_special_configs/1
    ),
    Config.
end_per_suite(Config) ->
    meck:unload([emqx_retainer]),
    emqx_prometheus_SUITE:end_group(),
    emqx_common_test_helpers:stop_apps(
        [emqx, emqx_conf, emqx_auth, emqx_rule_engine, emqx_prometheus]
    ),

    Config.

init_per_group(stats, Config) ->
    [{module, emqx_prometheus} | Config];
init_per_group(auth, Config) ->
    [{module, emqx_prometheus_auth} | Config];
init_per_group(data_integration, Config) ->
    [{module, emqx_prometheus_data_integration} | Config];
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

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(emqx_auth) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], true),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(emqx_prometheus) ->
    emqx_prometheus_SUITE:load_config(),
    ok;
set_special_configs(_App) ->
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
    ?assert(is_map(JsonData), true),
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
    do_assert_prom_data_stats(NDataL, Mode).

-define(MGU(K, MAP), maps:get(K, MAP, undefined)).

assert_json_data(emqx_prometheus, Data, Mode) ->
    lists:foreach(
        fun(FunSeed) ->
            erlang:apply(?MODULE, fun_name(FunSeed), [?MGU(FunSeed, Data), Mode]),
            ok
        end,
        maps:keys(Data)
    ),
    ok;
%% TOOD auth/data_integration
assert_json_data(_, _, _) ->
    ok.

fun_name(Seed) ->
    binary_to_atom(<<"assert_json_data_", (atom_to_binary(Seed))/binary>>).

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

do_assert_prom_data_stats([], _Mode) ->
    ok;
do_assert_prom_data_stats([Metric | RestDataL], Mode) ->
    [_MetricNamme | _] = Metric,
    assert_stats_metric_labels(Metric, Mode),
    do_assert_prom_data_stats(RestDataL, Mode).

assert_stats_metric_labels([MetricName | R] = _Metric, Mode) ->
    case maps:get(Mode, metric_meta(MetricName), undefined) of
        %% for uncatched metrics (by prometheus.erl)
        undefined ->
            ok;
        N when is_integer(N) ->
            ?assertEqual(N, length(lists:droplast(R)))
    end.

-define(meta(NODE, AGGRE, UNAGGRE), #{
    ?PROM_DATA_MODE__NODE => NODE,
    ?PROM_DATA_MODE__ALL_NODES_AGGREGATED => AGGRE,
    ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED => UNAGGRE
}).

%% TODO: auth/data_integration
%% BEGIN always no label
metric_meta(<<"emqx_topics_max">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_topics_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_retained_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_retained_max">>) -> ?meta(0, 0, 0);
%% END
%% BEGIN no label in mode `node`
metric_meta(<<"emqx_vm_cpu_use">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_cpu_idle">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_run_queue">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_process_messages_in_queues">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_total_memory">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_used_memory">>) -> ?meta(0, 1, 1);
%% END
metric_meta(<<"emqx_cert_expiry_at">>) -> ?meta(2, 2, 2);
metric_meta(<<"emqx_license_expiry_at">>) -> ?meta(0, 0, 0);
%% mria metric with label `shard` and `node` when not in mode `node`
metric_meta(<<"emqx_mria_", _Tail/binary>>) -> ?meta(1, 2, 2);
metric_meta(_) -> #{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Assert Json Data Structure

assert_json_data_messages(M, Mode) when
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
assert_json_data_messages(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_stats(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{
            emqx_connections_count := _,
            emqx_connections_max := _,
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
assert_json_data_stats(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_olp(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(#{}, M);
assert_json_data_olp(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    ok.

assert_json_data_client(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
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
assert_json_data_client(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_session(M, Mode) when
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
assert_json_data_session(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when is_list(Ms) ->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_metrics(M, ?PROM_DATA_MODE__NODE) ->
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
assert_json_data_metrics(Ms, Mode) when
    is_list(Ms) andalso
        (Mode =:= ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED orelse
            Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_delivery(M, Mode) when
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
assert_json_data_delivery(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_cluster(M, Mode) when
    (Mode =:= ?PROM_DATA_MODE__NODE orelse
        Mode =:= ?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
->
    ?assertMatch(
        #{emqx_cluster_nodes_running := _, emqx_cluster_nodes_stopped := _},
        M
    );
assert_json_data_cluster(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_acl(M, Mode) when
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
assert_json_data_acl(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_authn(M, Mode) when
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
assert_json_data_authn(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

assert_json_data_packets(M, Mode) when
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
assert_json_data_packets(Ms, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED) when
    is_list(Ms)
->
    eval_foreach_assert(?FUNCTION_NAME, Ms).

%% certs always return json list
assert_json_data_certs(Ms, _) ->
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

-if(?EMQX_RELEASE_EDITION == ee).
%% license always map
assert_json_data_license(M, _) ->
    ?assertMatch(#{emqx_license_expiry_at := _}, M).
-else.
-endif.

eval_foreach_assert(FunctionName, Ms) ->
    Fun = fun() ->
        ok = lists:foreach(
            fun(M) -> erlang:apply(?MODULE, FunctionName, [M, ?PROM_DATA_MODE__NODE]) end, Ms
        ),
        ok = lists:foreach(fun(M) -> ?assertMatch(#{node := _}, M) end, Ms)
    end,
    Fun().
