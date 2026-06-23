%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_data_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_prometheus.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%% erlfmt-ignore
-define(EMQX_CONF_CONFIG, <<"
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

%% erlfmt-ignore
-define(EMQX_CONFIG, <<"
durable_sessions.enable = true
durable_storage.messages {
  backend = builtin_local
}
">>).

%% erlfmt-ignore
-define(EMQX_DS_RAFT_CONFIG, <<"
durable_sessions.enable = true
durable_storage.messages {
  backend = builtin_raft
  n_shards = 8
}
">>).

all() ->
    [
        {group, general},
        {group, ds_raft}
    ].

groups() ->
    TCs = [t_collect_prom_data],
    GeneralGroups = [
        {group, '/prometheus/stats'},
        {group, '/prometheus/auth'},
        {group, '/prometheus/data_integration'},
        {group, '/prometheus/schema_validation'},
        {group, '/prometheus/message_transformation'}
    ],
    DSGroups = [
        {group, '/prometheus/stats'}
    ],
    AcceptGroups = [
        {group, 'text/plain'}
    ],
    ModeGroups = [
        {group, ?PROM_DATA_MODE__NODE},
        {group, ?PROM_DATA_MODE__ALL_NODES_AGGREGATED},
        {group, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED}
    ],
    [
        {general, GeneralGroups},
        {ds_raft, DSGroups},
        {'/prometheus/stats', ModeGroups},
        {'/prometheus/auth', ModeGroups},
        {'/prometheus/data_integration', ModeGroups},
        {'/prometheus/schema_validation', ModeGroups},
        {'/prometheus/message_transformation', ModeGroups},
        {?PROM_DATA_MODE__NODE, AcceptGroups},
        {?PROM_DATA_MODE__ALL_NODES_AGGREGATED, AcceptGroups},
        {?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, AcceptGroups},
        {'text/plain', TCs}
    ].

init_per_suite(Config) ->
    emqx_prometheus_SUITE:mock_license(),
    emqx_prometheus_SUITE:start_mock_pushgateway(9091),
    Config.

end_per_suite(_Config) ->
    emqx_prometheus_SUITE:unmock_license(),
    emqx_prometheus_SUITE:stop_mock_pushgateway().

init_per_group(general = GroupName, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, ?EMQX_CONFIG},
            {emqx_conf, ?EMQX_CONF_CONFIG},
            emqx_auth,
            emqx_auth_mnesia,
            emqx_rule_engine,
            emqx_bridge_http,
            emqx_connector,
            {emqx_schema_validation, #{config => schema_validation_config()}},
            {emqx_message_transformation, #{config => message_transformation_config()}},
            {emqx_prometheus, emqx_prometheus_SUITE:legacy_conf_default()},
            emqx_management
        ],
        #{work_dir => emqx_cth_suite:work_dir(GroupName, Config)}
    ),
    [{apps, Apps} | Config];
init_per_group(ds_raft = GroupName, Config) ->
    Apps = emqx_cth_suite:start(
        lists:flatten([
            {emqx, ?EMQX_DS_RAFT_CONFIG},
            emqx_conf,
            {emqx_prometheus, emqx_prometheus_SUITE:legacy_conf_default()},
            emqx_management
        ]),
        #{work_dir => emqx_cth_suite:work_dir(GroupName, Config)}
    ),
    [{apps, Apps} | Config];
init_per_group('/prometheus/stats', Config) ->
    [{module, emqx_prometheus} | Config];
init_per_group('/prometheus/auth', Config) ->
    [{module, emqx_prometheus_auth} | Config];
init_per_group('/prometheus/data_integration', Config) ->
    [
        {module, emqx_prometheus_data_integration},
        {setup, fun() ->
            emqx_prometheus_data_integration:put_namespace_pd(?global_ns)
        end}
        | Config
    ];
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
init_per_group(_Group, Config) ->
    Config.

end_per_group(general, Config) ->
    emqx_cth_suite:stop(?config(apps, Config));
end_per_group(ds_raft, Config) ->
    emqx_cth_suite:stop(?config(apps, Config));
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_collect_prom_data(Config) ->
    CollectOpts = collect_opts(Config),
    Module = ?config(module, Config),
    maybe
        {setup, SetupFn} ?= lists:keyfind(setup, 1, Config),
        SetupFn()
    end,
    Response = emqx_prometheus_api:collect(Module, CollectOpts),
    assert_data(Module, Response, CollectOpts).

%%--------------------------------------------------------------------
%% Helper fns
%%--------------------------------------------------------------------

assert_data(Module, {Code, Header, RawDataBinary}, #{type := <<"prometheus">>, mode := Mode}) ->
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
    assert_prom_data(DataL, Mode, Module).

%%%%%%%%%%%%%%%%%%%%
%% assert text/plain format
assert_prom_data(DataL, Mode, Module) ->
    NDataL = lists:map(
        fun(Line) ->
            binary:split(Line, [<<"{">>, <<",">>, <<"} ">>, <<" ">>], [global])
        end,
        DataL
    ),
    case Module of
        emqx_prometheus -> assert_prom_data_required_metrics(NDataL);
        _ -> ok
    end,
    do_assert_prom_data(NDataL, Mode).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

collect_opts(Config) ->
    #{
        type => accept(?config(accept, Config)),
        mode => ?config(mode, Config)
    }.

accept('text/plain') ->
    <<"prometheus">>.

do_assert_prom_data([], _Mode) ->
    ok;
do_assert_prom_data([Metric | RestDataL], Mode) ->
    [_MetricName | _] = Metric,
    assert_stats_metric_labels(Metric, Mode),
    do_assert_prom_data(RestDataL, Mode).

assert_prom_data_required_metrics(NDataL) ->
    MetricNames = sets:from_list([Name || [Name | _] <- NDataL]),
    lists:foreach(
        fun(Required) ->
            ?assert(
                sets:is_element(Required, MetricNames),
                lists:flatten(
                    io_lib:format("Required metric ~s not found in prometheus output", [Required])
                )
            )
        end,
        required_prom_metrics()
    ).

required_prom_metrics() ->
    [
        <<"emqx_routes_count">>,
        <<"emqx_routes_max">>
    ].

assert_stats_metric_labels([MetricName | R] = _Metric, Mode) ->
    %% The last element is the value
    LabelCount = length(R) - 1,
    ExpectedLabelCount = maps:get(Mode, metric_meta(MetricName), undefined),
    case ExpectedLabelCount of
        %% for uncatched metrics (by prometheus.erl)
        undefined ->
            ok;
        LabelCount ->
            ok;
        _ ->
            ct:fail(
                "Label count mismatch for metric: ~p and mode: ~p, expected: ~p, got: ~p",
                [MetricName, Mode, ExpectedLabelCount, LabelCount]
            )
    end.

-define(is_time(BIN), binary_part(BIN, byte_size(BIN), -4) == <<"time">>).

-define(meta(AGGRE), ?meta(AGGRE, AGGRE, AGGRE + 1)).
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
metric_meta(<<"emqx_routes_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_routes_max">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_retained_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_retained_max">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_subscriptions_shared_count">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_subscriptions_shared_max">>) -> ?meta(0, 0, 0);
%% END
%% BEGIN no label in mode `node`
metric_meta(<<"emqx_vm_cpu_use">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_cpu_idle">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_run_queue">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_uptime_ms">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_max_fds">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_total_memory">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_used_memory">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_mnesia_tm_mailbox_size">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_vm_broker_pool_max_mailbox_size">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_cluster_nodes_running">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_cluster_nodes_stopped">>) -> ?meta(0, 1, 1);
metric_meta(<<"emqx_conf_sync_txid">>) -> ?meta(0, 1, 1);
%% END
metric_meta(<<"emqx_cert_expiry_at">>) -> ?meta(2, 2, 2);
metric_meta(<<"emqx_license_max_sessions">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_license_expiry_at">>) -> ?meta(0, 0, 0);
metric_meta(<<"emqx_license_issued_at">>) -> ?meta(0, 0, 0);
%% broker instr
metric_meta(<<"emqx_instr_", _Tail/binary>>) -> #{};
%% mria metric with label `shard` and `node` when not in mode `node`
metric_meta(<<"emqx_mria_", _Tail/binary>>) -> ?meta(1, 2, 2);
%% `/prometheus/auth`
metric_meta(<<"emqx_authn_latency_bucket">>) -> ?meta(2, 2, 3);
metric_meta(<<"emqx_authz_latency_bucket">>) -> ?meta(2, 2, 3);
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
%% DS metrics
metric_meta(<<"emqx_ds_buffer_", Tail/binary>>) when ?is_time(Tail) -> ?meta(3);
metric_meta(<<"emqx_ds_buffer_latency">>) -> ?meta(3);
metric_meta(<<"emqx_ds_buffer_", _Tail/binary>>) -> ?meta(2);
metric_meta(<<"emqx_ds_store_batch_time">>) -> ?meta(2);
metric_meta(<<"emqx_ds_builtin_next_time">>) -> ?meta(2);
metric_meta(<<"emqx_ds_subs_fanout_time">>) -> ?meta(2);
metric_meta(<<"emqx_ds_subs_stuck_total">>) -> ?meta(1);
metric_meta(<<"emqx_ds_subs_unstuck_total">>) -> ?meta(1);
metric_meta(<<"emqx_ds_subs_request_sharing", _Tail/binary>>) -> ?meta(4);
metric_meta(<<"emqx_ds_subs_", Tail/binary>>) when ?is_time(Tail) -> ?meta(4);
metric_meta(<<"emqx_ds_subs", _Tail/binary>>) -> ?meta(3);
metric_meta(<<"emqx_ds_storage", _Tail/binary>>) -> ?meta(1);
%% DS Raft metrics
metric_meta(<<"emqx_ds_raft_cluster_sites_num">>) -> ?meta(1, 1, 1);
metric_meta(<<"emqx_ds_raft_db_sites_num">>) -> ?meta(2, 2, 2);
metric_meta(<<"emqx_ds_raft_db_shards_online_num">>) -> ?meta(1);
metric_meta(<<"emqx_ds_raft_db", _Tail/binary>>) -> ?meta(1, 1, 1);
metric_meta(<<"emqx_ds_raft_shard_transitions">>) -> ?meta(4);
metric_meta(<<"emqx_ds_raft_shard_transition_errors">>) -> ?meta(2);
metric_meta(<<"emqx_ds_raft_shard_transition_queue_len">>) -> ?meta(3, 3, 3);
metric_meta(<<"emqx_ds_raft_shard", _Tail/binary>>) -> ?meta(2, 2, 2);
metric_meta(<<"emqx_ds_raft_snapshot_reads">>) -> ?meta(3);
metric_meta(<<"emqx_ds_raft_snapshot_writes">>) -> ?meta(3);
metric_meta(<<"emqx_ds_raft_snapshot", _Tail/binary>>) -> ?meta(2);
metric_meta(<<"emqx_ds_raft_rasrv_state_changes">>) -> ?meta(3);
metric_meta(<<"emqx_ds_raft_rasrv_replication_msgs">>) -> ?meta(3);
metric_meta(<<"emqx_ds_raft_rasrv_index", _Tail/binary>>) -> ?meta(3);
metric_meta(<<"emqx_ds_raft_rasrv", _Tail/binary>>) -> ?meta(2);
metric_meta(<<"emqx_ds", _Tail/binary>>) -> #{};
%% normal emqx metrics
metric_meta(<<"emqx_", _Tail/binary>>) -> ?meta(0, 0, 1);
metric_meta(_) -> #{}.

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
