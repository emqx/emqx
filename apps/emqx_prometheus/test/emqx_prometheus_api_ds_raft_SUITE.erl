%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_api_ds_raft_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_prometheus/include/emqx_prometheus.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        lists:flatten([
            emqx_ds_builtin_raft,
            {emqx_conf, #{
                schema_mod => emqx_enterprise_schema,
                config =>
                    "\ndurable_sessions { enable = true }"
                    "\ndurable_storage.messages {"
                    "\n  backend = builtin_raft"
                    "\n  n_sites = 1"
                    "\n  n_shards = 2"
                    "\n}"
            }},
            emqx,
            {emqx_prometheus, "prometheus { enable_basic_auth = false }"}
        ]),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_ds_builtin_raft(_Config) ->
    Opts1 = #{type => <<"prometheus">>, mode => ?PROM_DATA_MODE__NODE},
    {200, Headers, Body1} = emqx_prometheus_api:collect(emqx_prometheus_ds_builtin_raft, Opts1),
    ?assertMatch(#{<<"content-type">> := <<"text/plain">>}, Headers),
    ?assertMatch(
        [
            "# TYPE emqx_ds_raft_cluster_sites_num gauge",
            "# HELP" ++ _,
            "emqx_ds_raft_cluster_sites_num{status=\"any\"} 1",
            "emqx_ds_raft_cluster_sites_num{status=\"lost\"} 0",
            "# TYPE emqx_ds_raft_db_shards_num gauge",
            "# HELP" ++ _,
            "emqx_ds_raft_db_shards_num{db=\"messages\"} 2",
            "# TYPE emqx_ds_raft_db_sites_num gauge",
            "# HELP" ++ _,
            "emqx_ds_raft_db_sites_num{status=\"current\",db=\"messages\"} 1",
            "emqx_ds_raft_db_sites_num{status=\"assigned\",db=\"messages\"} 1",
            "# TYPE emqx_ds_raft_shard_replication_factor gauge",
            "# HELP" ++ _,
            "emqx_ds_raft_shard_replication_factor{db=\"messages\",shard=\"0\"} 1",
            "emqx_ds_raft_shard_replication_factor{db=\"messages\",shard=\"1\"} 1",
            "# TYPE emqx_ds_raft_shard_transition_queue_len gauge",
            "# HELP" ++ _,
            "emqx_ds_raft_shard_transition_queue_len{type=\"add\",db=\"messages\",shard=\"0\"} 0",
            "emqx_ds_raft_shard_transition_queue_len{type=\"del\",db=\"messages\",shard=\"0\"} 0",
            "emqx_ds_raft_shard_transition_queue_len{type=\"add\",db=\"messages\",shard=\"1\"} 0",
            "emqx_ds_raft_shard_transition_queue_len{type=\"del\",db=\"messages\",shard=\"1\"} 0",
            "# TYPE emqx_ds_raft_db_shards_online_num gauge",
            "# HELP" ++ _,
            "emqx_ds_raft_db_shards_online_num{db=\"messages\"} 2",
            "# TYPE emqx_ds_raft_shard_transitions counter",
            "# HELP" ++ _,
            "emqx_ds_raft_shard_transitions{type=\"add\",status=\"started\",db=\"messages\",shard=\"0\"} 0",
            "emqx_ds_raft_shard_transitions{type=\"del\",status=\"started\",db=\"messages\",shard=\"0\"} 0",
            "emqx_ds_raft_shard_transitions{type=\"add\",status=\"completed\",db=\"messages\",shard=\"0\"} 0",
            "emqx_ds_raft_shard_transitions{type=\"del\",status=\"completed\",db=\"messages\",shard=\"0\"} 0"
            | _
        ],
        string:split(unicode:characters_to_list(Body1), "\n", all)
    ).
