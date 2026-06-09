%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Exercises the v2 prometheus collector module directly: register
%% it with the prometheus registry, populate counters, render the
%% text exposition, and assert on the resulting series lines.

-module(emqx_topic_metrics_prometheus_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, #{override_env => [{boot_modules, [broker]}]}},
            emqx_topic_metrics,
            emqx_prometheus
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    ok.

%%------------------------------------------------------------------------------
%% Cases
%%------------------------------------------------------------------------------

t_emits_series_per_collection(_Config) ->
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"beta">>, <<"beta/#">>, <<"acme">>),
    {ok, #{counter_ref := AlphaRef}} =
        emqx_topic_metrics_registry:lookup({?global_ns, <<"alpha">>}),
    counters:add(AlphaRef, 1, 7),
    Out = emqx_prometheus_topic_metrics:collect(<<"prometheus">>),
    OutStr = binary_to_list(iolist_to_binary(Out)),
    %% one metric family per counter:
    ?assert(string:str(OutStr, "emqx_topic_metric_messages_in_count") > 0),
    ?assert(string:str(OutStr, "emqx_topic_metric_messages_dropped_count") > 0),
    %% label set:
    ?assert(string:str(OutStr, "name=\"alpha\"") > 0),
    ?assert(string:str(OutStr, "topic_filter=\"alpha/#\"") > 0),
    ?assert(string:str(OutStr, "namespace=\"$global\"") > 0),
    ?assert(string:str(OutStr, "namespace=\"acme\"") > 0),
    %% counter value:
    ?assert(
        contains_line(
            OutStr,
            "emqx_topic_metric_messages_in_count{",
            "name=\"alpha\"",
            "} 7"
        )
    ).

t_no_collections_renders_empty(_Config) ->
    Out = emqx_prometheus_topic_metrics:collect(<<"prometheus">>),
    %% Headers per metric family ("# TYPE ...") are emitted even
    %% with no samples; concrete series lines should not appear.
    OutStr = binary_to_list(iolist_to_binary(Out)),
    ?assertEqual(
        0,
        string:str(
            OutStr,
            "emqx_topic_metric_messages_in_count{name=\""
        )
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

%% Returns true if any line of the rendered exposition contains all
%% the given fragments.
contains_line(Out, F1, F2, F3) ->
    lists:any(
        fun(Line) ->
            string:str(Line, F1) > 0 andalso
                string:str(Line, F2) > 0 andalso
                string:str(Line, F3) > 0
        end,
        string:split(Out, "\n", all)
    ).
