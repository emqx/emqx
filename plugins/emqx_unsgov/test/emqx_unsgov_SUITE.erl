%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_unsgov.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([mria], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    case whereis(emqx_unsgov_store) of
        undefined -> ok;
        Pid -> exit(Pid, shutdown)
    end,
    case whereis(emqx_unsgov_metrics) of
        undefined -> ok;
        Pid2 -> exit(Pid2, shutdown)
    end,
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    Config.

init_per_testcase(_Case, Config) ->
    case whereis(emqx_unsgov_store) of
        undefined ->
            {ok, Pid} = emqx_unsgov_store:start_link(),
            true = unlink(Pid),
            ok;
        _ ->
            ok
    end,
    case whereis(emqx_unsgov_metrics) of
        undefined ->
            {ok, Pid2} = emqx_unsgov_metrics:start_link(),
            true = unlink(Pid2),
            ok;
        _ ->
            ok
    end,
    ok = emqx_unsgov_store:reset(),
    ok = emqx_unsgov_metrics:reset(),
    ok = emqx_unsgov_config:update(#{
        <<"on_mismatch">> => <<"deny">>,
        <<"exempt_topics">> => [<<"$SYS/#">>]
    }),
    Config.

t_topic_validation(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    ?assertEqual(
        {allow, <<"model-v1">>},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>
        )
    ),
    ?assertEqual(
        {deny, <<"model-v1">>, topic_invalid},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/BadLine/LineControl">>
        )
    ),
    ?assertEqual(
        {deny, undefined, topic_nomatch},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1">>
        )
    ),
    ?assertEqual(
        {deny, <<"model-v1">>, payload_invalid},
        validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\"}">>
        )
    ).

t_plugin_api_get_put_post(_Config) ->
    {ok, 200, _, #{id := <<"model-v1">>, active := true}} = emqx_unsgov_api:handle(
        post, [<<"models">>], #{body => #{<<"activate">> => true, <<"model">> => sample_model()}}
    ),
    Model2 = sample_model_v2(),
    {ok, 200, _, #{id := <<"model-v2">>, active := false}} = emqx_unsgov_api:handle(
        post, [<<"models">>], #{body => #{<<"activate">> => false, <<"model">> => Model2}}
    ),
    {ok, 200, _, #{data := Data}} = emqx_unsgov_api:handle(get, [<<"models">>], #{}),
    ?assert(length(Data) >= 2),
    {ok, 200, _, #{id := <<"model-v2">>, active := true}} = emqx_unsgov_api:handle(
        post, [<<"models">>, <<"model-v2">>, <<"activate">>], #{}
    ),
    {ok, 200, _, #{id := <<"model-v2">>, active := false}} = emqx_unsgov_api:handle(
        post, [<<"models">>, <<"model-v2">>, <<"deactivate">>], #{}
    ),
    {ok, 200, _, #{id := <<"model-v2">>, active := true}} = emqx_unsgov_api:handle(
        post, [<<"models">>, <<"model-v2">>, <<"activate">>], #{}
    ),
    Model3 = (sample_model())#{<<"id">> => <<"model-v3">>, <<"name">> => <<"UNS Model V3">>},
    {ok, 200, _, #{id := <<"model-v3">>, active := false}} = emqx_unsgov_api:handle(
        post, [<<"models">>], #{body => #{<<"activate">> => false, <<"model">> => Model3}}
    ),
    {error, 409, _, #{
        code := <<"CONFLICT">>,
        message := _,
        reason := #{cause := conflicting_topic_filter, conflict_with := <<"model-v1">>}
    }} = emqx_unsgov_api:handle(
        post, [<<"models">>, <<"model-v3">>, <<"activate">>], #{}
    ),
    {ok, 200, #{<<"content-type">> := <<"text/html; charset=utf-8">>}, UiBody} =
        emqx_unsgov_api:handle(get, [<<"ui">>], #{}),
    ?assertMatch(<<_/binary>>, UiBody),
    ?assertNotEqual(nomatch, binary:match(UiBody, <<"UNS Governance">>)),
    {ok, 200, #{<<"content-type">> := <<"text/plain; version=0.0.4; charset=utf-8">>}, MetricsBody} =
        emqx_unsgov_api:handle(get, [<<"metrics">>], #{}),
    ?assertNotEqual(nomatch, binary:match(MetricsBody, <<"emqx_unsgov_messages_total">>)),
    {ok, 200, _, #{result := #{valid := true}}} = emqx_unsgov_api:handle(
        post,
        [<<"validate">>, <<"topic">>],
        #{
            body => #{
                <<"topic">> => <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>
            }
        }
    ),
    {ok, 200, _, #{messages_total := _, messages_allowed := _, messages_dropped := _}} =
        emqx_unsgov_api:handle(get, [<<"stats">>], #{}).

t_multi_active_models_and_per_model_stats(_Config) ->
    Model1 = sample_model(),
    Model2 = sample_model_v2(),
    {ok, _} = emqx_unsgov_store:put_model(Model1, true),
    {ok, _} = emqx_unsgov_store:put_model(Model2, true),
    {ok, Entries} = emqx_unsgov_store:list_models(),
    ActiveIds = [maps:get(id, E) || E <- Entries, maps:get(active, E, false) =:= true],
    ?assert(lists:member(<<"model-v1">>, ActiveIds)),
    ?assert(lists:member(<<"model-v2">>, ActiveIds)),

    %% Topic matching model-v1 should be allowed by model-v1.
    ?assertEqual(
        {allow, <<"model-v1">>},
        validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\",\"Mode\":\"auto\"}">>
        )
    ),
    %% Topic matching model-v2 should be allowed by model-v2.
    ?assertEqual(
        {allow, <<"model-v2">>},
        validate_message(
            <<"enterprise/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\",\"Mode\":\"auto\"}">>
        )
    ),

    %% Record allow/drop per model and assert stats snapshot shape.
    emqx_unsgov_metrics:record_allowed(<<"model-v1">>),
    emqx_unsgov_metrics:record_allowed(<<"model-v2">>),
    emqx_unsgov_metrics:record_drop(
        <<"bad/topic">>,
        topic_invalid,
        topic_invalid
    ),
    emqx_unsgov_metrics:record_drop_model(<<"model-v1">>, topic_invalid, topic_invalid),
    emqx_unsgov_metrics:record_drop_model(<<"model-v2">>, topic_invalid, topic_invalid),
    {ok, 200, _, #{per_model := PerModel}} = emqx_unsgov_api:handle(get, [<<"stats">>], #{}),
    ?assert(maps:is_key(<<"model-v1">>, PerModel)),
    ?assert(maps:is_key(<<"model-v2">>, PerModel)),
    M1 = maps:get(<<"model-v1">>, PerModel),
    M2 = maps:get(<<"model-v2">>, PerModel),
    ?assertEqual(2, maps:get(messages_total, M1)),
    ?assertEqual(1, maps:get(messages_allowed, M1)),
    ?assertEqual(1, maps:get(messages_dropped, M1)),
    ?assertEqual(1, maps:get(topic_invalid, M1)),
    ?assertEqual(2, maps:get(messages_total, M2)),
    ?assertEqual(1, maps:get(messages_allowed, M2)),
    ?assertEqual(1, maps:get(messages_dropped, M2)),
    ?assertEqual(1, maps:get(topic_invalid, M2)).

t_models_ordered_by_id_for_ui_and_runtime(_Config) ->
    ModelA = (sample_model())#{<<"id">> => <<"a-model">>, <<"name">> => <<"Model A">>},
    ModelB = (sample_model_v2())#{<<"id">> => <<"b-model">>, <<"name">> => <<"Model B">>},
    %% Insert in reverse lexical order to verify deterministic ID ordering.
    {ok, _} = emqx_unsgov_store:put_model(ModelA, true),
    {ok, _} = emqx_unsgov_store:put_model(ModelB, true),

    {ok, Entries} = emqx_unsgov_store:list_models(),
    Ids = [maps:get(id, E) || E <- Entries],
    ?assertEqual([<<"a-model">>, <<"b-model">>], Ids),

    %% Both models match; runtime should pick the lowest ID first.
    ?assertEqual(
        {allow, <<"a-model">>},
        validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\",\"Mode\":\"auto\"}">>
        )
    ).

t_multi_active_deny_reason_priority_and_details(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    {ok, _} = emqx_unsgov_store:put_model(sample_model_v2(), true),
    ?assertEqual(
        {deny, <<"model-v1">>, payload_invalid},
        validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\"}">>
        )
    ).

t_screen_topic_filter_skips_unrelated_models(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    {ok, _} = emqx_unsgov_store:put_model(sample_model_v2(), true),
    ?assertEqual(
        {deny, <<"model-v1">>, topic_invalid},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/BadLine/LineControl">>
        )
    ),
    ?assertEqual(
        {deny, <<"model-v2">>, topic_invalid},
        emqx_unsgov_store:validate_topic(
            <<"enterprise/Plant1/BatchHouse/Furnaces/F1/Lines/BadLine/LineControl">>
        )
    ).

t_first_screened_model_is_final_choice(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    {error, #{cause := conflicting_topic_filter, conflict_with := <<"model-v1">>}} = emqx_unsgov_store:put_model(
        sample_model_lenient_topic(), true
    ),
    %% Conflicting active model is rejected; existing active model still validates.
    ?assertEqual(
        {deny, <<"model-v1">>, topic_invalid},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/BadLine/LineControl">>
        )
    ).

t_topic_nomatch_counter(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    ?assertEqual(
        {deny, undefined, topic_nomatch},
        emqx_unsgov_store:validate_topic(
            <<"enterprise/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>
        )
    ),
    emqx_unsgov_metrics:record_drop(
        <<"enterprise/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
        topic_nomatch,
        topic_nomatch
    ),
    {ok, 200, _, Stats} = emqx_unsgov_api:handle(get, [<<"stats">>], #{}),
    ?assertEqual(1, maps:get(topic_nomatch, Stats)),
    %% topic_nomatch is not counted as a model-level drop
    ?assertEqual(0, maps:get(messages_dropped, Stats)),
    ?assertEqual(1, maps:get(messages_total, Stats)).

t_no_active_models_deny_all_non_exempt(_Config) ->
    ?assertEqual(
        {deny, undefined, topic_nomatch},
        emqx_unsgov_store:validate_topic(<<"default/Plant1/Status">>)
    ),
    ?assertEqual(
        {deny, undefined, topic_nomatch},
        validate_message(
            <<"default/Plant1/Status">>, <<"{\"Status\":\"running\"}">>
        )
    ),
    ?assertMatch(
        {stop, #{result := deny, reason := topic_nomatch}},
        emqx_unsgov:on_client_authorize(
            #{},
            #{action_type => publish},
            <<"default/Plant1/Status">>,
            allow
        )
    ),
    %% on_message_publish won't deny because the pdict key is not set after authz deny
    Msg = emqx_message:make(<<"client-1">>, <<"default/Plant1/Status">>, <<"payload">>),
    ?assertMatch({ok, _}, emqx_unsgov:on_message_publish(Msg)).

t_no_active_models_allow_exempt_topics(_Config) ->
    ?assertEqual(
        {ok, allow},
        emqx_unsgov:on_client_authorize(
            #{},
            #{action_type => publish},
            <<"$SYS/brokers">>,
            allow
        )
    ),
    Msg = emqx_message:make(<<"client-1">>, <<"$SYS/brokers">>, <<"payload">>),
    ?assertEqual({ok, Msg}, emqx_unsgov:on_message_publish(Msg)).

t_single_model_multi_branch_topic_filters(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model_multi_branch(), true),
    %% Branch A
    ?assertEqual(
        {allow, <<"model-multi-branch">>},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>
        )
    ),
    %% Branch B
    ?assertEqual(
        {allow, <<"model-multi-branch">>},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Utilities/U1/Status">>
        )
    ),
    %% No branch match -> topic_nomatch
    ?assertEqual(
        {deny, undefined, topic_nomatch},
        emqx_unsgov_store:validate_topic(
            <<"default/Plant1/BatchHouse/Unknown/X">>
        )
    ).

t_simplified_tree_without_type_and_var_type(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model_simplified_tree(), true),
    ?assertEqual(
        {allow, <<"model-simple-tree">>},
        emqx_unsgov_store:validate_topic(<<"default/Plant1/Status">>)
    ),
    ?assertEqual(
        {deny, undefined, topic_nomatch},
        emqx_unsgov_store:validate_topic(<<"default/unknown/path">>)
    ).

t_tree_plus_and_hash_wildcards(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model_with_wildcards(), true),
    ?assertEqual(
        {allow, <<"model-wildcards">>},
        emqx_unsgov_store:validate_topic(<<"default/Plant1/Status">>)
    ),
    ?assertEqual(
        {allow, <<"model-wildcards">>},
        emqx_unsgov_store:validate_topic(<<"default/stream">>)
    ),
    ?assertEqual(
        {allow, <<"model-wildcards">>},
        emqx_unsgov_store:validate_topic(<<"default/stream/a/b/c">>)
    ).

t_per_model_stats_breakdown_counters(_Config) ->
    emqx_unsgov_metrics:record_allowed(<<"model-v1">>),
    emqx_unsgov_metrics:record_drop_model(<<"model-v1">>, payload_invalid, payload_invalid),
    emqx_unsgov_metrics:record_drop_model(<<"model-v1">>, not_endpoint, not_endpoint),
    emqx_unsgov_metrics:record_drop_model(<<"model-v2">>, topic_invalid, topic_invalid),
    {ok, 200, _, #{per_model := PerModel}} = emqx_unsgov_api:handle(get, [<<"stats">>], #{}),
    M1 = maps:get(<<"model-v1">>, PerModel),
    M2 = maps:get(<<"model-v2">>, PerModel),
    ?assertEqual(3, maps:get(messages_total, M1)),
    ?assertEqual(1, maps:get(messages_allowed, M1)),
    ?assertEqual(2, maps:get(messages_dropped, M1)),
    ?assertEqual(1, maps:get(payload_invalid, M1)),
    ?assertEqual(1, maps:get(not_endpoint, M1)),
    ?assertEqual(0, maps:get(topic_invalid, M1)),
    ?assertEqual(1, maps:get(messages_total, M2)),
    ?assertEqual(0, maps:get(messages_allowed, M2)),
    ?assertEqual(1, maps:get(messages_dropped, M2)),
    ?assertEqual(1, maps:get(topic_invalid, M2)).

t_delete_model_cleans_per_model_metrics(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    emqx_unsgov_metrics:record_allowed(<<"model-v1">>),
    emqx_unsgov_metrics:record_drop_model(<<"model-v1">>, topic_invalid, topic_invalid),
    {ok, 200, _, #{per_model := PerModel0}} = emqx_unsgov_api:handle(get, [<<"stats">>], #{}),
    ?assert(maps:is_key(<<"model-v1">>, PerModel0)),
    ok = emqx_unsgov_store:delete_model(<<"model-v1">>),
    {ok, 200, _, #{per_model := PerModel1}} = emqx_unsgov_api:handle(get, [<<"stats">>], #{}),
    ?assertEqual(false, maps:is_key(<<"model-v1">>, PerModel1)),
    {error, not_found} = emqx_unsgov_store:get_model(<<"model-v1">>).

t_plugin_api_delete_model(_Config) ->
    {ok, 200, _, #{id := <<"model-v1">>, active := true}} = emqx_unsgov_api:handle(
        post, [<<"models">>], #{body => #{<<"activate">> => true, <<"model">> => sample_model()}}
    ),
    {ok, 200, _, #{id := <<"model-v1">>, deleted := true}} = emqx_unsgov_api:handle(
        delete, [<<"models">>, <<"model-v1">>], #{}
    ),
    {error, 404, _, _} = emqx_unsgov_api:handle(get, [<<"models">>, <<"model-v1">>], #{}),
    {error, 404, _, _} = emqx_unsgov_api:handle(delete, [<<"models">>, <<"model-v1">>], #{}).

t_message_payload_invalid_rejects_immediately(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    {error, #{cause := conflicting_topic_filter, conflict_with := <<"model-v1">>}} = emqx_unsgov_store:put_model(
        sample_model_lenient_payload(), true
    ),
    %% Conflicting active model is rejected, so model-v1 remains selected.
    ?assertEqual(
        {deny, <<"model-v1">>, payload_invalid},
        validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\"}">>
        )
    ).

t_payload_schema_accepts_self_contained_json_schema(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model_full_json_schema_payload(), true),
    ?assertEqual(
        {allow, <<"model-full-json-schema">>},
        validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\",\"Mode\":\"auto\"}">>
        )
    ),
    ?assertEqual(
        {deny, <<"model-full-json-schema">>, payload_invalid},
        validate_message(
            <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
            <<"{\"Status\":\"running\"}">>
        )
    ).

t_payload_schema_rejects_non_object_root(_Config) ->
    ?assertMatch(
        {error, #{cause := invalid_payload_schema, reason := #{cause := payload_schema_not_object}}},
        emqx_unsgov_store:put_model(sample_model_non_object_payload_schema(), true)
    ).

%%--------------------------------------------------------------------
%% Hook behavior: on_mismatch=ignore, non-publish action
%%--------------------------------------------------------------------

t_hook_on_mismatch_ignore(_Config) ->
    %% With no active models, topic_nomatch fires; on_mismatch=ignore should pass through.
    ok = emqx_unsgov_config:update(#{<<"on_mismatch">> => <<"ignore">>}),
    ?assertEqual(
        {ok, allow},
        emqx_unsgov:on_client_authorize(
            #{},
            #{action_type => publish},
            <<"default/Plant1/Status">>,
            allow
        )
    ).

t_hook_non_publish_action_passes_through(_Config) ->
    %% subscribe action should be passed through without governance checks
    ?assertEqual(
        {ok, allow},
        emqx_unsgov:on_client_authorize(
            #{},
            #{action_type => subscribe},
            <<"default/Plant1/Status">>,
            allow
        )
    ).

t_on_message_publish_allow_path(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    Topic = <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
    Payload = <<"{\"Status\":\"running\",\"Mode\":\"auto\"}">>,
    Msg = emqx_message:make(<<"client-1">>, Topic, Payload),
    %% Simulate authz setting the pdict key
    erlang:put(emqx_unsgov_model_id, <<"model-v1">>),
    ?assertMatch({ok, _}, emqx_unsgov:on_message_publish(Msg)).

t_on_message_publish_deny_path(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    Topic = <<"default/Plant1/BatchHouse/Furnaces/F1/Lines/Line1/LineControl">>,
    Payload = <<"{\"Status\":\"running\"}">>,
    Msg = emqx_message:make(<<"client-1">>, Topic, Payload),
    %% Simulate authz setting the pdict key
    erlang:put(emqx_unsgov_model_id, <<"model-v1">>),
    ?assertMatch({stop, _}, emqx_unsgov:on_message_publish(Msg)).

%%--------------------------------------------------------------------
%% Config robustness
%%--------------------------------------------------------------------

t_config_invalid_on_mismatch(_Config) ->
    ok = emqx_unsgov_config:update(#{<<"on_mismatch">> => <<"crash">>}),
    ?assert(lists:member(emqx_unsgov_config:on_mismatch(), [deny, ignore])).

t_config_invalid_exempt_topics_not_list(_Config) ->
    ok = emqx_unsgov_config:update(#{<<"exempt_topics">> => <<"$SYS/#">>}),
    ?assert(is_list(emqx_unsgov_config:exempt_topics())).

t_config_invalid_exempt_topics_empty_string(_Config) ->
    ok = emqx_unsgov_config:update(#{<<"exempt_topics">> => [<<>>]}),
    ?assert(is_list(emqx_unsgov_config:exempt_topics())).

t_config_non_map(_Config) ->
    ok = emqx_unsgov_config:update(not_a_map),
    ?assert(is_atom(emqx_unsgov_config:on_mismatch())).

%%--------------------------------------------------------------------
%% API negative paths
%%--------------------------------------------------------------------

t_api_unknown_route(_Config) ->
    ?assertEqual(
        {error, not_found},
        emqx_unsgov_api:handle(get, [<<"nonexistent">>], #{})
    ).

t_api_validate_topic_missing_topic(_Config) ->
    {ok, _} = emqx_unsgov_store:put_model(sample_model(), true),
    ?assertMatch(
        {error, 400, _, #{code := <<"BAD_REQUEST">>}},
        emqx_unsgov_api:handle(post, [<<"validate">>, <<"topic">>], #{body => #{}})
    ).

t_api_post_model_malformed_body(_Config) ->
    %% Invalid model ID triggers compile error -> 400
    ?assertMatch(
        {error, 400, _, _},
        emqx_unsgov_api:handle(
            post,
            [<<"models">>],
            #{body => #{<<"model">> => #{<<"id">> => <<"has spaces!">>}}}
        )
    ).

t_api_post_model_missing_id(_Config) ->
    %% Model without id is rejected
    ?assertMatch(
        {error, 400, _, #{reason := #{cause := missing_model_id}}},
        emqx_unsgov_api:handle(
            post,
            [<<"models">>],
            #{body => #{<<"model">> => #{<<"name">> => <<"no id">>}}}
        )
    ).

t_api_activate_not_found(_Config) ->
    ?assertMatch(
        {error, 404, _, _},
        emqx_unsgov_api:handle(post, [<<"models">>, <<"nonexistent">>, <<"activate">>], #{})
    ).

t_api_deactivate_not_found(_Config) ->
    ?assertMatch(
        {error, 404, _, _},
        emqx_unsgov_api:handle(post, [<<"models">>, <<"nonexistent">>, <<"deactivate">>], #{})
    ).

t_api_get_model_not_found(_Config) ->
    ?assertMatch(
        {error, 404, _, _},
        emqx_unsgov_api:handle(get, [<<"models">>, <<"nonexistent">>], #{})
    ).

%%--------------------------------------------------------------------
%% Runtime conflict/recovery branches
%%--------------------------------------------------------------------

t_runtime_apply_delete_unknown_model(_Config) ->
    %% Deleting a model that doesn't exist in runtime should not crash
    State0 = #{runtime_filter_owner => #{}},
    State1 = emqx_unsgov_runtime:runtime_apply_delete(<<"ghost">>, State0),
    ?assertMatch(#{runtime_filter_owner := #{}}, State1).

t_runtime_apply_deactivate_unknown_model(_Config) ->
    State0 = #{runtime_filter_owner => #{}},
    State1 = emqx_unsgov_runtime:runtime_apply_deactivate(<<"ghost">>, State0),
    ?assertMatch(#{runtime_filter_owner := #{}}, State1).

t_validate_message_deleted_model_race(_Config) ->
    %% When the compiled model is missing (deleted between topic check and message check),
    %% should return deny with topic_nomatch.
    ?assertEqual(
        {deny, undefined, topic_nomatch},
        emqx_unsgov_runtime:validate_message(<<"deleted-model">>, <<"some/topic">>, <<"{}">>)
    ).

t_bootstrap_conflict_reconciles_active_flag(_Config) ->
    %% Insert two models with conflicting topic filters directly into mria,
    %% both marked active=true. After restarting the store (which runs
    %% build_runtime_state in init), the second one (by ID order) should
    %% have its persisted active flag reconciled to false.
    Model1 = sample_model(),
    Model2 = (sample_model())#{<<"id">> => <<"model-v1-dup">>},
    {ok, C1} = emqx_unsgov_model:compile(Model1),
    {ok, C2} = emqx_unsgov_model:compile(Model2),
    Ts = erlang:system_time(millisecond),
    ok = mria:dirty_write(?MODEL_TAB, #?MODEL_TAB{
        id = maps:get(id, C1),
        model = maps:get(raw, C1),
        summary = emqx_unsgov_model:summary(C1),
        active = true,
        updated_at_ms = Ts
    }),
    ok = mria:dirty_write(?MODEL_TAB, #?MODEL_TAB{
        id = maps:get(id, C2),
        model = maps:get(raw, C2),
        summary = emqx_unsgov_model:summary(C2),
        active = true,
        updated_at_ms = Ts
    }),
    %% Both are active=true in DB before restart
    {ok, R1Before} = emqx_unsgov_store:get_model_record(<<"model-v1">>),
    {ok, R2Before} = emqx_unsgov_store:get_model_record(<<"model-v1-dup">>),
    ?assertEqual(true, emqx_unsgov_store:model_active(R1Before)),
    ?assertEqual(true, emqx_unsgov_store:model_active(R2Before)),
    %% Restart store — init calls build_runtime_state which reconciles conflicts
    ok = gen_server:stop(emqx_unsgov_store),
    {ok, Pid} = emqx_unsgov_store:start_link(),
    true = unlink(Pid),
    %% model-v1 wins (first by ID order), model-v1-dup should be flipped to inactive
    {ok, R1After} = emqx_unsgov_store:get_model_record(<<"model-v1">>),
    {ok, R2After} = emqx_unsgov_store:get_model_record(<<"model-v1-dup">>),
    ?assertEqual(true, emqx_unsgov_store:model_active(R1After)),
    ?assertEqual(false, emqx_unsgov_store:model_active(R2After)).

validate_message(Topic, Payload) ->
    case emqx_unsgov_store:validate_topic(Topic) of
        {allow, ModelId} ->
            emqx_unsgov_store:validate_message(ModelId, Topic, Payload);
        {deny, _, _} = Deny ->
            Deny
    end.

sample_model() ->
    #{
        <<"id">> => <<"model-v1">>,
        <<"name">> => <<"UNS Model V1">>,
        <<"variable_types">> => #{
            <<"site_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"area_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"process_cell_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"line_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^Line[0-9]{1,4}$">>
            }
        },
        <<"payload_types">> => #{
            <<"line_control">> => #{
                <<"type">> => <<"object">>,
                <<"required">> => [<<"Status">>, <<"Mode">>],
                <<"properties">> => #{
                    <<"Status">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"running">>, <<"stopped">>]
                    },
                    <<"Mode">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"auto">>, <<"manual">>]
                    }
                },
                <<"additionalProperties">> => false
            }
        },
        <<"tree">> => #{
            <<"default">> => #{
                <<"_type">> => <<"namespace">>,
                <<"children">> => #{
                    <<"{site_id}">> => #{
                        <<"_type">> => <<"variable">>,
                        <<"_var_type">> => <<"site_id">>,
                        <<"children">> => #{
                            <<"{area_id}">> => #{
                                <<"_type">> => <<"variable">>,
                                <<"_var_type">> => <<"area_id">>,
                                <<"children">> => #{
                                    <<"Furnaces">> => #{
                                        <<"_type">> => <<"namespace">>,
                                        <<"children">> => #{
                                            <<"{process_cell_id}">> => #{
                                                <<"_type">> => <<"variable">>,
                                                <<"_var_type">> => <<"process_cell_id">>,
                                                <<"children">> => #{
                                                    <<"Lines">> => #{
                                                        <<"_type">> => <<"namespace">>,
                                                        <<"children">> => #{
                                                            <<"{line_id}">> => #{
                                                                <<"_type">> => <<"variable">>,
                                                                <<"_var_type">> => <<"line_id">>,
                                                                <<"children">> => #{
                                                                    <<"LineControl">> => #{
                                                                        <<"_type">> =>
                                                                            <<"endpoint">>,
                                                                        <<"_payload">> =>
                                                                            <<"line_control">>
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }.

sample_model_full_json_schema_payload() ->
    Model0 = sample_model(),
    Model0#{
        <<"id">> => <<"model-full-json-schema">>,
        <<"name">> => <<"UNS Model Full JSON Schema">>,
        <<"payload_types">> => #{
            <<"line_control">> => #{
                <<"type">> => <<"object">>,
                <<"required">> => [<<"Status">>, <<"Mode">>],
                <<"properties">> => #{
                    <<"Status">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"running">>, <<"stopped">>]
                    },
                    <<"Mode">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"auto">>, <<"manual">>]
                    }
                },
                <<"additionalProperties">> => false
            }
        }
    }.

sample_model_non_object_payload_schema() ->
    Model0 = sample_model(),
    Model0#{
        <<"id">> => <<"model-invalid-payload-schema">>,
        <<"name">> => <<"UNS Model Invalid Payload Schema">>,
        <<"payload_types">> => #{
            <<"line_control">> => #{
                <<"type">> => <<"string">>
            }
        }
    }.

sample_model_v2() ->
    Model = sample_model(),
    Tree1 = maps:get(<<"tree">>, Model),
    Root = maps:get(<<"default">>, Tree1),
    Tree2 = #{<<"enterprise">> => Root},
    Model#{
        <<"id">> => <<"model-v2">>,
        <<"name">> => <<"UNS Model V2">>,
        <<"tree">> => Tree2
    }.

sample_model_lenient_payload() ->
    Model = sample_model(),
    PayloadTypes0 = maps:get(<<"payload_types">>, Model),
    LineControl0 = maps:get(<<"line_control">>, PayloadTypes0),
    LineControl = LineControl0#{<<"required">> => [<<"Status">>]},
    PayloadTypes = PayloadTypes0#{<<"line_control">> => LineControl},
    Model#{
        <<"id">> => <<"model-v3">>,
        <<"name">> => <<"UNS Model V3 Lenient Payload">>,
        <<"payload_types">> => PayloadTypes
    }.

sample_model_lenient_topic() ->
    Model = sample_model(),
    VarTypes0 = maps:get(<<"variable_types">>, Model),
    LineType0 = maps:get(<<"line_id">>, VarTypes0),
    LineType = LineType0#{<<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>},
    VarTypes = VarTypes0#{<<"line_id">> => LineType},
    Model#{
        <<"id">> => <<"model-v9">>,
        <<"name">> => <<"UNS Model V9 Lenient Topic">>,
        <<"variable_types">> => VarTypes
    }.

sample_model_multi_branch() ->
    #{
        <<"id">> => <<"model-multi-branch">>,
        <<"name">> => <<"UNS Model Multi Branch">>,
        <<"variable_types">> => #{
            <<"site_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"area_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"process_cell_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            },
            <<"line_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^Line[0-9]{1,4}$">>
            },
            <<"unit_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            }
        },
        <<"payload_types">> => #{
            <<"line_control">> => #{
                <<"type">> => <<"object">>,
                <<"required">> => [<<"Status">>, <<"Mode">>],
                <<"properties">> => #{
                    <<"Status">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"running">>, <<"stopped">>]
                    },
                    <<"Mode">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"auto">>, <<"manual">>]
                    }
                },
                <<"additionalProperties">> => false
            },
            <<"unit_status">> => #{
                <<"type">> => <<"object">>,
                <<"required">> => [<<"State">>],
                <<"properties">> => #{
                    <<"State">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"ok">>, <<"warn">>, <<"error">>]
                    }
                },
                <<"additionalProperties">> => false
            }
        },
        <<"tree">> => #{
            <<"default">> => #{
                <<"_type">> => <<"namespace">>,
                <<"children">> => #{
                    <<"{site_id}">> => #{
                        <<"_type">> => <<"variable">>,
                        <<"_var_type">> => <<"site_id">>,
                        <<"children">> => #{
                            <<"{area_id}">> => #{
                                <<"_type">> => <<"variable">>,
                                <<"_var_type">> => <<"area_id">>,
                                <<"children">> => #{
                                    <<"Furnaces">> => #{
                                        <<"_type">> => <<"namespace">>,
                                        <<"children">> => #{
                                            <<"{process_cell_id}">> => #{
                                                <<"_type">> => <<"variable">>,
                                                <<"_var_type">> => <<"process_cell_id">>,
                                                <<"children">> => #{
                                                    <<"Lines">> => #{
                                                        <<"_type">> => <<"namespace">>,
                                                        <<"children">> => #{
                                                            <<"{line_id}">> => #{
                                                                <<"_type">> => <<"variable">>,
                                                                <<"_var_type">> => <<"line_id">>,
                                                                <<"children">> => #{
                                                                    <<"LineControl">> => #{
                                                                        <<"_type">> =>
                                                                            <<"endpoint">>,
                                                                        <<"_payload">> =>
                                                                            <<"line_control">>
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    <<"Utilities">> => #{
                                        <<"_type">> => <<"namespace">>,
                                        <<"children">> => #{
                                            <<"{unit_id}">> => #{
                                                <<"_type">> => <<"variable">>,
                                                <<"_var_type">> => <<"unit_id">>,
                                                <<"children">> => #{
                                                    <<"Status">> => #{
                                                        <<"_type">> => <<"endpoint">>,
                                                        <<"_payload">> => <<"unit_status">>
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }.

sample_model_simplified_tree() ->
    #{
        <<"id">> => <<"model-simple-tree">>,
        <<"name">> => <<"UNS Model Simple Tree">>,
        <<"variable_types">> => #{
            <<"site_id">> => #{
                <<"type">> => <<"string">>,
                <<"pattern">> => <<"^[A-Za-z][A-Za-z0-9_]{0,31}$">>
            }
        },
        <<"tree">> => #{
            <<"default">> => #{
                <<"children">> => #{
                    <<"{site_id}">> => #{
                        <<"children">> => #{
                            <<"Status">> => #{
                                <<"_payload">> => <<"any">>
                            }
                        }
                    }
                }
            }
        }
    }.

sample_model_with_wildcards() ->
    #{
        <<"id">> => <<"model-wildcards">>,
        <<"name">> => <<"UNS Model Wildcards">>,
        <<"tree">> => #{
            <<"default">> => #{
                <<"children">> => #{
                    <<"+">> => #{
                        <<"children">> => #{
                            <<"Status">> => #{
                                <<"_payload">> => <<"any">>
                            }
                        }
                    },
                    <<"stream">> => #{
                        <<"children">> => #{
                            <<"#">> => #{
                                <<"_payload">> => <<"any">>
                            }
                        }
                    }
                }
            }
        }
    }.
