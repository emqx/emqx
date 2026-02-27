%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-include("../src/emqx_a2a_registry_internal.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(durable_sessions, durable_sessions).
-define(memory_sessions, memory_sessions).

-define(ORG_ID, <<"org_id">>).
-define(UNIT_ID, <<"unit_id">>).
-define(AGENT_ID, <<"agent_id">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?memory_sessions},
        {group, ?durable_sessions}
    ].

groups() ->
    AllTCs0 = emqx_common_test_helpers:all_with_matrix(?MODULE),
    AllTCs = lists:filter(
        fun
            ({group, _}) -> false;
            (_) -> true
        end,
        AllTCs0
    ),
    CustomMatrix = emqx_common_test_helpers:groups_with_matrix(?MODULE),
    DSTCs = merge_custom_groups(?durable_sessions, AllTCs, CustomMatrix),
    MemTCs = merge_custom_groups(?memory_sessions, AllTCs, CustomMatrix),
    [
        {?durable_sessions, DSTCs},
        {?memory_sessions, MemTCs}
    ].

merge_custom_groups(RootGroup, GroupTCs, CustomMatrix0) ->
    CustomMatrix =
        lists:flatmap(
            fun
                ({G, _, SubGroup}) when G == RootGroup ->
                    SubGroup;
                (_) ->
                    []
            end,
            CustomMatrix0
        ),
    CustomMatrix ++ GroupTCs.

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?durable_sessions, TCConfig) ->
    ExtraApps = [
        emqx_conf,
        {emqx_retainer, #{
            config => #{<<"retainer">> => #{<<"enable">> => true}}
        }},
        {emqx_a2a_registry, #{config => #{<<"a2a_registry">> => #{<<"enable">> => true}}}}
    ],
    emqx_common_test_helpers:start_apps_ds(TCConfig, ExtraApps, #{});
init_per_group(?memory_sessions, TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx_retainer, #{
                config => #{<<"retainer">> => #{<<"enable">> => true}}
            }},
            {emqx_a2a_registry, #{config => #{<<"a2a_registry">> => #{<<"enable">> => true}}}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?durable_sessions, TCConfig) ->
    emqx_common_test_helpers:run_cleanups(TCConfig),
    ok;
end_per_group(?memory_sessions, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_a2a_registry_cth:clear_all_cards(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

card_count() ->
    emqx_a2a_registry_cth:card_count().

sample_card_bin() ->
    emqx_utils_json:encode(emqx_a2a_registry_cth:sample_card()).

start_client(Overrides) ->
    emqx_a2a_registry_cth:start_client(Overrides).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

discovery_topic(OrgId, UnitId, AgentId) ->
    emqx_a2a_registry:discovery_topic(OrgId, UnitId, AgentId).

agent_clientid(OrgId, UnitId, AgentId) ->
    emqx_a2a_registry_cth:agent_clientid(OrgId, UnitId, AgentId).

publish_card(C, OrgId, UnitId, AgentId, Card) ->
    emqtt:publish(
        C,
        discovery_topic(OrgId, UnitId, AgentId),
        Card,
        [{retain, true}, {qos, 1}]
    ).

update_config(Path, Value, ValueToRestore) ->
    on_exit(fun() ->
        {ok, _} = emqx:update_config(Path, ValueToRestore, #{override_to => cluster})
    end),
    {ok, _} = emqx:update_config(Path, Value, #{override_to => cluster}),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Simple smoke test that explores the happy path of a2a registry.

  - A card is registered by an agent client.
    - The card passes validations.
  - Another MQTT client connects and subscribes to discovery topic filter.
    - It receives the card with augmented properties indicating the agent to be online.
  - The agent goes offline, the second client re-subscribes.
    - It receives the card with augmented properties indicating the agent to be offline.
""".
t_smoke_01(_TCConfig) ->
    AgentClientId = agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Agent = start_client(#{clientid => AgentClientId}),
    {ok, _} = publish_card(Agent, ?ORG_ID, ?UNIT_ID, ?AGENT_ID, sample_card_bin()),

    C = start_client(#{}),
    {ok, _, _} = emqtt:subscribe(C, discovery_topic(<<"+">>, <<"+">>, <<"+">>), [{qos, 1}]),
    %% At first, the agent is connected
    ?assertReceive(
        {publish, #{
            properties := #{'User-Property' := [{?A2A_PROP_STATUS_KEY, ?A2A_PROP_ONLINE_VAL}]}
        }}
    ),

    %% Then, the agent disconnects and we re-subscribe
    emqtt:stop(Agent),
    ct:sleep(300),
    {ok, _, _} = emqtt:subscribe(C, discovery_topic(<<"+">>, <<"+">>, <<"+">>), [{qos, 1}]),
    ?assertReceive(
        {publish, #{
            properties := #{'User-Property' := [{?A2A_PROP_STATUS_KEY, ?A2A_PROP_OFFLINE_VAL}]}
        }}
    ),

    emqtt:stop(C),

    ok.

-doc """
Checks that we validate that the publishing clientid matches the id extracted from the
discovery topic.

Clientid must be `{org_id}/{unit_id}/{agent_id}`, matching the last 3 segments of the
discovery topic.

Also, each of these segments must match the `^[A-Za-z0-9._-]+$` regex.
""".
t_clientid_topic_id_mismatch() ->
    [{matrix, true}].
t_clientid_topic_id_mismatch(matrix) ->
    [[?memory_sessions]];
t_clientid_topic_id_mismatch(_TCConfig) ->
    AgentClientId = agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Agent = start_client(#{clientid => AgentClientId}),

    %% If any segment mismatches, it's invalid and not stored.
    lists:foreach(
        fun({OrgId, UnitId, AgentId}) ->
            ?assertMatch(
                {{ok, _}, {ok, _}},
                ?wait_async_action(
                    publish_card(Agent, OrgId, UnitId, AgentId, sample_card_bin()),
                    #{?snk_kind := "a2a_registry_invalid_card_message"},
                    1_000
                ),
                #{id => {OrgId, UnitId, AgentId}}
            ),
            ?assertEqual(0, card_count())
        end,
        [
            {OrgId, UnitId, AgentId}
         || OrgId <- [?ORG_ID, <<"other_org_id">>],
            UnitId <- [?UNIT_ID, <<"other_unit_id">>],
            AgentId <- [?AGENT_ID, <<"other_agent_id">>]
        ] -- [{?ORG_ID, ?UNIT_ID, ?AGENT_ID}]
    ),

    %% Any segments that do not match the allowed regex must be denied.
    InvalidSegmentIds = [
        <<"">>,
        <<" ">>,
        <<"ç"/utf8>>,
        <<"🫠"/utf8>>
    ],
    lists:foreach(
        fun({OrgId, UnitId, AgentId}) ->
            Agent2 = start_client(#{clientid => agent_clientid(OrgId, UnitId, AgentId)}),
            ?assertMatch(
                {{ok, _}, {ok, _}},
                ?wait_async_action(
                    publish_card(Agent2, OrgId, UnitId, AgentId, sample_card_bin()),
                    #{?snk_kind := "a2a_registry_invalid_card_message"},
                    1_000
                ),
                #{id => {OrgId, UnitId, AgentId}}
            ),
            ?assertEqual(0, card_count()),
            emqtt:stop(Agent2)
        end,
        [
            {OrgId, UnitId, AgentId}
         || OrgId <- [?ORG_ID | InvalidSegmentIds],
            UnitId <- [?UNIT_ID | InvalidSegmentIds],
            AgentId <- [?AGENT_ID | InvalidSegmentIds]
        ] -- [{?ORG_ID, ?UNIT_ID, ?AGENT_ID}]
    ),

    ok.

-doc """
Checks that we validate the card payload against the schema.
""".
t_validate_schema() ->
    [{matrix, true}].
t_validate_schema(matrix) ->
    [[?memory_sessions]];
t_validate_schema(_TCConfig) ->
    Agent = start_client(#{clientid => agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID)}),
    %% The card schema has required fields, hence an empty object should fail.
    BadCard = emqx_utils_json:encode(#{}),
    ?assertMatch(
        {{ok, _}, {ok, _}},
        ?wait_async_action(
            publish_card(Agent, ?ORG_ID, ?UNIT_ID, ?AGENT_ID, BadCard),
            #{?snk_kind := "a2a_registry_invalid_card_message"},
            1_000
        )
    ),
    ?assertEqual(0, card_count()),
    %% If we disable schema validation, any payload should be accepted
    update_config([a2a_registry, validate_schema], false, _ValueToRestore = true),
    {ok, _} = publish_card(Agent, ?ORG_ID, ?UNIT_ID, ?AGENT_ID, BadCard),
    ?assertEqual(1, card_count()),
    emqtt:stop(Agent),
    ok.

-doc """
Checks that we don't run any hooks when the feature is disabled.
""".
t_registry_disabled(_TCConfig) ->
    update_config([a2a_registry, enable], false, _ValueToRestore = true),

    Agent = start_client(#{clientid => agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID)}),
    %% Payload is invalid, clientid does not match; would be blocked, if feature was
    %% enabled.
    {ok, _} = publish_card(Agent, <<"a">>, <<"b">>, <<"c">>, <<"not a card">>),

    %% There's no liveness check info when feature is disabled.
    C = start_client(#{}),
    {ok, _, _} = emqtt:subscribe(C, discovery_topic(<<"+">>, <<"+">>, <<"+">>), [{qos, 1}]),
    ?assertReceive(
        {publish, #{
            properties := #{} = Props
        }} when not is_map_key('User-Property', Props)
    ),
    emqtt:stop(Agent),
    {ok, _, _} = emqtt:subscribe(C, discovery_topic(<<"+">>, <<"+">>, <<"+">>), [{qos, 1}]),
    ?assertReceive(
        {publish, #{
            properties := #{} = Props
        }} when not is_map_key('User-Property', Props)
    ),
    emqtt:stop(C),

    ok.
