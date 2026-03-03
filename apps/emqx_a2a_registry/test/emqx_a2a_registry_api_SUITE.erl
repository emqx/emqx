%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-include("../src/emqx_a2a_registry_internal.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(ORG_ID, <<"org_id">>).
-define(UNIT_ID, <<"unit_id">>).
-define(AGENT_ID, <<"agent_id">>).
-define(ORG_ID2, <<"org_id2">>).
-define(UNIT_ID2, <<"unit_id2">>).
-define(AGENT_ID2, <<"agent_id2">>).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx_retainer, #{
                config => #{<<"retainer">> => #{<<"enable">> => true}}
            }},
            {emqx_a2a_registry, #{config => #{<<"a2a_registry">> => #{<<"enable">> => true}}}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    clear_all_cards(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

clear_all_cards() ->
    emqx_retainer:clean(),
    ok.

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

sample_card_bin(Overrides) ->
    Card = emqx_utils_maps:deep_merge(emqx_a2a_registry_cth:sample_card(), Overrides),
    emqx_utils_json:encode(Card).

simple_request(Params) ->
    emqx_mgmt_api_test_util:simple_request(Params).

list_cards(QueryParams) ->
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "list"]),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

get_card(OrgId, UnitId, AgentId) ->
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "card", OrgId, UnitId, AgentId]),
    simple_request(#{method => get, url => URL}).

delete_card(OrgId, UnitId, AgentId) ->
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "card", OrgId, UnitId, AgentId]),
    simple_request(#{method => delete, url => URL}).

register_card(OrgId0, UnitId0, AgentId0, Card) ->
    [OrgId, UnitId, AgentId] = lists:map(fun uri_string:quote/1, [OrgId0, UnitId0, AgentId0]),
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "card", OrgId, UnitId, AgentId]),
    Body = #{<<"card">> => Card},
    simple_request(#{method => post, url => URL, body => Body}).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
CRUD smoke tests.
""".
t_crud(_TCConfig) ->
    ?assertMatch({200, []}, list_cards(#{})),
    ?assertMatch({404, _}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID)),
    ?assertMatch({204, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID)),

    Name1 = <<"1">>,
    Card1 = sample_card_bin(#{<<"name">> => Name1}),
    ?assertMatch({204, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, Card1)),
    ?assertMatch({200, [_]}, list_cards(#{})),
    ?assertMatch({200, _}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID)),

    %% List filters
    Name2 = <<"2">>,
    Card2 = sample_card_bin(#{<<"name">> => Name2}),
    ?assertMatch({204, _}, register_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2, Card2)),

    ?assertMatch({200, [_, _]}, list_cards(#{<<"name">> => <<"2">>})),
    ?assertMatch({200, [#{<<"name">> := Name1}]}, list_cards(#{<<"org_id">> => ?ORG_ID})),
    ?assertMatch({200, [#{<<"name">> := Name2}]}, list_cards(#{<<"agent_id">> => ?AGENT_ID2})),
    ?assertMatch({200, []}, list_cards(#{<<"unit_id">> => <<"unknown">>})),

    %% Delete
    ?assertMatch({204, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID)),
    ?assertMatch({204, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID)),
    ?assertMatch({200, [#{<<"name">> := Name2}]}, list_cards(#{})),
    ?assertMatch({200, _}, get_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2)),
    ?assertMatch({404, _}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID)),

    %% Bad requests
    ?assertMatch({400, _}, register_card(<<"#">>, ?UNIT_ID, ?AGENT_ID, Card1)),
    ?assertMatch({400, _}, register_card(?ORG_ID, <<"+">>, ?AGENT_ID, Card1)),
    ?assertMatch({400, _}, register_card(?ORG_ID, ?UNIT_ID, <<"*">>, Card1)),
    ?assertMatch({400, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, <<"not a card">>)),

    ok.
