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

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ORG_ID, <<"org_id">>).
-define(UNIT_ID, <<"unit_id">>).
-define(AGENT_ID, <<"agent_id">>).
-define(ORG_ID2, <<"org_id2">>).
-define(UNIT_ID2, <<"unit_id2">>).
-define(AGENT_ID2, <<"agent_id2">>).
-define(NS, <<"some_ns">>).

-define(no_namespace, no_namespace).
-define(namespaced, namespaced).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx_retainer, #{
                config => #{<<"retainer">> => #{<<"enable">> => true}}
            }},
            {emqx_a2a_registry, #{config => #{<<"a2a_registry">> => #{<<"enable">> => true}}}},
            emqx_mt,
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

init_per_group(?namespaced, TCConfig) ->
    _ = emqx_mt_config:create_managed_ns(?NS),
    AuthHeader = ensure_namespaced_api_key(?NS),
    [{auth_header, AuthHeader}, {namespaced, true} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?namespaced, _TCConfig) ->
    ok;
end_per_group(_Group, _TCConfig) ->
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
get_config(K, TCConfig, Default) -> emqx_bridge_v2_testlib:get_value(K, TCConfig, Default).

sample_card_bin(Overrides) ->
    Card = emqx_utils_maps:deep_merge(emqx_a2a_registry_cth:sample_card(), Overrides),
    emqx_utils_json:encode(Card).

ensure_namespaced_api_key(Namespace) when is_binary(Namespace) ->
    emqx_bridge_v2_testlib:ensure_namespaced_api_key(#{namespace => Namespace}).

simple_request(Params) ->
    emqx_mgmt_api_test_util:simple_request(Params).

with_auth_header(Opts, TCConfig) ->
    case get_config(auth_header, TCConfig, undefined) of
        undefined ->
            Opts;
        AuthHeader ->
            Opts#{auth_header => AuthHeader}
    end.

list_cards(QueryParams, TCConfig) ->
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "list"]),
    simple_request(
        with_auth_header(#{method => get, url => URL, query_params => QueryParams}, TCConfig)
    ).

get_card(OrgId, UnitId, AgentId, TCConfig) ->
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "card", OrgId, UnitId, AgentId]),
    simple_request(with_auth_header(#{method => get, url => URL}, TCConfig)).

delete_card(OrgId, UnitId, AgentId, TCConfig) ->
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "card", OrgId, UnitId, AgentId]),
    simple_request(with_auth_header(#{method => delete, url => URL}, TCConfig)).

register_card(OrgId0, UnitId0, AgentId0, Card, TCConfig) ->
    [OrgId, UnitId, AgentId] = lists:map(fun uri_string:quote/1, [OrgId0, UnitId0, AgentId0]),
    URL = emqx_mgmt_api_test_util:api_path(["a2a", "cards", "card", OrgId, UnitId, AgentId]),
    Body = #{<<"card">> => Card},
    simple_request(with_auth_header(#{method => post, url => URL, body => Body}, TCConfig)).

update_config(Path, Value, ValueToRestore) ->
    on_exit(fun() ->
        {ok, _} = emqx:update_config(Path, ValueToRestore, #{override_to => cluster})
    end),
    {ok, _} = emqx:update_config(Path, Value, #{override_to => cluster}),
    ok.

get_config_api() ->
    URL = emqx_mgmt_api_test_util:api_path(["configs", "a2a_registry"]),
    simple_request(#{method => get, url => URL}).

update_config_api(Config) ->
    URL = emqx_mgmt_api_test_util:api_path(["configs", "a2a_registry"]),
    simple_request(#{method => put, body => Config, url => URL}).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
CRUD smoke tests.
""".
t_crud() ->
    [{matrix, true}].
t_crud(matrix) ->
    [[?no_namespace], [?namespaced]];
t_crud(TCConfig) when is_list(TCConfig) ->
    ?assertMatch({200, []}, list_cards(#{}, TCConfig)),
    ?assertMatch({404, _}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),
    ?assertMatch({204, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),

    Name1 = <<"1">>,
    Card1 = sample_card_bin(#{<<"name">> => Name1}),
    ?assertMatch({204, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, Card1, TCConfig)),
    ?assertMatch(
        {200, [
            #{
                <<"namespace">> := _,
                <<"id">> := _,
                <<"org_id">> := ?ORG_ID,
                <<"unit_id">> := ?UNIT_ID,
                <<"agent_id">> := ?AGENT_ID
            }
        ]},
        list_cards(#{}, TCConfig)
    ),
    ?assertMatch({200, #{<<"namespace">> := _}}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),

    %% List filters
    Name2 = <<"2">>,
    Card2 = sample_card_bin(#{<<"name">> => Name2}),
    ?assertMatch({204, _}, register_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2, Card2, TCConfig)),

    ?assertMatch({200, [_, _]}, list_cards(#{<<"name">> => <<"2">>}, TCConfig)),
    ?assertMatch({200, [#{<<"name">> := Name1}]}, list_cards(#{<<"org_id">> => ?ORG_ID}, TCConfig)),
    ?assertMatch(
        {200, [#{<<"name">> := Name2}]}, list_cards(#{<<"agent_id">> => ?AGENT_ID2}, TCConfig)
    ),
    ?assertMatch({200, []}, list_cards(#{<<"unit_id">> => <<"unknown">>}, TCConfig)),

    %% Namespaces are isolated.
    OtherNs = <<"another_ns">>,
    _ = emqx_mt_config:create_managed_ns(OtherNs),
    OtherAuthHeader = ensure_namespaced_api_key(OtherNs),
    TCConfigOtherNs = [{auth_header, OtherAuthHeader} | TCConfig],
    ?assertMatch({200, []}, list_cards(#{}, TCConfigOtherNs)),
    ?assertMatch({404, _}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfigOtherNs)),
    ?assertMatch({204, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfigOtherNs)),
    %% Other namespace is untouched
    ?assertMatch({200, [_, _]}, list_cards(#{}, TCConfig)),
    ?assertMatch({204, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, Card2, TCConfigOtherNs)),
    %% Global admin can manage cards from all namespaces
    maybe
        false ?= get_config(namespaced, TCConfig, false),
        ?assertMatch({200, [_, _, _]}, list_cards(#{}, TCConfig)),
        ?assertMatch({200, [_, _]}, list_cards(#{<<"only_global">> => true}, TCConfig)),
        ?assertMatch({200, [_]}, list_cards(#{<<"ns">> => OtherNs}, TCConfig))
    end,

    %% Delete
    ?assertMatch({204, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),
    ?assertMatch({204, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),
    ?assertMatch(
        {200, [#{<<"name">> := Name2}]}, list_cards(#{<<"only_global">> => true}, TCConfig)
    ),
    ?assertMatch({200, _}, get_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2, TCConfig)),
    ?assertMatch({404, _}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),

    %% Bad requests
    ?assertMatch({400, _}, register_card(<<"#">>, ?UNIT_ID, ?AGENT_ID, Card1, TCConfig)),
    ?assertMatch({400, _}, register_card(?ORG_ID, <<"+">>, ?AGENT_ID, Card1, TCConfig)),
    ?assertMatch({400, _}, register_card(?ORG_ID, ?UNIT_ID, <<"*">>, Card1, TCConfig)),
    ?assertMatch({400, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, <<"not a json">>, TCConfig)),

    %% When schema validation is disabled, we only ensure it's a valid JSON object.
    update_config([a2a_registry, validate_schema], false, _ValueToRestore = true),
    ?assertMatch({400, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, <<"not a json">>, TCConfig)),
    ?assertMatch({204, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, <<"{}">>, TCConfig)),

    ok.

-doc """
Verifies that we return 503 errors if the feature is disabled.
""".
t_disabled() ->
    [{matrix, true}].
t_disabled(matrix) ->
    [[?no_namespace], [?namespaced]];
t_disabled(TCConfig) ->
    update_config([a2a_registry, enable], false, _ValueToRestore = true),

    ?assertMatch({503, _}, list_cards(#{}, TCConfig)),
    ?assertMatch({503, _}, get_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),
    ?assertMatch({503, _}, delete_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig)),
    Name1 = <<"1">>,
    Card1 = sample_card_bin(#{<<"name">> => Name1}),
    ?assertMatch({503, _}, register_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, Card1, TCConfig)),

    ok.

-doc """
Simple smoke test to verify updating configurations via the default, generic config
management api.
""".
t_management_api(_TCConfig) ->
    {200, Config0} = get_config_api(),
    on_exit(fun() -> {200, _} = update_config_api(Config0) end),

    ?assertMatch(#{<<"enable">> := true, <<"validate_schema">> := true}, Config0),
    ?assert(emqx_a2a_registry_config:is_enabled()),
    ?assert(emqx_a2a_registry_config:is_schema_validation_enabled()),

    Config1 = Config0#{<<"enable">> := false, <<"validate_schema">> := false},
    ?assertMatch({200, Config1}, update_config_api(Config1)),
    ?assertNot(emqx_a2a_registry_config:is_enabled()),
    ?assertNot(emqx_a2a_registry_config:is_schema_validation_enabled()),

    ?assertMatch({200, Config0}, update_config_api(Config0)),
    ?assert(emqx_a2a_registry_config:is_enabled()),
    ?assert(emqx_a2a_registry_config:is_schema_validation_enabled()),

    ok.
