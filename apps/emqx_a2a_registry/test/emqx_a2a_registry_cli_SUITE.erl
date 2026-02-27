%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_cli_SUITE).

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

%% -import(emqx_common_test_helpers, [on_exit/1]).

-define(ORG_ID, <<"org_id">>).
-define(UNIT_ID, <<"unit_id">>).
-define(AGENT_ID, <<"agent_id">>).
-define(ORG_ID2, <<"org_id2">>).
-define(UNIT_ID2, <<"unit_id2">>).
-define(AGENT_ID2, <<"agent_id2">>).

-define(CAPTURE(Expr), emqx_common_test_helpers:capture_io_format(fun() -> Expr end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
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

start_client(Overrides) ->
    emqx_a2a_registry_cth:start_client(Overrides).

sample_card_bin() ->
    sample_card_bin(_Overrides = #{}).

sample_card_bin(Overrides) ->
    Card = emqx_utils_maps:deep_merge(emqx_a2a_registry_cth:sample_card(), Overrides),
    emqx_utils_json:encode(Card).

write_card(OrgId, UnitId, AgentId, Card) ->
    Topic = emqx_a2a_registry:discovery_topic(OrgId, UnitId, AgentId),
    ClientId = emqx_a2a_registry_cth:agent_clientid(OrgId, UnitId, AgentId),
    QoS = 1,
    Headers = #{},
    Msg = emqx_message:make(ClientId, QoS, Topic, Card, #{retain => true}, Headers),
    emqx_retainer:with_backend(fun(Mod, State) ->
        Mod:store_retained(State, Msg)
    end).

simple_write_card(OrgId, UnitId, AgentId) ->
    Name = emqx_a2a_registry_cth:agent_clientid(OrgId, UnitId, AgentId),
    Card = sample_card_bin(#{<<"name">> => Name}),
    write_card(OrgId, UnitId, AgentId, Card).

list_cards(Args0) ->
    Args = lists:map(fun emqx_utils_conv:str/1, Args0),
    emqx_a2a_registry_cli:a2a(["list" | Args]).

get_card(Args0) ->
    Args = lists:map(fun emqx_utils_conv:str/1, Args0),
    emqx_a2a_registry_cli:a2a(["get" | Args]).

delete_card(Args0) ->
    Args = lists:map(fun emqx_utils_conv:str/1, Args0),
    emqx_a2a_registry_cli:a2a(["delete" | Args]).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Smoke test for calling usage (doesn't crash, basically)
t_usage(_TCConfig) ->
    _ = emqx_a2a_registry_cli:a2a([]),
    ok.

%% Smoke test for calling list
t_list_cards(_TCConfig) ->
    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    simple_write_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),
    Id1 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Id2 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),
    SId1 = byte_size(Id1),
    SId2 = byte_size(Id2),

    {ok, Out1} = ?CAPTURE(list_cards([])),
    ?assertMatch(
        [
            <<"Name: ", Id1:SId1/binary, _/binary>>,
            <<"Name: ", Id2:SId2/binary, _/binary>>
        ],
        lists:sort(Out1)
    ),

    ?assertMatch(
        {ok, [<<"Name: ", Id2:SId2/binary, _/binary>>]},
        ?CAPTURE(list_cards(["--org-id", ?ORG_ID2]))
    ),
    ?assertMatch(
        {ok, [<<"Name: ", Id1:SId1/binary, _/binary>>]},
        ?CAPTURE(list_cards(["--unit-id", ?UNIT_ID]))
    ),
    ?assertMatch(
        {ok, [<<"Name: ", Id1:SId1/binary, _/binary>>]},
        ?CAPTURE(list_cards(["--agent-id", ?AGENT_ID]))
    ),
    ?assertMatch(
        {ok, [<<"Name: ", Id1:SId1/binary, _/binary>>]},
        ?CAPTURE(
            list_cards([
                "--org-id",
                ?ORG_ID,
                "--unit-id",
                ?UNIT_ID,
                "--agent-id",
                ?AGENT_ID
            ])
        )
    ),
    ?assertMatch(
        {ok, []},
        ?CAPTURE(
            list_cards([
                "--org-id",
                ?ORG_ID,
                "--unit-id",
                ?UNIT_ID,
                "--agent-id",
                ?AGENT_ID2
            ])
        )
    ),

    ?assertMatch(
        {ok, [_, _]},
        ?CAPTURE(list_cards(["--status", "offline"]))
    ),
    C = start_client(#{clientid => Id1}),
    ?assertMatch(
        {ok, [<<"Name: ", Id2:SId2/binary, _/binary>>]},
        ?CAPTURE(list_cards(["--status", "offline"]))
    ),
    ?assertMatch(
        {ok, [<<"Name: ", Id1:SId1/binary, _/binary>>]},
        ?CAPTURE(list_cards(["--status", "online"]))
    ),
    emqtt:stop(C),

    %% invalid args
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--status", "invalid"]))),
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--org-id", "/"]))),
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--unit-id", "/"]))),
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--agent-id", "/"]))),

    ok.

%% Smoke test for calling get
t_get_card(_TCConfig) ->
    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    simple_write_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),
    Id1 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Id2 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),
    SId1 = byte_size(Id1),
    SId2 = byte_size(Id2),

    ?assertMatch(
        {ok, [
            <<"Name: ", Id1:SId1/binary, _/binary>>
        ]},
        ?CAPTURE(get_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID]))
    ),
    ?assertMatch(
        {ok, [
            <<"Name: ", Id2:SId2/binary, _/binary>>
        ]},
        ?CAPTURE(get_card([?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2]))
    ),
    ?assertMatch(
        {ok, [<<"Not found", _/binary>>]},
        ?CAPTURE(get_card([<<"foo">>, ?UNIT_ID, ?AGENT_ID]))
    ),

    ok.

%% Smoke test for calling delete
t_delete_card(_TCConfig) ->
    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    simple_write_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),
    Id2 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),

    ?assertEqual(2, emqx_a2a_registry_cth:card_count()),

    ?assertMatch(ok, delete_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID])),
    ?assertEqual(1, emqx_a2a_registry_cth:card_count()),

    %% Idempotency; also, deleting inexistent card
    ?assertMatch(ok, delete_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID])),
    ?assertEqual(1, emqx_a2a_registry_cth:card_count()),

    ?assertMatch(
        [#{<<"name">> := Id2}],
        emqx_a2a_registry_cth:all_cards()
    ),

    ok.
