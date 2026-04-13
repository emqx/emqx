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
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ORG_ID, <<"org_id">>).
-define(UNIT_ID, <<"unit_id">>).
-define(AGENT_ID, <<"agent_id">>).
-define(ORG_ID2, <<"org_id2">>).
-define(UNIT_ID2, <<"unit_id2">>).
-define(AGENT_ID2, <<"agent_id2">>).
-define(NS, <<"some_ns">>).

-define(ns, ns).

-define(no_namespace, no_namespace).
-define(namespaced, namespaced).

-define(CAPTURE(Expr),
    (fun() ->
        case emqx_common_test_helpers:capture_io_format(fun() -> Expr end) of
            {ok, ___LINES} ->
                {ok, lists:map(fun emqx_utils_json:decode/1, ___LINES)};
            {false, ___LINES} ->
                {false,
                    lists:map(
                        fun(___X) ->
                            case emqx_utils_json:safe_decode(___X) of
                                {ok, ___Y} -> ___Y;
                                _ -> ___X
                            end
                        end,
                        ___LINES
                    )};
            ___X ->
                ___X
        end
    end)()
).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?no_namespace},
        {group, ?namespaced}
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
    GlobalTCs = emqx_common_test_helpers:merge_custom_groups(?no_namespace, AllTCs, CustomMatrix),
    NsTCs = emqx_common_test_helpers:merge_custom_groups(?namespaced, AllTCs, CustomMatrix),
    [
        {?no_namespace, GlobalTCs},
        {?namespaced, NsTCs}
    ].

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

init_per_group(?namespaced, TCConfig) ->
    ok = emqx_hooks:add(
        'namespace.resource_pre_create',
        {?MODULE, on_namespace_resource_pre_create, []},
        ?HP_HIGHEST
    ),
    [{?ns, ?NS} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?namespaced, _TCConfig) ->
    ok = emqx_hooks:del(
        'namespace.resource_pre_create',
        {?MODULE, on_namespace_resource_pre_create}
    ),
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

start_client(Overrides) ->
    emqx_a2a_registry_cth:start_client(Overrides).

sample_card_bin() ->
    sample_card_bin(_Overrides = #{}).

sample_card_bin(Overrides) ->
    Card = emqx_utils_maps:deep_merge(emqx_a2a_registry_cth:sample_card(), Overrides),
    emqx_utils_json:encode(Card).

write_card(OrgId, UnitId, AgentId, Card, TCConfig) ->
    Namespace = get_config(?ns, TCConfig, ?global_ns),
    Topic = emqx_a2a_registry:discovery_topic(Namespace, OrgId, UnitId, AgentId),
    ClientId = emqx_a2a_registry_cth:agent_clientid(OrgId, UnitId, AgentId),
    QoS = 1,
    Headers = #{},
    Msg = emqx_message:make(ClientId, QoS, Topic, Card, #{retain => true}, Headers),
    emqx_retainer:with_backend(fun(Mod, State) ->
        Mod:store_retained(State, Msg)
    end).

simple_write_card(OrgId, UnitId, AgentId, TCConfig) ->
    Name = emqx_a2a_registry_cth:agent_clientid(OrgId, UnitId, AgentId),
    Card = sample_card_bin(#{<<"name">> => Name}),
    write_card(OrgId, UnitId, AgentId, Card, TCConfig).

list_cards(Args0, TCConfig) ->
    Args1 = lists:map(fun emqx_utils_conv:str/1, Args0),
    Args = maybe_add_ns(Args1, TCConfig),
    emqx_a2a_registry_cli:a2a(["list" | Args]).

get_card(Args0, TCConfig) ->
    Args1 = lists:map(fun emqx_utils_conv:str/1, Args0),
    Args = maybe_add_ns(Args1, TCConfig),
    emqx_a2a_registry_cli:a2a(["get" | Args]).

delete_card(Args0, TCConfig) ->
    Args1 = lists:map(fun emqx_utils_conv:str/1, Args0),
    Args = maybe_add_ns(Args1, TCConfig),
    emqx_a2a_registry_cli:a2a(["delete" | Args]).

register_card(Args0, TCConfig) ->
    Args1 = lists:map(fun emqx_utils_conv:str/1, Args0),
    Args = maybe_add_ns(Args1, TCConfig),
    emqx_a2a_registry_cli:a2a(["register" | Args]).

card_stats(TCConfig) ->
    Args = maybe_add_ns([], TCConfig),
    emqx_a2a_registry_cli:a2a(["stats" | Args]).

maybe_add_ns(Args, TCConfig) ->
    case get_config(?ns, TCConfig, ?global_ns) of
        ?global_ns ->
            Args;
        Namespace when is_binary(Namespace) ->
            ["--namespace", binary_to_list(Namespace) | Args]
    end.

card_count(TCConfig) ->
    Namespace = get_config(?ns, TCConfig, ?global_ns),
    emqx_a2a_registry_cth:card_count(#{namespace => Namespace}).

all_cards(TCConfig) ->
    Namespace = get_config(?ns, TCConfig, ?global_ns),
    emqx_a2a_registry_cth:all_cards(#{namespace => Namespace}).

on_namespace_resource_pre_create(#{namespace := _Namespace}, ResCtx) ->
    Res = persistent_term:get({?MODULE, namespace_exists}, true),
    {stop, ResCtx#{exists := Res}}.

set_namespace_existence_check_result(Res) when is_boolean(Res) ->
    on_exit(fun() -> persistent_term:erase({?MODULE, namespace_exists}) end),
    persistent_term:put({?MODULE, namespace_exists}, Res).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Smoke test for calling usage (doesn't crash, basically)
t_usage(_TCConfig) ->
    _ = emqx_a2a_registry_cli:a2a([]),
    ok.

%% Smoke test for calling list
t_list_cards(TCConfig) ->
    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig),
    simple_write_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2, TCConfig),
    Id1 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Id2 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),

    {ok, Out1} = ?CAPTURE(list_cards([], TCConfig)),
    ?assertMatch(
        [
            [
                #{<<"name">> := Id1},
                #{<<"name">> := Id2}
            ]
        ],
        lists:sort(Out1)
    ),

    ?assertMatch(
        {ok, [[#{<<"name">> := Id2}]]},
        ?CAPTURE(list_cards(["--org-id", ?ORG_ID2], TCConfig))
    ),
    ?assertMatch(
        {ok, [[#{<<"name">> := Id1}]]},
        ?CAPTURE(list_cards(["--unit-id", ?UNIT_ID], TCConfig))
    ),
    ?assertMatch(
        {ok, [[#{<<"name">> := Id1}]]},
        ?CAPTURE(list_cards(["--agent-id", ?AGENT_ID], TCConfig))
    ),
    ?assertMatch(
        {ok, [[#{<<"name">> := Id1}]]},
        ?CAPTURE(
            list_cards(
                [
                    "--org-id",
                    ?ORG_ID,
                    "--unit-id",
                    ?UNIT_ID,
                    "--agent-id",
                    ?AGENT_ID
                ],
                TCConfig
            )
        )
    ),
    ?assertMatch(
        {ok, [[]]},
        ?CAPTURE(
            list_cards(
                [
                    "--org-id",
                    ?ORG_ID,
                    "--unit-id",
                    ?UNIT_ID,
                    "--agent-id",
                    ?AGENT_ID2
                ],
                TCConfig
            )
        )
    ),

    ?assertMatch(
        {ok, [[_, _]]},
        ?CAPTURE(list_cards(["--status", "offline"], TCConfig))
    ),
    C = start_client(#{clientid => Id1}),
    ?assertMatch(
        {ok, [[#{<<"name">> := Id2}]]},
        ?CAPTURE(list_cards(["--status", "offline"], TCConfig))
    ),
    ?assertMatch(
        {ok, [[#{<<"name">> := Id1}]]},
        ?CAPTURE(list_cards(["--status", "online"], TCConfig))
    ),
    emqtt:stop(C),

    %% invalid args
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--status", "invalid"], TCConfig))),
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--org-id", "/"], TCConfig))),
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--unit-id", "/"], TCConfig))),
    ?assertMatch({false, _}, ?CAPTURE(list_cards(["--agent-id", "/"], TCConfig))),

    ok.

%% Smoke test for calling get
t_get_card(TCConfig) ->
    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig),
    simple_write_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2, TCConfig),
    Id1 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Id2 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),

    ?assertMatch(
        {ok, [
            #{
                <<"name">> := Id1,
                %% Raw JSON card
                <<"raw">> := <<"{", _/binary>>
            }
        ]},
        ?CAPTURE(get_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfig))
    ),
    {ok, [#{<<"raw">> := RawCard1}]} = ?CAPTURE(get_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfig)),
    Name1 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Card1 = sample_card_bin(#{<<"name">> => Name1}),
    ?assertEqual(emqx_utils_json:decode(Card1), emqx_utils_json:decode(RawCard1)),
    ?assertMatch(
        {ok, [
            #{
                <<"name">> := Id2,
                %% Raw JSON card
                <<"raw">> := <<"{", _/binary>>
            }
        ]},
        ?CAPTURE(get_card([?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2], TCConfig))
    ),
    ?assertMatch(
        {ok, [null]},
        ?CAPTURE(get_card([<<"foo">>, ?UNIT_ID, ?AGENT_ID], TCConfig))
    ),

    ok.

%% Smoke test for calling delete
t_delete_card(TCConfig) ->
    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig),
    simple_write_card(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2, TCConfig),
    Id2 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID2, ?UNIT_ID2, ?AGENT_ID2),

    ?assertEqual(2, card_count(TCConfig)),

    ?assertMatch(ok, delete_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfig)),
    ?assertEqual(1, card_count(TCConfig)),

    %% Idempotency; also, deleting inexistent card
    ?assertMatch(ok, delete_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfig)),
    ?assertEqual(1, card_count(TCConfig)),

    ?assertMatch(
        [#{<<"name">> := Id2}],
        all_cards(TCConfig)
    ),

    ok.

%% Smoke test for calling register
t_register_card(TCConfig) ->
    ?assertEqual(0, card_count(TCConfig)),

    PrivDir = get_config(priv_dir, TCConfig),
    Name = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    Card = sample_card_bin(#{<<"name">> => Name}),
    Filepath = filename:join([PrivDir, ?FUNCTION_NAME, "agent-card.json"]),
    ok = filelib:ensure_dir(Filepath),
    ok = file:write_file(Filepath, Card),

    ?assertMatch(ok, register_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID, Filepath], TCConfig)),
    ?assertEqual(1, card_count(TCConfig)),
    Id = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),
    ?assertMatch(
        [#{<<"name">> := Id}],
        all_cards(TCConfig)
    ),

    %% Invalid ids
    ?assertMatch(false, register_card(["/", ?UNIT_ID, ?AGENT_ID, Filepath], TCConfig)),
    ?assertMatch(false, register_card([?ORG_ID, "/", ?AGENT_ID, Filepath], TCConfig)),
    ?assertMatch(false, register_card([?ORG_ID, ?UNIT_ID, "/", Filepath], TCConfig)),

    %% Invalid file path
    ?assertMatch(
        false, register_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID, "i-dont-exist.json"], TCConfig)
    ),

    ok.

%% Smoke test for calling stats
t_card_stats(TCConfig) ->
    ?assertEqual(0, card_count(TCConfig)),
    ?assertMatch(
        {ok, [#{<<"total">> := 0}]},
        ?CAPTURE(card_stats(TCConfig))
    ),

    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig),
    ?assertMatch(
        {ok, [#{<<"total">> := 1}]},
        ?CAPTURE(card_stats(TCConfig))
    ),

    ok.

-doc """
Verifies that operations on cards in a namespace do not affect those in a different one.
""".
t_namespace_isolation() ->
    [{matrix, true}].
t_namespace_isolation(matrix) ->
    [[?namespaced]];
t_namespace_isolation(TCConfig) when is_list(TCConfig) ->
    TCConfigGlobal = [{?ns, ?global_ns} | TCConfig],

    %% Creating a card in namespace
    simple_write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, TCConfig),
    Id1 = emqx_a2a_registry_cth:agent_clientid(?ORG_ID, ?UNIT_ID, ?AGENT_ID),

    %% No cards in global namespace
    ?assertMatch(
        {ok, [#{<<"total">> := 0}]},
        ?CAPTURE(card_stats(TCConfigGlobal))
    ),
    ?assertMatch(
        {ok, [null]},
        ?CAPTURE(get_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfigGlobal))
    ),
    ?assertMatch(
        {ok, [[]]},
        ?CAPTURE(list_cards([], TCConfigGlobal))
    ),
    %% Create a card with the same clientid in global ns.  It should not be visible in the
    %% namespace.
    Card = sample_card_bin(#{<<"name">> => <<"global card">>}),
    write_card(?ORG_ID, ?UNIT_ID, ?AGENT_ID, Card, TCConfigGlobal),
    ?assertMatch(
        {ok, [#{<<"name">> := <<"global card">>}]},
        ?CAPTURE(get_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfigGlobal))
    ),
    ?assertMatch(
        {ok, [#{<<"name">> := Id1}]},
        ?CAPTURE(get_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfig))
    ),
    ?assertMatch(
        {ok, [[#{<<"name">> := <<"global card">>}]]},
        ?CAPTURE(list_cards([], TCConfigGlobal))
    ),
    ?assertMatch(
        {ok, [[#{<<"name">> := Id1}]]},
        ?CAPTURE(list_cards([], TCConfig))
    ),

    %% Deletion is isolated too.
    ?assertMatch({ok, [#{<<"total">> := 1}]}, ?CAPTURE(card_stats(TCConfigGlobal))),
    ?assertMatch({ok, [#{<<"total">> := 1}]}, ?CAPTURE(card_stats(TCConfig))),
    ?assertMatch(ok, delete_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID], TCConfigGlobal)),
    ?assertMatch({ok, [#{<<"total">> := 0}]}, ?CAPTURE(card_stats(TCConfigGlobal))),
    ?assertMatch({ok, [#{<<"total">> := 1}]}, ?CAPTURE(card_stats(TCConfig))),

    %% We check for namespace existence when registering
    set_namespace_existence_check_result(false),
    PrivDir = get_config(priv_dir, TCConfig),
    Card1 = sample_card_bin(#{<<"name">> => Id1}),
    Filepath = filename:join([PrivDir, ?FUNCTION_NAME, "agent-card.json"]),
    ok = filelib:ensure_dir(Filepath),
    ok = file:write_file(Filepath, Card1),

    ?assertMatch(
        {false, [#{<<"message">> := <<"Namespace not found:", _/binary>>}]},
        ?CAPTURE(register_card([?ORG_ID, ?UNIT_ID, ?AGENT_ID, Filepath], TCConfig))
    ),

    ok.
