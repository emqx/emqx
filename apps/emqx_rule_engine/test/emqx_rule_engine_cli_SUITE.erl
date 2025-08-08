%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Definitions
%%------------------------------------------------------------------------------

-define(RULE_ID, <<"somerule">>).

-define(CAPTURE(Expr), emqx_common_test_helpers:capture_io_format(fun() -> Expr end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                before_start =>
                    fun(App, AppOpts) ->
                        %% We need this in the tests because `emqx_cth_suite` does not start apps in
                        %% the exact same way as the release works: in the release,
                        %% `emqx_enterprise_schema` is the root schema that knows all root keys.  In
                        %% CTH, we need to manually load the schema below so that when
                        %% `emqx_config:init_load` runs and encounters a namespaced root key, it knows
                        %% the schema module for it.
                        Mods = [
                            emqx_rule_engine_schema,
                            emqx_bridge_v2_schema,
                            emqx_connector_schema
                        ],
                        lists:foreach(
                            fun(Mod) ->
                                emqx_config:init_load(Mod, <<"">>)
                            end,
                            Mods
                        ),
                        ok = emqx_schema_hooks:inject_from_modules(Mods),
                        emqx_cth_suite:inhibit_config_loader(App, AppOpts)
                    end
            }},
            emqx_conf,
            emqx_mt,
            emqx_bridge_mqtt,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

cli_conf(?global_ns, Cmd, Args) ->
    emqx_conf_cli:conf([Cmd | Args]);
cli_conf(Namespace, Cmd, Args) when is_binary(Namespace) ->
    emqx_conf_cli:conf([Cmd, "--namespace", str(Namespace) | Args]).

cli_rules(?global_ns, Cmd, Args) ->
    emqx_rule_engine_cli:cmd([Cmd | Args]);
cli_rules(Namespace, Cmd, Args) when is_binary(Namespace) ->
    emqx_rule_engine_cli:cmd([Cmd, "--namespace", str(Namespace) | Args]).

prepare_conf_file(Name, Content, TCConfig) when is_binary(Content) ->
    emqx_config_SUITE:prepare_conf_file(Name, Content, TCConfig);
prepare_conf_file(Name, RawConf, TCConfig) ->
    HoconBin = hocon_pp:do(RawConf, #{}),
    emqx_config_SUITE:prepare_conf_file(Name, HoconBin, TCConfig).

str(X) -> emqx_utils_conv:str(X).

get_raw_config(Namespace, KeyPath, Default) when is_binary(Namespace) ->
    emqx:get_raw_namespaced_config(Namespace, KeyPath, Default);
get_raw_config(?global_ns, KeyPath, Default) ->
    emqx:get_raw_config(KeyPath, Default).

raw_config_namespace_resources_scenario1() ->
    ConnectorType = <<"mqtt">>,
    ConnectorName = <<"conn">>,
    ActionName = <<"channel1">>,
    SourceName = <<"channel2">>,
    ActionType = <<"mqtt">>,
    SourceType = <<"mqtt">>,
    ActionId = emqx_bridge_resource:bridge_id(ActionType, ActionName),
    SourceId = emqx_bridge_resource:bridge_id(SourceType, SourceName),
    SourceHookPoint = emqx_bridge_v2:source_hookpoint(SourceId),
    #{
        <<"connectors">> => #{
            ConnectorType => #{
                ConnectorName =>
                    maps:without(
                        [<<"name">>, <<"type">>],
                        emqx_bridge_v2_api_SUITE:source_connector_create_config(#{
                            <<"description">> => <<"connector ns1">>,
                            <<"pool_size">> => 1,
                            <<"server">> => <<"127.0.0.1:1883">>
                        })
                    )
            }
        },
        <<"actions">> => #{
            ActionType => #{
                ActionName => emqx_bridge_v2_api_SUITE:source_update_config(#{
                    <<"description">> => <<"action ns1">>,
                    <<"connector">> => ConnectorName
                })
            }
        },
        <<"sources">> => #{
            SourceType => #{
                SourceName => emqx_bridge_v2_api_SUITE:source_update_config(#{
                    <<"description">> => <<"source ns1">>,
                    <<"connector">> => ConnectorName
                })
            }
        },
        <<"rule_engine">> => #{
            <<"rules">> => #{
                ?RULE_ID => emqx_rule_engine_api_2_SUITE:rule_config(#{
                    <<"name">> => <<"rule_name">>,
                    <<"description">> => <<"rule ns1">>,
                    <<"sql">> => fmt(<<"select * from \"${t}\" ">>, #{t => SourceHookPoint}),
                    <<"actions">> => [ActionId]
                })
            }
        }
    }.

fmt(Template, Context) -> emqx_bridge_v2_testlib:fmt(Template, Context).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Smoke tests for using `emqx ctl conf load|show|remove ...` with namespaced configurations.

This test is placed here instead of `emqx_conf_cli_SUITE` because it requires some allowed
namespaced configs to be defined.

""".
t_namespaced_load_config(TCConfig) when is_list(TCConfig) ->
    Namespace = <<"ns1">>,

    %% Bad option: just `--namespace` in place of a key.
    ?assertMatch(
        {{error, "bad arguments:" ++ _}, _},
        ?CAPTURE(emqx_conf_cli:conf(["load", "--namespace"]))
    ),
    ?assertMatch(
        {{error, "bad arguments:" ++ _}, _},
        ?CAPTURE(emqx_conf_cli:conf(["show", "--namespace"]))
    ),

    %% Trying to import a root key that is not allowed for namespaced configs should fail.
    %% Currently, `config_backup_interval` is one such key.
    FileWithForbiddenRoot =
        prepare_conf_file(
            ?FUNCTION_NAME,
            #{<<"config_backup_interval">> => <<"10m">>},
            TCConfig
        ),
    Res1 = cli_conf(Namespace, "load", [FileWithForbiddenRoot]),
    ?assertMatch({error, _}, Res1),
    {error, Msg1} = Res1,
    ?assertMatch(match, re:run(Msg1, <<"root_key_not_namespaced">>, [{capture, none}])),

    %% No configs to show yet.
    ?assertMatch({ok, [<<"\n">>]}, ?CAPTURE(cli_conf(Namespace, "show", []))),

    %% Load valid config
    OkConfig1 = raw_config_namespace_resources_scenario1(),
    OkConfigFile1 = prepare_conf_file("ok1.hocon", OkConfig1, TCConfig),
    ?assertMatch(
        {ok, [
            <<"loading config for namespace", _/binary>>,
            <<"load connectors on cluster ok\n">>,
            <<"load actions on cluster ok\n">>,
            <<"load sources on cluster ok\n">>,
            <<"load rule_engine on cluster ok\n">>
        ]},
        ?CAPTURE(cli_conf(Namespace, "load", [OkConfigFile1]))
    ),
    {ok, ShowOutput1} = ?CAPTURE(cli_conf(Namespace, "show", [])),
    ?assertMatch(
        {ok, #{
            <<"connectors">> := _,
            <<"actions">> := _,
            <<"sources">> := _,
            <<"rule_engine">> := _
        }},
        hocon:binary(ShowOutput1)
    ),

    RuleKeyPath = str(fmt(<<"rule_engine.rules.${id}">>, #{id => ?RULE_ID})),
    {ok, ShowOutput2} = ?CAPTURE(cli_conf(Namespace, "show", ["rule_engine"])),
    ?assertMatch(
        {ok, #{
            <<"rule_engine">> := #{
                <<"rules">> := #{?RULE_ID := _}
            }
        }},
        hocon:binary(ShowOutput2)
    ),

    %% Remove configs
    ?assertMatch(
        {ok, [<<"ok\n">>]},
        ?CAPTURE(cli_conf(Namespace, "remove", [RuleKeyPath]))
    ),
    {ok, ShowOutput3} = ?CAPTURE(cli_conf(Namespace, "show", ["rule_engine"])),
    EmptyMap = #{},
    ?assertMatch(
        {ok, #{
            <<"rule_engine">> := #{
                <<"rules">> := EmptyMap
            }
        }},
        hocon:binary(ShowOutput3)
    ),

    ok.

-doc """
Smoke tests for `emqx ctl rules list|show ...` when namespaces are involved.
""".
t_rule_engine_cli(TCConfig) when is_list(TCConfig) ->
    Namespace = <<"ns1">>,

    %% Bad option: just `--namespace` in place of a key.
    ?assertMatch(
        {{error, "bad arguments:" ++ _}, _},
        ?CAPTURE(emqx_rule_engine_cli:cmd(["list", "--namespace"]))
    ),
    ?assertMatch(
        {{error, "bad arguments:" ++ _}, _},
        ?CAPTURE(emqx_rule_engine_cli:cmd(["show", "--namespace"]))
    ),

    %% Nothing to show yet
    ?assertMatch(
        {ok, []},
        ?CAPTURE(cli_rules(Namespace, "list", []))
    ),
    ?assertMatch(
        {ok, []},
        ?CAPTURE(cli_rules(Namespace, "show", [?RULE_ID]))
    ),

    OkConfig0 = raw_config_namespace_resources_scenario1(),
    OkConfig = maps:with([<<"rule_engine">>], OkConfig0),
    OkConfigFile = prepare_conf_file("ok2.hocon", OkConfig, TCConfig),
    ?assertMatch(
        {ok, [
            <<"loading config for namespace", _/binary>>,
            <<"load rule_engine on cluster ok\n">>
        ]},
        ?CAPTURE(cli_conf(Namespace, "load", [OkConfigFile]))
    ),

    ?assertMatch(
        {ok, [<<"Rule{id=somerule, name=rule_name, enabled=true, descr=rule ns1}\n">>]},
        ?CAPTURE(cli_rules(Namespace, "list", []))
    ),
    ?assertMatch(
        {ok, [<<"Id:", _/binary>>]},
        ?CAPTURE(cli_rules(Namespace, "show", [?RULE_ID]))
    ),

    ok.
