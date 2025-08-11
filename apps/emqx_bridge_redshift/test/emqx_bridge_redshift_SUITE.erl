%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_redshift_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_redshift.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

%% -import(emqx_common_test_helpers, [on_exit/1]).

-define(PROXY_NAME, "pgsql_tcp").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_redshift,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    HelperCfg = [
        {pgsql_host, "toxiproxy"},
        {pgsql_port, 5432},
        {enable_tls, false}
    ],
    emqx_bridge_pgsql_SUITE:connect_and_create_table(HelperCfg),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME},
        {helper_cfg, HelperCfg}
        | TCConfig
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:print(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    emqx_bridge_pgsql_SUITE:connect_and_clear_table(get_value(helper_cfg, TCConfig)),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    reset_proxy(),
    emqx_bridge_pgsql_SUITE:connect_and_clear_table(get_value(helper_cfg, TCConfig)),
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"database">> => <<"mqtt">>,
        <<"server">> => <<"toxiproxy:5432">>,
        <<"pool_size">> => 8,
        <<"username">> => <<"root">>,
        <<"password">> => <<"public">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"sql">> => <<
                "INSERT INTO mqtt_test(payload, arrived) "
                "VALUES (${payload}, TO_TIMESTAMP((${timestamp} :: bigint)/1000))"
            >>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

get_value(Key, TCConfig) ->
    emqx_bridge_v2_testlib:get_value(Key, TCConfig).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, postgres_stopped).

t_on_get_status(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config).

t_rule_action(Config) when is_list(Config) ->
    HelperCfg = get_value(helper_cfg, Config),
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?assertMatch(
            Payload,
            emqx_bridge_pgsql_SUITE:connect_and_get_payload(HelperCfg)
        )
    end,
    CheckTrace = fun(Trace) ->
        ?assertMatch(
            [#{config := #{codecs := []}} | _],
            ?of_kind("starting_postgresql_connector", Trace)
        ),
        ?assertMatch(
            [#{opts := _} | _],
            ?of_kind("postgres_epgsql_connect", Trace)
        ),
        [#{opts := ConnOpts} | _] = ?of_kind("postgres_epgsql_connect", Trace),
        ?assertEqual([], proplists:get_value(codecs, ConnOpts, undefined))
    end,
    Opts = #{
        post_publish_fn => PostPublishFn,
        trace_checkers => [CheckTrace]
    },
    emqx_bridge_v2_testlib:t_rule_action(Config, Opts).
