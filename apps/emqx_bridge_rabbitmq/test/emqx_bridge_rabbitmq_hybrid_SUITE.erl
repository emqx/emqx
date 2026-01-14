%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_hybrid_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, rabbitmq).
-define(CONNECTOR_TYPE_BIN, <<"rabbitmq">>).
-define(ACTION_TYPE, rabbitmq).
-define(ACTION_TYPE_BIN, <<"rabbitmq">>).
-define(SOURCE_TYPE, rabbitmq).
-define(SOURCE_TYPE_BIN, <<"rabbitmq">>).

-define(USER, <<"guest">>).
-define(PASSWORD, <<"guest">>).
-define(EXCHANGE, <<"messages">>).
-define(QUEUE, <<"test_queue">>).
-define(ROUTING_KEY, <<"test_routing_key">>).

-define(tcp, tcp).
-define(tls, tls).
-define(without_batch, without_batch).
-define(with_batch, with_batch).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_rabbitmq,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(?tcp, TCConfig) ->
    [
        {host, <<"rabbitmq">>},
        {port, 5672},
        {enable_tls, false}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {host, <<"rabbitmq">>},
        {port, 5671},
        {enable_tls, true}
        | TCConfig
    ];
init_per_group(?with_batch, TCConfig) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig];
init_per_group(?without_batch, TCConfig) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    SSL =
        case get_config(enable_tls, TCConfig, false) of
            true ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_testlib:ssl_options(true)
                );
            false ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_testlib:ssl_options(false)
                )
        end,
    ConnectorConfig = connector_config(#{
        <<"server">> => get_config(host, TCConfig, <<"rabbitmq">>),
        <<"port">> => get_config(port, TCConfig, 5672),
        <<"ssl">> => SSL
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    SourceName = ConnectorName,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName
    }),
    Client = emqx_bridge_rabbitmq_testlib:connect_and_setup_exchange_and_queue(#{
        host => get_config(host, TCConfig, <<"rabbitmq">>),
        port => get_config(port, TCConfig, 5672),
        use_tls => get_config(enable_tls, TCConfig, false),
        exchange => ?EXCHANGE,
        queue => ?QUEUE,
        routing_key => ?ROUTING_KEY
    }),
    snabbkaffe:start_trace(),
    [
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig},
        {client, Client}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    Client = get_config(client, TCConfig),
    emqx_bridge_rabbitmq_testlib:cleanup_client_and_queue(Client),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:connector_config(Overrides).

action_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:action_config(Overrides).

source_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:source_config(Overrides).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
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

receive_message(TCConfig) ->
    Client = get_config(client, TCConfig),
    emqx_bridge_rabbitmq_testlib:receive_message(Client).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api([{bridge_kind, action} | TCConfig], Overrides)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api([{bridge_kind, source} | TCConfig], Overrides).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

delete_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(Kind, Type, Name).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

prepare_conf_file(Name, Content, TCConfig) ->
    emqx_config_SUITE:prepare_conf_file(Name, Content, TCConfig).

payload() ->
    payload(_Overrides = #{}).

payload(Overrides) ->
    maps:merge(
        #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
        Overrides
    ).

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_replace_action_source(TCConfig) ->
    ConnectorName = get_config(connector_name, TCConfig),
    ActionConfig = get_config(action_config, TCConfig),
    SourceConfig = get_config(source_config, TCConfig),
    ConnectorConfig = get_config(connector_config, TCConfig),
    Action = #{<<"rabbitmq">> => #{<<"my_action">> => ActionConfig}},
    Source = #{<<"rabbitmq">> => #{<<"my_source">> => SourceConfig}},
    Connector = #{<<"rabbitmq">> => #{ConnectorName => ConnectorConfig}},
    Rabbitmq = #{
        <<"actions">> => Action,
        <<"sources">> => Source,
        <<"connectors">> => Connector
    },
    ConfBin0 = hocon_pp:do(Rabbitmq, #{}),
    ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, TCConfig),
    ?assertMatch(ok, emqx_conf_cli:conf(["load", "--replace", ConfFile0])),
    ?assertMatch(
        #{<<"rabbitmq">> := #{<<"my_action">> := _}},
        emqx_config:get_raw([<<"actions">>]),
        Action
    ),
    ?assertMatch(
        #{<<"rabbitmq">> := #{<<"my_source">> := _}},
        emqx_config:get_raw([<<"sources">>]),
        Source
    ),
    ?assertMatch(
        #{<<"rabbitmq">> := #{ConnectorName := _}},
        emqx_config:get_raw([<<"connectors">>]),
        Connector
    ),

    Empty = #{
        <<"actions">> => #{},
        <<"sources">> => #{},
        <<"connectors">> => #{}
    },
    ConfBin1 = hocon_pp:do(Empty, #{}),
    ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, TCConfig),
    ?assertMatch(ok, emqx_conf_cli:conf(["load", "--replace", ConfFile1])),

    ?assertEqual(#{}, emqx_config:get_raw([<<"actions">>])),
    ?assertEqual(#{}, emqx_config:get_raw([<<"sources">>])),
    ?assertMatch(#{}, emqx_config:get_raw([<<"connectors">>])),

    %% restore connectors
    Rabbitmq2 = #{<<"connectors">> => Connector},
    ConfBin2 = hocon_pp:do(Rabbitmq2, #{}),
    ConfFile2 = prepare_conf_file(?FUNCTION_NAME, ConfBin2, TCConfig),
    ?assertMatch(ok, emqx_conf_cli:conf(["load", "--replace", ConfFile2])),
    ?assertMatch(
        #{<<"rabbitmq">> := #{ConnectorName := _}},
        emqx_config:get_raw([<<"connectors">>]),
        Connector
    ),
    ok.
