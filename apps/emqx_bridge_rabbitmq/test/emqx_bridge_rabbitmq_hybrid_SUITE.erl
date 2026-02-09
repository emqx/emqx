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

-define(tpal(MSG, ARGS), begin
    ct:pal(lists:flatten(io_lib:format(MSG, ARGS))),
    ?tp(notice, lists:flatten(io_lib:format(MSG, ARGS)), #{})
end).

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
    ClientOpts = #{
        host => get_config(host, TCConfig, <<"rabbitmq">>),
        port => get_config(port, TCConfig, 5672),
        use_tls => get_config(enable_tls, TCConfig, false),
        exchange => ?EXCHANGE,
        queue => ?QUEUE,
        routing_key => ?ROUTING_KEY
    },
    emqx_bridge_rabbitmq_testlib:connect_and_setup_exchange_and_queue(ClientOpts),
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
        {client_opts, ClientOpts}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:cleanup_client_and_queue(ClientOpts),
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

get_source_api(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

delete_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(Kind, Type, Name).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

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

%% Attempts to list all rabbitmq connections processes (`amqp_connection:start`) without
%% relying on details about how it's started/managed by emqx.
list_rabbitmq_connection_processes() ->
    [
        Pid
     || Pid <- erlang:processes(),
        case proc_lib:initial_call(Pid) of
            {amqp_gen_connection, init, [_]} ->
                true;
            _ ->
                false
        end
    ].

%% Attempts to list all rabbitmq channel processes (`amqp_connection:open_channel`)
%% without relying on details about how it's started/managed by emqx.
list_rabbitmq_channel_processes() ->
    [
        Pid
     || Pid <- erlang:processes(),
        case proc_lib:initial_call(Pid) of
            {amqp_channel, init, [_]} ->
                true;
            _ ->
                false
        end
    ].

random(List) ->
    lists:nth(rand:uniform(length(List)), List).

get_common_values_action(TCConfig) ->
    emqx_bridge_v2_testlib:get_common_values([{bridge_kind, action} | TCConfig]).

get_common_values_source(TCConfig) ->
    emqx_bridge_v2_testlib:get_common_values([{bridge_kind, source} | TCConfig]).

%% Must be called after action and source are created.
snk_subscribe_to_health_checks(TCConfig) ->
    #{
        resource_namespace := Ns,
        name := ActionName
    } = get_common_values_action(TCConfig),
    #{name := SourceName} = get_common_values_source(TCConfig),
    {ok, {_ConnResId, ActionResId}} =
        emqx_bridge_v2:get_resource_ids(Ns, actions, ?ACTION_TYPE, ActionName),
    {ok, {_ConnResId, SourceResId}} =
        emqx_bridge_v2:get_resource_ids(Ns, sources, ?SOURCE_TYPE, SourceName),
    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "rabbitmq_on_get_status_enter0"})
    ),
    {ok, SRef2} = snabbkaffe:subscribe(
        ?match_event(#{
            ?snk_kind := "rabbitmq_on_get_channel_status_enter",
            channel_id := ActionResId
        })
    ),
    {ok, SRef3} = snabbkaffe:subscribe(
        ?match_event(#{
            ?snk_kind := "rabbitmq_on_get_channel_status_enter",
            channel_id := SourceResId
        })
    ),
    #{connector => SRef1, action => SRef2, source => SRef3}.

snk_receive_health_check_events(SRefs) ->
    maps:map(
        fun(_K, SRef) ->
            snabbkaffe:receive_events(SRef)
        end,
        SRefs
    ).

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

-doc """
Verifies that actions/sources/connectors recovers themselves when a rabbitmq
connection process crashes.
""".
t_connection_crash_recovery(TCConfig) ->
    test_crash_recovery(_CrashWhat = connection_pid, TCConfig).

-doc """
Verifies that actions/sources/connectors recovers themselves when a rabbitmq action
channel process crashes.
""".
t_action_channel_crash_recovery(TCConfig) ->
    test_crash_recovery(_CrashWhat = action_chan_pid, TCConfig).

-doc """
Verifies that actions/sources/connectors recovers themselves when a rabbitmq source
channel process crashes.
""".
t_source_channel_crash_recovery(TCConfig) ->
    test_crash_recovery(_CrashWhat = source_chan_pid, TCConfig).

-doc """
Verifies that actions/sources/connectors recovers themselves when a rabbitmq connection or
channel process crashes.
""".
test_crash_recovery(CrashWhat, TCConfig) ->
    %% Sanity check
    ?assertEqual([], list_rabbitmq_channel_processes()),
    ?assertEqual([], list_rabbitmq_connection_processes()),

    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    ConnectionPidsBefore = list_rabbitmq_connection_processes(),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    ActionChanPidsBefore = list_rabbitmq_channel_processes(),
    {201, #{<<"status">> := <<"connected">>}} = create_source_api(TCConfig, #{}),
    ChanPidsBefore = list_rabbitmq_channel_processes(),
    SourceChanPidsBefore = ChanPidsBefore -- ActionChanPidsBefore,
    ?assertMatch([_ | _], ChanPidsBefore),
    ?assertMatch([_ | _], ConnectionPidsBefore),

    ct:timetrap({seconds, 15}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            %% ∙ Connection/channel crashes and is detected by connector/channel health check.
            PidToKill =
                case CrashWhat of
                    connection_pid ->
                        random(ConnectionPidsBefore);
                    action_chan_pid ->
                        random(ActionChanPidsBefore);
                    source_chan_pid ->
                        random(SourceChanPidsBefore)
                end,
            ?force_ordering(
                #{?snk_kind := "rabbitmq_on_get_status_enter0"},
                #{?snk_kind := kill_connection, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := kill_connection, ?snk_span := {complete, _}},
                #{?snk_kind := "rabbitmq_on_get_status_enter1"}
            ),
            SRefs1 = snk_subscribe_to_health_checks(TCConfig),
            spawn_link(fun() ->
                ?tp_span(kill_connection, #{}, begin
                    ?tpal("killing pid ~p", [PidToKill]),
                    exit(PidToKill, boom),
                    %% Allow some time for exit signal to propagate to ecpool
                    %% worker (assuming it's monitoring the pid).  Otherwise,
                    %% assertion might succeed because (current) `gen_server` call
                    %% could fail due to the same exit signal, propagating to the
                    %% health check pid instead.
                    ct:sleep(100)
                end)
            end),
            ?assertMatch(
                #{
                    connector := {ok, _},
                    action := {ok, _},
                    source := {ok, _}
                },
                snk_receive_health_check_events(SRefs1)
            ),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_action_api(TCConfig)
                )
            ),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_source_api(TCConfig)
                )
            ),
            %%   ∙ Should be eventually restarted/recover.
            ?retry(
                200,
                10,
                ?assertEqual(
                    length(ConnectionPidsBefore),
                    length(list_rabbitmq_connection_processes())
                )
            ),
            ?retry(
                200,
                10,
                ?assertEqual(
                    length(ChanPidsBefore),
                    length(list_rabbitmq_channel_processes())
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_action_api(TCConfig)
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_source_api(TCConfig)
                )
            ),

            ok
        end,
        []
    ),
    ok.

-doc """
Asserts that we don't leak channel processes if, for whatever reason, creating a list of
channels fails midway.
""".
t_start_channel_no_leak(TCConfig) ->
    %% Sanity check
    ?assertEqual([], list_rabbitmq_channel_processes()),
    ?assertEqual([], list_rabbitmq_connection_processes()),

    {201, #{
        <<"status">> := <<"connected">>,
        <<"pool_size">> := PoolSize
    }} = create_connector_api(TCConfig, #{}),
    ct:timetrap({seconds, 5}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            ?inject_crash(
                #{?snk_kind := "rabbitmq_will_make_channel"},
                snabbkaffe_nemesis:periodic_crash(
                    _Period = PoolSize,
                    _DutyCycle = 0.5,
                    _Phase = 0
                )
            ),
            {201, #{<<"status">> := <<"disconnected">>}} = create_action_api(TCConfig, #{}),
            ?assertEqual(0, length(list_rabbitmq_channel_processes())),
            ok
        end,
        []
    ),
    ok.
