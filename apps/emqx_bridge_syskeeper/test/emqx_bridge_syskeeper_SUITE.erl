%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_syskeeper_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HOST, "127.0.0.1").
-define(PORT, 9092).
-define(ACK_TIMEOUT, 2000).
-define(HANDSHAKE_TIMEOUT, 10000).
-define(SYSKEEPER_NAME, <<"syskeeper">>).
-define(SYSKEEPER_PROXY_NAME, <<"syskeeper_proxy">>).
-define(BATCH_SIZE, 3).
-define(TOPIC, <<"syskeeper/message">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, lifecycle},
        {group, need_ack},
        {group, no_ack}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    Lifecycle = [
        t_setup_proxy_via_config,
        t_setup_proxy_via_http_api,
        t_setup_forwarder_via_config,
        t_setup_forwarder_via_http_api,
        t_get_status,
        t_list_v1_bridges_forwarder
    ],
    Write = TCs -- Lifecycle,
    BatchingGroups = [{group, with_batch}, {group, without_batch}],
    [
        {need_ack, BatchingGroups},
        {no_ack, BatchingGroups},
        {with_batch, Write},
        {without_batch, Write},
        {lifecycle, Lifecycle}
    ].

init_per_group(need_ack, Config) ->
    [{ack_mode, need_ack} | Config];
init_per_group(no_ack, Config) ->
    [{ack_mode, no_ack} | Config];
init_per_group(with_batch, Config0) ->
    [{enable_batch, true} | Config0];
init_per_group(without_batch, Config0) ->
    [{enable_batch, false} | Config0];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_connector,
            emqx_bridge_syskeeper,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _Api} = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Testcase, Config) ->
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok = snabbkaffe:stop(),
    delete_bridge(syskeeper_forwarder, ?SYSKEEPER_NAME),
    delete_connectors(syskeeper_forwarder, ?SYSKEEPER_NAME),
    delete_connectors(syskeeper_proxy, ?SYSKEEPER_PROXY_NAME),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------
syskeeper_config(Config) ->
    BatchSize =
        case proplists:get_value(enable_batch, Config, false) of
            true -> ?BATCH_SIZE;
            false -> 1
        end,
    ConfigString =
        io_lib:format(
            "actions.~s.~s {\n"
            "  enable = true\n"
            "  connector = ~ts\n"
            "  parameters = {\n"
            "    target_topic = \"${topic}\"\n"
            "    template = \"${payload}\"\n"
            "  },\n"
            "  resource_opts = {\n"
            "    request_ttl = 500ms\n"
            "    batch_size = ~b\n"
            "  }\n"
            "}",
            [
                syskeeper_forwarder,
                ?SYSKEEPER_NAME,
                ?SYSKEEPER_NAME,
                BatchSize
            ]
        ),
    {?SYSKEEPER_NAME, parse_bridge_and_check(ConfigString, syskeeper_forwarder, ?SYSKEEPER_NAME)}.

syskeeper_connector_config(Config) ->
    AckMode = proplists:get_value(ack_mode, Config, no_ack),
    ConfigString =
        io_lib:format(
            "connectors.~s.~s {\n"
            "  enable = true\n"
            "  server = \"~ts\"\n"
            "  ack_mode = ~p\n"
            "  ack_timeout = ~p\n"
            "  pool_size = 1\n"
            "}",
            [
                syskeeper_forwarder,
                ?SYSKEEPER_NAME,
                server(),
                AckMode,
                ?ACK_TIMEOUT
            ]
        ),
    {?SYSKEEPER_NAME,
        parse_connectors_and_check(ConfigString, syskeeper_forwarder, ?SYSKEEPER_NAME)}.

syskeeper_proxy_config(_Config) ->
    ConfigString =
        io_lib:format(
            "connectors.~s.~s {\n"
            "  enable = true\n"
            "  listen = \"~ts\"\n"
            "  acceptors = 1\n"
            "  handshake_timeout = ~p\n"
            "}",
            [
                syskeeper_proxy,
                ?SYSKEEPER_PROXY_NAME,
                server(),
                ?HANDSHAKE_TIMEOUT
            ]
        ),
    {?SYSKEEPER_PROXY_NAME,
        parse_connectors_and_check(ConfigString, syskeeper_proxy, ?SYSKEEPER_PROXY_NAME)}.

parse_and_check(ConfigString, SchemaMod, RootKey, Type0, Name) ->
    Type = to_bin(Type0),
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(SchemaMod, RawConf, #{required => false, atom_key => false}),
    #{RootKey := #{Type := #{Name := Config}}} = RawConf,
    Config.

parse_bridge_and_check(ConfigString, BridgeType, Name) ->
    parse_and_check(ConfigString, emqx_bridge_schema, <<"actions">>, BridgeType, Name).

parse_connectors_and_check(ConfigString, ConnectorType, Name) ->
    Config = parse_and_check(
        ConfigString, emqx_connector_schema, <<"connectors">>, ConnectorType, Name
    ),
    emqx_utils_maps:safe_atom_key_map(Config).

create_bridge(Type, Name, Conf) ->
    emqx_bridge_v2:create(Type, Name, Conf).

delete_bridge(Type, Name) ->
    emqx_bridge_v2:remove(Type, Name).

create_both_bridges(Config) ->
    {ProxyName, ProxyConf} = syskeeper_proxy_config(Config),
    {ConnectorName, ConnectorConf} = syskeeper_connector_config(Config),
    {Name, Conf} = syskeeper_config(Config),
    ?assertMatch(
        {ok, _},
        create_connectors(syskeeper_proxy, ProxyName, ProxyConf)
    ),
    timer:sleep(1000),
    ?assertMatch(
        {ok, _},
        create_connectors(syskeeper_forwarder, ConnectorName, ConnectorConf)
    ),
    timer:sleep(1000),
    ?assertMatch({ok, _}, create_bridge(syskeeper_forwarder, Name, Conf)),
    Name.

create_bridge_http(Params) ->
    call_create_http("actions", Params).

create_connectors_http(Params) ->
    call_create_http("connectors", Params).

call_create_http(Root, Params) ->
    Path = emqx_mgmt_api_test_util:api_path([Root]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

create_connectors(Type, Name, Conf) ->
    emqx_connector:create(Type, Name, Conf).

delete_connectors(Type, Name) ->
    emqx_connector:remove(Type, Name).

send_message(_Config, Payload) ->
    Name = ?SYSKEEPER_NAME,
    BridgeType = syskeeper_forwarder,
    emqx_bridge_v2:send_message(BridgeType, Name, Payload, #{}).

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8);
to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom);
to_bin(Bin) when is_binary(Bin) ->
    Bin.

to_str(Atom) when is_atom(Atom) ->
    erlang:atom_to_list(Atom).

server() ->
    erlang:iolist_to_binary(io_lib:format("~ts:~B", [?HOST, ?PORT])).

make_message() ->
    Message = emqx_message:make(?MODULE, ?TOPIC, ?SYSKEEPER_NAME),
    Id = emqx_guid:to_hexstr(emqx_guid:gen()),
    From = emqx_message:from(Message),
    Msg = emqx_message:to_map(Message),
    Msg#{id => Id, clientid => From}.

receive_msg() ->
    receive
        {deliver, ?TOPIC, Msg} ->
            {ok, Msg}
    after 500 ->
        {error, no_message}
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------
t_setup_proxy_via_config(Config) ->
    {Name, Conf} = syskeeper_proxy_config(Config),
    ?assertMatch(
        {ok, _},
        create_connectors(syskeeper_proxy, Name, Conf)
    ),
    ?assertMatch(
        X when is_pid(X),
        esockd:listener({emqx_bridge_syskeeper_proxy_server, {?HOST, ?PORT}})
    ),
    delete_connectors(syskeeper_proxy, Name),
    ?assertError(
        not_found,
        esockd:listener({emqx_bridge_syskeeper_proxy_server, {?HOST, ?PORT}})
    ).

t_setup_proxy_via_http_api(Config) ->
    {Name, ProxyConf0} = syskeeper_proxy_config(Config),
    ProxyConf = ProxyConf0#{
        <<"name">> => Name,
        <<"type">> => syskeeper_proxy
    },
    ?assertMatch(
        {ok, _},
        create_connectors_http(ProxyConf)
    ),

    ?assertMatch(
        X when is_pid(X),
        esockd:listener({emqx_bridge_syskeeper_proxy_server, {?HOST, ?PORT}})
    ),

    delete_connectors(syskeeper_proxy, Name),

    ?assertError(
        not_found,
        esockd:listener({emqx_bridge_syskeeper_proxy_server, {?HOST, ?PORT}})
    ).

t_setup_forwarder_via_config(Config) ->
    {ConnectorName, ConnectorConf} = syskeeper_connector_config(Config),
    {Name, Conf} = syskeeper_config(Config),
    ?assertMatch(
        {ok, _},
        create_connectors(syskeeper_forwarder, ConnectorName, ConnectorConf)
    ),
    ?assertMatch({ok, _}, create_bridge(syskeeper_forwarder, Name, Conf)).

t_setup_forwarder_via_http_api(Config) ->
    {ConnectorName, ConnectorConf0} = syskeeper_connector_config(Config),
    {Name, Conf0} = syskeeper_config(Config),

    ConnectorConf = ConnectorConf0#{
        <<"name">> => ConnectorName,
        <<"type">> => syskeeper_forwarder
    },

    Conf = Conf0#{
        <<"name">> => Name,
        <<"type">> => syskeeper_forwarder
    },

    ?assertMatch(
        {ok, _},
        create_connectors_http(ConnectorConf)
    ),

    ?assertMatch(
        {ok, _},
        create_bridge_http(Conf)
    ).

t_get_status(Config) ->
    create_both_bridges(Config),
    ?assertMatch(
        #{status := connected}, emqx_bridge_v2:health_check(syskeeper_forwarder, ?SYSKEEPER_NAME)
    ),
    delete_connectors(syskeeper_proxy, ?SYSKEEPER_PROXY_NAME),
    ?retry(
        _Sleep = 500,
        _Attempts = 10,
        ?assertMatch(
            #{status := disconnected},
            emqx_bridge_v2:health_check(syskeeper_forwarder, ?SYSKEEPER_NAME)
        )
    ).

t_write_failure(Config) ->
    create_both_bridges(Config),
    delete_connectors(syskeeper_proxy, ?SYSKEEPER_PROXY_NAME),
    SentData = make_message(),
    Result =
        ?wait_async_action(
            send_message(Config, SentData),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    ?assertMatch({{error, {resource_error, _}}, _}, Result).

t_invalid_data(Config) ->
    create_both_bridges(Config),
    {_, {ok, #{result := Result}}} =
        ?wait_async_action(
            send_message(Config, #{}),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    ?assertMatch({error, {unrecoverable_error, {invalid_data, _}}}, Result).

t_forward(Config) ->
    emqx_broker:subscribe(?TOPIC),
    create_both_bridges(Config),
    SentData = make_message(),
    {_, {ok, #{result := _Result}}} =
        ?wait_async_action(
            send_message(Config, SentData),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    ?retry(
        500,
        10,
        ?assertMatch({ok, _}, receive_msg())
    ),
    emqx_broker:unsubscribe(?TOPIC),
    ok.

t_list_v1_bridges_forwarder(Config) ->
    ?check_trace(
        begin
            Name = create_both_bridges(Config),

            ?assertMatch(
                {ok, {{_, 200, _}, _, []}}, emqx_bridge_v2_testlib:list_bridges_http_api_v1()
            ),
            ?assertMatch(
                {ok, {{_, 200, _}, _, [_]}}, emqx_bridge_v2_testlib:list_actions_http_api()
            ),
            ?assertMatch(
                {ok, {{_, 200, _}, _, [_, _]}}, emqx_bridge_v2_testlib:list_connectors_http_api()
            ),

            RuleTopic = <<"t/c">>,
            {ok, #{<<"id">> := RuleId0}} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    <<"syskeeper_forwarder">>,
                    RuleTopic,
                    [{bridge_name, Name} | Config],
                    #{overrides => #{enable => true}}
                ),
            ?assert(emqx_bridge_v2_testlib:is_rule_enabled(RuleId0)),
            ?assertMatch(
                {ok, {{_, 200, _}, _, _}}, emqx_bridge_v2_testlib:enable_rule_http(RuleId0)
            ),
            ?assert(emqx_bridge_v2_testlib:is_rule_enabled(RuleId0)),

            ?assertMatch(
                {error, no_v1_equivalent},
                emqx_action_info:bridge_v1_type_name(syskeeper_forwarder)
            ),

            ok
        end,
        []
    ),
    ok.
