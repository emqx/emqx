%%--------------------------------------------------------------------
% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rocketmq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% Bridge defaults
-define(TOPIC, "TopicTest").
-define(DENY_TOPIC, "DENY_TOPIC").
-define(ACCESS_KEY, "RocketMQ").
-define(SECRET_KEY, "12345678").
-define(BATCH_SIZE, 10).
-define(PAYLOAD, <<"HELLO">>).

-define(GET_CONFIG(KEY__, CFG__), proplists:get_value(KEY__, CFG__)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, async},
        {group, sync},
        {group, acl}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE) -- [t_acl_deny],
    BatchingGroups = [{group, with_batch}, {group, without_batch}],
    [
        {async, BatchingGroups},
        {sync, BatchingGroups},
        {with_batch, TCs},
        {without_batch, TCs},
        {acl, [t_acl_deny]}
    ].

init_per_group(async, Config) ->
    [{query_mode, async} | Config];
init_per_group(sync, Config) ->
    [{query_mode, sync} | Config];
init_per_group(with_batch, Config0) ->
    Config = [{batch_size, ?BATCH_SIZE} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{batch_size, 1} | Config0],
    common_init(Config);
init_per_group(acl, Config0) ->
    Config = [{batch_size, 1}, {query_mode, sync} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    Apps = ?config(apps, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    delete_bridge(Config),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = snabbkaffe:stop(),
    delete_bridge(Config),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(ConfigT) ->
    BridgeType = <<"rocketmq">>,
    Host = os:getenv("ROCKETMQ_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("ROCKETMQ_PORT", "9876")),

    Config0 = [
        {host, Host},
        {port, Port},
        {proxy_name, "rocketmq"}
        | ConfigT
    ],

    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge_rocketmq,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config0)}
            ),
            {Name, RocketMQConf} = rocketmq_config(BridgeType, Config0),
            RocketMQSSLConf = rocketmq_ssl_config(RocketMQConf, Config0),
            Config =
                [
                    {apps, Apps},
                    {rocketmq_config, RocketMQConf},
                    {rocketmq_config_ssl, RocketMQSSLConf},
                    {rocketmq_bridge_type, BridgeType},
                    {rocketmq_name, Name},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                false ->
                    {skip, no_rocketmq};
                _ ->
                    throw(no_rocketmq)
            end
    end.

rocketmq_ssl_config(NonSSLConfig, _TCConfig) ->
    %% TODO: generate fixed files for server and client to actually test TLS...
    %% DataDir = ?config(data_dir, TCConfig),
    %% emqx_test_tls_certs_helper:generate_tls_certs(TCConfig),
    %% Keyfile = filename:join([DataDir, "client1.key"]),
    %% Certfile = filename:join([DataDir, "client1.pem"]),
    %% CACertfile = filename:join([DataDir, "intermediate1.pem"]),
    NonSSLConfig#{
        <<"servers">> => <<"rocketmq_namesrv_ssl:9876">>,
        <<"ssl">> => #{
            %% <<"keyfile">> => iolist_to_binary(Keyfile),
            %% <<"certfile">> => iolist_to_binary(Certfile),
            %% <<"cacertfile">> => iolist_to_binary(CACertfile),
            <<"enable">> => true,
            <<"verify">> => <<"verify_none">>
        }
    }.

rocketmq_config(BridgeType, Config) ->
    Port = integer_to_list(?GET_CONFIG(port, Config)),
    Server = ?GET_CONFIG(host, Config) ++ ":" ++ Port,
    Name = atom_to_binary(?MODULE),
    BatchSize = ?config(batch_size, Config),
    QueryMode = ?config(query_mode, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  servers = ~p\n"
            "  access_key = ~p\n"
            "  secret_key = ~p\n"
            "  topic = ~p\n"
            "  resource_opts = {\n"
            "    request_ttl = 1500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = ~s\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                ?ACCESS_KEY,
                ?SECRET_KEY,
                ?TOPIC,
                BatchSize,
                QueryMode
            ]
        ),
    {Name, emqx_bridge_testlib:parse_and_check(BridgeType, Name, ConfigString)}.

create_bridge(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config, Config),
    emqx_bridge:create(BridgeType, Name, RocketMQConf).

create_bridge_ssl(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config_ssl, Config),
    emqx_bridge_testlib:create_bridge_api([
        {bridge_type, BridgeType},
        {bridge_name, Name},
        {bridge_config, RocketMQConf}
    ]).

create_bridge_ssl_bad_ssl_opts(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf0 = ?GET_CONFIG(rocketmq_config_ssl, Config),
    %% This config is wrong because we use verify_peer without
    %% a cert that can be used in the verification.
    RocketMQConf1 = maps:put(
        <<"ssl">>,
        #{
            <<"enable">> => true,
            <<"verify">> => verify_peer
        },
        RocketMQConf0
    ),
    emqx_bridge:create(BridgeType, Name, RocketMQConf1).

delete_bridge(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ActionId = emqx_bridge_v2:id(BridgeType, Name),
    emqx_bridge_v2:query(BridgeType, Name, {ActionId, Payload}, #{}).

query_resource(Config, Request) ->
    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ID = emqx_bridge_v2:id(BridgeType, Name),
    ResID = emqx_connector_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ID, Request, #{timeout => 500, connector_resource_id => ResID}).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_setup_via_config_and_publish_ssl(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge_ssl(Config)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

%% Check that we can not connect to the SSL only RocketMQ instance
%% with incorrect SSL options
t_setup_via_config_ssl_host_bad_ssl_opts(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge_ssl_bad_ssl_opts(Config)
    ),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceID)),
    ?assertMatch(#{status := disconnected}, emqx_bridge_v2:health_check(BridgeType, Name)),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config, Config),
    RocketMQConf2 = RocketMQConf#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(RocketMQConf2)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_setup_two_actions_via_http_api_and_publish(Config) ->
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    RocketMQConf = ?GET_CONFIG(rocketmq_config, Config),
    RocketMQConf2 = RocketMQConf#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(RocketMQConf2)
    ),
    {ok, #{raw_config := ActionConf}} = emqx_bridge_v2:lookup(actions, BridgeType, Name),
    Topic2 = <<"Topic2">>,
    ActionConf2 = emqx_utils_maps:deep_force_put(
        [<<"parameters">>, <<"topic">>], ActionConf, Topic2
    ),
    Action2Name = atom_to_binary(?FUNCTION_NAME),
    {ok, _} = emqx_bridge_v2:create(BridgeType, Action2Name, ActionConf2),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    Config2 = proplists:delete(rocketmq_name, Config),
    Config3 = [{rocketmq_name, Action2Name} | Config2],
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config3, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),

    Name = ?GET_CONFIG(rocketmq_name, Config),
    BridgeType = ?GET_CONFIG(rocketmq_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID)),
    ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(BridgeType, Name)),
    ok.

t_simple_query(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Type = ?GET_CONFIG(rocketmq_bridge_type, Config),
    Name = ?GET_CONFIG(rocketmq_name, Config),
    ActionId = emqx_bridge_v2:id(Type, Name),
    Request = {ActionId, #{message => <<"Hello">>}},
    Result = query_resource(Config, Request),
    ?assertEqual(ok, Result),
    ok.

t_acl_deny(Config0) ->
    RocketCfg = ?GET_CONFIG(rocketmq_config, Config0),
    RocketCfg2 = RocketCfg#{<<"topic">> := ?DENY_TOPIC},
    Config = lists:keyreplace(rocketmq_config, 1, Config0, {rocketmq_config, RocketCfg2}),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    SentData = #{payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertMatch({error, #{<<"code">> := 1}}, send_message(Config, SentData)),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{error := #{<<"code">> := 1}}], Trace),
            ok
        end
    ),
    ok.
