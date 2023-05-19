%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_opents_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% DB defaults
-define(BATCH_SIZE, 10).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, with_batch},
        {group, without_batch}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {with_batch, TCs},
        {without_batch, TCs}
    ].

init_per_group(with_batch, Config0) ->
    Config = [{batch_size, ?BATCH_SIZE} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{batch_size, 1} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_conf]),
    ok.

init_per_testcase(_Testcase, Config) ->
    delete_bridge(Config),
    snabbkaffe:start_trace(),
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
    Host = os:getenv("OPENTS_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("OPENTS_PORT", "4242")),

    Config0 = [
        {opents_host, Host},
        {opents_port, Port},
        {proxy_name, "opents"}
        | ConfigT
    ],

    BridgeType = proplists:get_value(bridge_type, Config0, <<"opents">>),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            % Ensure EE bridge module is loaded
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            {Name, OpenTSConf} = opents_config(BridgeType, Config0),
            Config =
                [
                    {opents_config, OpenTSConf},
                    {opents_bridge_type, BridgeType},
                    {opents_name, Name},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_opents);
                _ ->
                    {skip, no_opents}
            end
    end.

opents_config(BridgeType, Config) ->
    Port = integer_to_list(?config(opents_port, Config)),
    Server = "http://" ++ ?config(opents_host, Config) ++ ":" ++ Port,
    Name = atom_to_binary(?MODULE),
    BatchSize = ?config(batch_size, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  server = ~p\n"
            "  resource_opts = {\n"
            "    request_timeout = 500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = sync\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                BatchSize
            ]
        ),
    {Name, parse_and_check(ConfigString, BridgeType, Name)}.

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := Config}}} = RawConf,
    Config.

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    BridgeType = ?config(opents_bridge_type, Config),
    Name = ?config(opents_name, Config),
    Config0 = ?config(opents_config, Config),
    Config1 = emqx_utils_maps:deep_merge(Config0, Overrides),
    emqx_bridge:create(BridgeType, Name, Config1).

delete_bridge(Config) ->
    BridgeType = ?config(opents_bridge_type, Config),
    Name = ?config(opents_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(opents_name, Config),
    BridgeType = ?config(opents_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    emqx_bridge:send_message(BridgeID, Payload).

query_resource(Config, Request) ->
    query_resource(Config, Request, 1_000).

query_resource(Config, Request, Timeout) ->
    Name = ?config(opents_name, Config),
    BridgeType = ?config(opents_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ResourceID, Request, #{timeout => Timeout}).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    SentData = make_data(),
    ?check_trace(
        begin
            {_, {ok, #{result := Result}}} =
                ?wait_async_action(
                    send_message(Config, SentData),
                    #{?snk_kind := buffer_worker_flush_ack},
                    2_000
                ),
            ?assertMatch(
                {ok, 200, #{failed := 0, success := 1}}, Result
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(opents_connector_query_return, Trace0),
            ?assertMatch([#{result := {ok, 200, #{failed := 0, success := 1}}}], Trace),
            ok
        end
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?config(opents_bridge_type, Config),
    Name = ?config(opents_name, Config),
    OpentsConfig0 = ?config(opents_config, Config),
    OpentsConfig = OpentsConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(OpentsConfig)
    ),
    SentData = make_data(),
    ?check_trace(
        begin
            Request = {send_message, SentData},
            Res0 = query_resource(Config, Request, 2_500),
            ?assertMatch(
                {ok, 200, #{failed := 0, success := 1}}, Res0
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(opents_connector_query_return, Trace0),
            ?assertMatch([#{result := {ok, 200, #{failed := 0, success := 1}}}], Trace),
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),

    Name = ?config(opents_name, Config),
    BridgeType = ?config(opents_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID)),
    ok.

t_create_disconnected(Config) ->
    BridgeType = proplists:get_value(bridge_type, Config, <<"opents">>),
    Config1 = lists:keyreplace(opents_port, 1, Config, {opents_port, 61234}),
    {_Name, OpenTSConf} = opents_config(BridgeType, Config1),

    Config2 = lists:keyreplace(opents_config, 1, Config1, {opents_config, OpenTSConf}),
    ?assertMatch({ok, _}, create_bridge(Config2)),

    Name = ?config(opents_name, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceID)),
    ok.

t_write_failure(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    {ok, _} = create_bridge(Config),
    SentData = make_data(),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        {_, {ok, #{result := Result}}} =
            ?wait_async_action(
                send_message(Config, SentData),
                #{?snk_kind := buffer_worker_flush_ack},
                2_000
            ),
        ?assertMatch({error, _}, Result),
        ok
    end),
    ok.

t_write_timeout(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    {ok, _} = create_bridge(
        Config,
        #{
            <<"resource_opts">> => #{
                <<"request_timeout">> => 500,
                <<"resume_interval">> => 100,
                <<"health_check_interval">> => 100
            }
        }
    ),
    SentData = make_data(),
    emqx_common_test_helpers:with_failure(
        timeout, ProxyName, ProxyHost, ProxyPort, fun() ->
            ?assertMatch(
                {error, {resource_error, #{reason := timeout}}},
                query_resource(Config, {send_message, SentData})
            )
        end
    ),
    ok.

t_missing_data(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    {_, {ok, #{result := Result}}} =
        ?wait_async_action(
            send_message(Config, #{}),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    ?assertMatch(
        {error, {400, #{failed := 1, success := 0}}},
        Result
    ),
    ok.

t_bad_data(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Data = maps:without([metric], make_data()),
    {_, {ok, #{result := Result}}} =
        ?wait_async_action(
            send_message(Config, Data),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),

    ?assertMatch(
        {error, {400, #{failed := 1, success := 0}}}, Result
    ),
    ok.

make_data() ->
    make_data(<<"cpu">>, 12).

make_data(Metric, Value) ->
    #{
        metric => Metric,
        tags => #{
            <<"host">> => <<"serverA">>
        },
        value => Value
    }.
