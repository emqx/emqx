%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_testlib).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% ct setup helpers

init_per_suite(Config, Apps) ->
    [{start_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps(lists:reverse(?config(start_apps, Config))),
    _ = application:stop(emqx_connector),
    ok.

init_per_group(TestGroup, BridgeType, Config) ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    application:load(emqx_bridge),
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:start_apps(?config(start_apps, Config)),
    {ok, _} = application:ensure_all_started(emqx_connector),
    emqx_mgmt_api_test_util:init_suite(),
    UniqueNum = integer_to_binary(erlang:unique_integer([positive])),
    MQTTTopic = <<"mqtt/topic/abc", UniqueNum/binary>>,
    [
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {mqtt_topic, MQTTTopic},
        {test_group, TestGroup},
        {bridge_type, BridgeType}
        | Config
    ].

end_per_group(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    delete_all_bridges(),
    ok.

init_per_testcase(TestCase, Config0, BridgeConfigCb) ->
    ct:timetrap(timer:seconds(60)),
    delete_all_bridges(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    BridgeTopic =
        <<
            (atom_to_binary(TestCase))/binary,
            UniqueNum/binary
        >>,
    TestGroup = ?config(test_group, Config0),
    Config = [{bridge_topic, BridgeTopic} | Config0],
    {Name, ConfigString, BridgeConfig} = BridgeConfigCb(
        TestCase, TestGroup, Config
    ),
    ok = snabbkaffe:start_trace(),
    [
        {bridge_name, Name},
        {bridge_config_string, ConfigString},
        {bridge_config, BridgeConfig}
        | Config
    ].

end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            ProxyHost = ?config(proxy_host, Config),
            ProxyPort = ?config(proxy_port, Config),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            delete_all_bridges(),
            %% in CI, apparently this needs more time since the
            %% machines struggle with all the containers running...
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            ok
    end.

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

%% test helpers
parse_and_check(Config, ConfigString, Name) ->
    BridgeType = ?config(bridge_type, Config),
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := BridgeConfig}}} = RawConf,
    BridgeConfig.

resource_id(Config) ->
    BridgeType = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    emqx_bridge_resource:resource_id(BridgeType, Name).

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    BridgeType = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    BridgeConfig0 = ?config(bridge_config, Config),
    BridgeConfig = emqx_utils_maps:deep_merge(BridgeConfig0, Overrides),
    ct:pal("creating bridge with config: ~p", [BridgeConfig]),
    emqx_bridge:create(BridgeType, Name, BridgeConfig).

create_bridge_api(Config) ->
    create_bridge_api(Config, _Overrides = #{}).

create_bridge_api(Config, Overrides) ->
    BridgeType = ?config(bridge_type, Config),
    BridgeName = ?config(bridge_name, Config),
    BridgeConfig0 = ?config(bridge_config, Config),
    BridgeConfig = emqx_utils_maps:deep_merge(BridgeConfig0, Overrides),
    create_bridge_api(BridgeType, BridgeName, BridgeConfig).

create_bridge_api(BridgeType, BridgeName, BridgeConfig) ->
    Params = BridgeConfig#{<<"type">> => BridgeType, <<"name">> => BridgeName},
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("creating bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, Opts) of
            {ok, {Status, Headers, Body0}} ->
                {ok, {Status, Headers, emqx_utils_json:decode(Body0, [return_maps])}};
            Error ->
                Error
        end,
    ct:pal("bridge create result: ~p", [Res]),
    Res.

update_bridge_api(Config) ->
    update_bridge_api(Config, _Overrides = #{}).

update_bridge_api(Config, Overrides) ->
    BridgeType = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    BridgeConfig0 = ?config(bridge_config, Config),
    BridgeConfig = emqx_utils_maps:deep_merge(BridgeConfig0, Overrides),
    BridgeId = emqx_bridge_resource:bridge_id(BridgeType, Name),
    Params = BridgeConfig#{<<"type">> => BridgeType, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("updating bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(put, Path, "", AuthHeader, Params, Opts) of
            {ok, {_Status, _Headers, Body0}} -> {ok, emqx_utils_json:decode(Body0, [return_maps])};
            Error -> Error
        end,
    ct:pal("bridge update result: ~p", [Res]),
    Res.

op_bridge_api(Op, BridgeType, BridgeName) ->
    BridgeId = emqx_bridge_resource:bridge_id(BridgeType, BridgeName),
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId, Op]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("calling bridge ~p (via http): ~p", [BridgeId, Op]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, "", Opts) of
            {ok, {Status, Headers, Body}} ->
                {ok, {Status, Headers, emqx_utils_json:decode(Body, [return_maps])}};
            {error, {Status, Headers, Body}} ->
                {error, {Status, Headers, emqx_utils_json:decode(Body, [return_maps])}};
            Error ->
                Error
        end,
    ct:pal("bridge op result: ~p", [Res]),
    Res.

probe_bridge_api(Config) ->
    probe_bridge_api(Config, _Overrides = #{}).

probe_bridge_api(Config, _Overrides) ->
    BridgeType = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    BridgeConfig = ?config(bridge_config, Config),
    Params = BridgeConfig#{<<"type">> => BridgeType, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("probing bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, Opts) of
            {ok, {{_, 204, _}, _Headers, _Body0} = Res0} -> {ok, Res0};
            Error -> Error
        end,
    ct:pal("bridge probe result: ~p", [Res]),
    Res.

create_rule_and_action_http(BridgeType, RuleTopic, Config) ->
    BridgeName = ?config(bridge_name, Config),
    BridgeId = emqx_bridge_resource:bridge_id(BridgeType, BridgeName),
    Params = #{
        enable => true,
        sql => <<"SELECT * FROM \"", RuleTopic/binary, "\"">>,
        actions => [BridgeId]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ct:pal("rule action params: ~p", [Params]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_sync_query(Config, MakeMessageFun, IsSuccessCheck, TracePoint) ->
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            Message = {send_message, MakeMessageFun()},
            IsSuccessCheck(emqx_resource:simple_sync_query(ResourceId, Message)),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{instance_id := ResourceId}], ?of_kind(TracePoint, Trace))
        end
    ),
    ok.

t_async_query(Config, MakeMessageFun, IsSuccessCheck, TracePoint) ->
    ResourceId = resource_id(Config),
    ReplyFun =
        fun(Pid, Result) ->
            Pid ! {result, Result}
        end,
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            Message = {send_message, MakeMessageFun()},
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_resource:query(ResourceId, Message, #{
                        async_reply_fun => {ReplyFun, [self()]}
                    }),
                    #{?snk_kind := TracePoint, instance_id := ResourceId},
                    5_000
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{instance_id := ResourceId}], ?of_kind(TracePoint, Trace))
        end
    ),
    receive
        {result, Result} -> IsSuccessCheck(Result)
    after 5_000 ->
        throw(timeout)
    end,
    ok.

t_create_via_http(Config) ->
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),

            %% lightweight matrix testing some configs
            ?assertMatch(
                {ok, _},
                update_bridge_api(
                    Config
                )
            ),
            ?assertMatch(
                {ok, _},
                update_bridge_api(
                    Config
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_start_stop(Config, StopTracePoint) ->
    BridgeType = ?config(bridge_type, Config),
    BridgeName = ?config(bridge_name, Config),
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge(Config)),
            %% Since the connection process is async, we give it some time to
            %% stabilize and avoid flakiness.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),

            %% Check that the bridge probe API doesn't leak atoms.
            ProbeRes0 = probe_bridge_api(
                Config,
                #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}}
            ),
            ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes0),
            AtomsBefore = erlang:system_info(atom_count),
            %% Probe again; shouldn't have created more atoms.
            ProbeRes1 = probe_bridge_api(
                Config,
                #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}}
            ),

            ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes1),
            AtomsAfter = erlang:system_info(atom_count),
            ?assertEqual(AtomsBefore, AtomsAfter),

            %% Now stop the bridge.
            ?assertMatch(
                {{ok, _}, {ok, _}},
                ?wait_async_action(
                    emqx_bridge:disable_enable(disable, BridgeType, BridgeName),
                    #{?snk_kind := StopTracePoint},
                    5_000
                )
            ),

            ok
        end,
        fun(Trace) ->
            %% one for each probe, one for real
            ?assertMatch([_, _, #{instance_id := ResourceId}], ?of_kind(StopTracePoint, Trace)),
            ok
        end
    ),
    ok.

t_on_get_status(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ResourceId = resource_id(Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    %% Since the connection process is async, we give it some time to
    %% stabilize and avoid flakiness.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ct:sleep(500),
        ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId))
    end),
    %% Check that it recovers itself.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ok.
