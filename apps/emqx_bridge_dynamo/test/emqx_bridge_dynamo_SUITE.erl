%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dynamo_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% DB defaults
-define(TABLE, "mqtt").
-define(TABLE_BIN, to_bin(?TABLE)).
-define(ACCESS_KEY_ID, "root").
-define(SECRET_ACCESS_KEY, "public").
-define(HOST, "dynamo").
-define(PORT, 8000).
-define(SCHEMA, "http://").
-define(BATCH_SIZE, 10).
-define(PAYLOAD, <<"HELLO">>).

%% How to run it locally (all commands are run in $PROJ_ROOT dir):
%% run ct in docker container
%% run script:
%% ```bash
%% ./scripts/ct/run.sh --ci --app apps/emqx_bridge_dynamo -- \
%%                     --name 'test@127.0.0.1' -c -v --readable true \
%%                     --suite apps/emqx_bridge_dynamo/test/emqx_bridge_dynamo_SUITE.erl

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, with_batch},
        {group, without_batch},
        {group, flaky}
    ].

groups() ->
    TCs0 = emqx_common_test_helpers:all(?MODULE),

    %% due to the poorly implemented driver or other reasons
    %% if we mix these cases with others, this suite will become flaky.
    Flaky = [t_get_status, t_write_failure],
    TCs = TCs0 -- Flaky,

    [
        {with_batch, TCs},
        {without_batch, TCs},
        {flaky, Flaky}
    ].

init_per_group(with_batch, Config0) ->
    Config = [{batch_size, ?BATCH_SIZE} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{batch_size, 1} | Config0],
    common_init(Config);
init_per_group(flaky, Config0) ->
    Config = [{batch_size, 1} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok;
end_per_group(Group, Config) when Group =:= flaky ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    timer:sleep(1000),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    SecretFile = filename:join(?config(priv_dir, Config), "secret"),
    ok = file:write_file(SecretFile, <<?SECRET_ACCESS_KEY>>),
    [{dynamo_secretfile, SecretFile} | Config].

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_resource, emqx_conf, erlcloud]),
    ok.

init_per_testcase(TestCase, Config) ->
    create_table(Config),
    ok = snabbkaffe:start_trace(),
    [{dynamo_name, atom_to_binary(TestCase)} | Config].

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = snabbkaffe:stop(),
    delete_table(Config),
    delete_all_bridges(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(ConfigT) ->
    Host = os:getenv("DYNAMO_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("DYNAMO_PORT", "8000")),

    Config0 = [
        {host, Host},
        {port, Port},
        {query_mode, sync},
        {proxy_name, "dynamo"}
        | ConfigT
    ],

    BridgeType = proplists:get_value(bridge_type, Config0, <<"dynamo">>),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            % Ensure enterprise bridge module is loaded
            ok = emqx_common_test_helpers:start_apps([
                emqx_conf, emqx_resource, emqx_bridge
            ]),
            _ = application:ensure_all_started(erlcloud),
            _ = emqx_bridge_enterprise:module_info(),
            emqx_mgmt_api_test_util:init_suite(),
            % setup dynamo
            setup_dynamo(Config0),
            {Name, TDConf} = dynamo_config(BridgeType, Config0),
            Config =
                [
                    {dynamo_config, TDConf},
                    {dynamo_bridge_type, BridgeType},
                    {dynamo_name, Name},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_dynamo);
                _ ->
                    {skip, no_dynamo}
            end
    end.

dynamo_config(BridgeType, Config) ->
    Host = ?config(host, Config),
    Port = ?config(port, Config),
    Name = atom_to_binary(?MODULE),
    BatchSize = ?config(batch_size, Config),
    QueryMode = ?config(query_mode, Config),
    SecretFile = ?config(dynamo_secretfile, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {"
            "\n   enable = true"
            "\n   url = \"http://~s:~p\""
            "\n   table = ~p"
            "\n   aws_access_key_id = ~p"
            "\n   aws_secret_access_key = ~p"
            "\n   resource_opts = {"
            "\n     request_ttl = 500ms"
            "\n     batch_size = ~b"
            "\n     query_mode = ~s"
            "\n   }"
            "\n }",
            [
                BridgeType,
                Name,
                Host,
                Port,
                ?TABLE,
                ?ACCESS_KEY_ID,
                %% NOTE: using file-based secrets with HOCON configs
                "file://" ++ SecretFile,
                BatchSize,
                QueryMode
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
    BridgeType = ?config(dynamo_bridge_type, Config),
    Name = ?config(dynamo_name, Config),
    DynamoConfig0 = ?config(dynamo_config, Config),
    DynamoConfig = emqx_utils_maps:deep_merge(DynamoConfig0, Overrides),
    emqx_bridge:create(BridgeType, Name, DynamoConfig).

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(dynamo_name, Config),
    BridgeType = ?config(dynamo_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    emqx_bridge:send_message(BridgeID, Payload).

query_resource(Config, Request) ->
    Name = ?config(dynamo_name, Config),
    BridgeType = ?config(dynamo_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ResourceID, Request, #{timeout => 1_000}).

%% create a table, use the apps/emqx_bridge_dynamo/priv/dynamo/mqtt_msg.json as template
create_table(Config) ->
    directly_setup_dynamo(),
    delete_table(Config),
    ?assertMatch(
        {ok, _},
        erlcloud_ddb2:create_table(
            ?TABLE_BIN,
            [{<<"id">>, s}],
            <<"id">>,
            [{provisioned_throughput, {5, 5}}]
        )
    ).

delete_table(_Config) ->
    erlcloud_ddb2:delete_table(?TABLE_BIN).

setup_dynamo(Config) ->
    Host = ?config(host, Config),
    Port = ?config(port, Config),
    erlcloud_ddb2:configure(?ACCESS_KEY_ID, ?SECRET_ACCESS_KEY, Host, Port, ?SCHEMA).

directly_setup_dynamo() ->
    erlcloud_ddb2:configure(?ACCESS_KEY_ID, ?SECRET_ACCESS_KEY, ?HOST, ?PORT, ?SCHEMA).

directly_query(Query) ->
    directly_setup_dynamo(),
    emqx_bridge_dynamo_connector_client:execute(Query, ?TABLE_BIN).

directly_get_payload(Key) ->
    case directly_query({get_item, {<<"id">>, Key}}) of
        {ok, Values} ->
            proplists:get_value(<<"payload">>, Values, {error, {invalid_item, Values}});
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertNotEqual(undefined, get(aws_config)),
    create_table(Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    MsgId = emqx_utils:gen_id(),
    SentData = #{id => MsgId, payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertMatch(
                    {ok, _}, send_message(Config, SentData)
                ),
                #{?snk_kind := dynamo_connector_query_return},
                10_000
            ),
            ?assertMatch(
                ?PAYLOAD,
                directly_get_payload(MsgId)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(dynamo_connector_query_return, Trace0),
            ?assertMatch([#{result := {ok, _}}], Trace),
            ok
        end
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?config(dynamo_bridge_type, Config),
    Name = ?config(dynamo_name, Config),
    PgsqlConfig0 = ?config(dynamo_config, Config),
    PgsqlConfig = PgsqlConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType,
        %% NOTE: using literal secret with HTTP API requests.
        <<"aws_secret_access_key">> => <<?SECRET_ACCESS_KEY>>
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(PgsqlConfig)
    ),
    MsgId = emqx_utils:gen_id(),
    SentData = #{id => MsgId, payload => ?PAYLOAD},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertMatch(
                    {ok, _}, send_message(Config, SentData)
                ),
                #{?snk_kind := dynamo_connector_query_return},
                10_000
            ),
            ?assertMatch(
                ?PAYLOAD,
                directly_get_payload(MsgId)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(dynamo_connector_query_return, Trace0),
            ?assertMatch([#{result := {ok, _}}], Trace),
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    {{ok, _}, {ok, _}} =
        ?wait_async_action(
            create_bridge(Config),
            #{?snk_kind := resource_connected_enter},
            20_000
        ),

    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),

    Name = ?config(dynamo_name, Config),
    BridgeType = ?config(dynamo_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID)),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        case emqx_resource_manager:health_check(ResourceID) of
            {ok, Status} when Status =:= disconnected orelse Status =:= connecting ->
                ok;
            {error, timeout} ->
                ok;
            Other ->
                ?assert(
                    false, lists:flatten(io_lib:format("invalid health check result:~p~n", [Other]))
                )
        end
    end),
    ok.

t_write_failure(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    {{ok, _}, {ok, _}} =
        ?wait_async_action(
            create_bridge(Config),
            #{?snk_kind := resource_connected_enter},
            20_000
        ),
    SentData = #{id => emqx_utils:gen_id(), payload => ?PAYLOAD},
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertMatch(
            {error, {resource_error, #{reason := timeout}}}, send_message(Config, SentData)
        )
    end),
    ok.

t_simple_query(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {get_item, {<<"id">>, <<"not_exists">>}},
    Result = query_resource(Config, Request),
    case ?config(batch_size, Config) of
        ?BATCH_SIZE ->
            ?assertMatch({error, {unrecoverable_error, {invalid_request, _}}}, Result);
        1 ->
            ?assertMatch({ok, []}, Result)
    end,
    ok.

t_missing_data(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Result = send_message(Config, #{}),
    ?assertMatch({error, {unrecoverable_error, {invalid_request, _}}}, Result),
    ok.

t_bad_parameter(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {insert_item, bad_parameter},
    Result = query_resource(Config, Request),
    ?assertMatch({error, {unrecoverable_error, {invalid_request, _}}}, Result),
    ok.

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8);
to_bin(Bin) when is_binary(Bin) ->
    Bin.
