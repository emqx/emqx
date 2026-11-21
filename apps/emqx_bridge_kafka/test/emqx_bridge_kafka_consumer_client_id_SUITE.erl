%% Checks the client ID that a Kafka consumer group presents to the broker.
%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_consumer_client_id_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONNECTOR_TYPE, kafka_consumer).
-define(SOURCE_TYPE, kafka_consumer).
-define(PROBE_CLIENT, emqx_bridge_kafka_consumer_client_id_probe).

all() ->
    [
        t_configured_group_client_id,
        t_default_group_client_id
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_bridge_kafka_testlib:wait_until_kafka_is_up(),
    ok = brod:start_client(
        emqx_bridge_kafka_testlib:kafka_hosts_direct(), ?PROBE_CLIENT, []
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    _ = catch brod:stop_client(?PROBE_CLIENT),
    emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    Name = atom_to_binary(TestCase),
    Topic = <<Name/binary, "-topic">>,
    GroupId = iolist_to_binary([Name, "-", integer_to_binary(erlang:unique_integer([positive]))]),
    emqx_bridge_kafka_testlib:ensure_kafka_topic(Topic),
    ConnectorConfig = emqx_bridge_kafka_testlib:source_connector_config(#{
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"5s">>}
    }),
    SourceConfig = emqx_bridge_kafka_testlib:source_config(#{
        <<"connector">> => Name,
        <<"parameters">> => #{<<"topic">> => Topic, <<"group_id">> => GroupId},
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"5s">>}
    }),
    [
        {bridge_kind, source},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, Name},
        {source_config, SourceConfig},
        {topic, Topic},
        {group_id, GroupId}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_configured_group_client_id(Config) ->
    ClientId = <<"consumer-group-client-id">>,
    {201, _} = create_connector_api(Config, #{<<"kafka_client_id">> => ClientId}),
    {201, _} = create_source_api(Config, #{}),
    wait_source_connected(Config),
    Group = wait_group(Config),
    ?assertEqual([ClientId], member_client_ids(Group)),
    ok.

t_default_group_client_id(Config) ->
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_source_api(Config, #{}),
    wait_source_connected(Config),
    Group = wait_group(Config),
    [ClientId] = member_client_ids(Group),
    NodePrefix = iolist_to_binary(io_lib:format("~p/", [node()])),
    ?assert(byte_size(ClientId) > byte_size(NodePrefix)),
    ?assertEqual(NodePrefix, binary:part(ClientId, 0, byte_size(NodePrefix))),
    ?assertMatch({_, _}, binary:match(ClientId, <<"/<">>)),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_connector_api2(Config, Overrides).

create_source_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(Config, Overrides).

get_source_api(Config) ->
    Type = ?config(source_type, Config),
    Name = ?config(source_name, Config),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

wait_source_connected(Config) ->
    wait_source_connected(Config, 30).

wait_source_connected(_Config, 0) ->
    ct:fail(source_not_connected);
wait_source_connected(Config, Attempts) ->
    case get_source_api(Config) of
        {200, #{<<"status">> := <<"connected">>}} ->
            ok;
        _Other ->
            timer:sleep(500),
            wait_source_connected(Config, Attempts - 1)
    end.

wait_group(Config) ->
    wait_group(?config(group_id, Config), 30).

wait_group(_GroupId, 0) ->
    ct:fail(group_not_found);
wait_group(GroupId, Attempts) ->
    case describe_group(GroupId) of
        {ok, [#{members := [_ | _]} = Group]} ->
            Group;
        _Other ->
            timer:sleep(500),
            wait_group(GroupId, Attempts - 1)
    end.

describe_group(GroupId) ->
    case brod_client:get_group_coordinator(?PROBE_CLIENT, GroupId) of
        {ok, {Endpoint, ConnConfig}} ->
            brod:describe_groups(Endpoint, ConnConfig, [GroupId]);
        Error ->
            Error
    end.

member_client_ids(#{members := Members}) ->
    lists:usort([maps:get(client_id, Member) || Member <- Members]).
