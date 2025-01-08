%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_greptimedb_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(GREPTIMEDB_RESOURCE_MOD, emqx_bridge_greptimedb_connector).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    GreptimedbTCPHost = os:getenv("GREPTIMEDB_GRPCV1_TCP_HOST", "toxiproxy"),
    GreptimedbTCPPort = list_to_integer(os:getenv("GREPTIMEDB_GRPCV1_TCP_PORT", "4001")),
    Servers = [{GreptimedbTCPHost, GreptimedbTCPPort}],
    case emqx_common_test_helpers:is_all_tcp_servers_available(Servers) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_greptimedb,
                    emqx_bridge
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [
                {apps, Apps},
                {greptimedb_tcp_host, GreptimedbTCPHost},
                {greptimedb_tcp_port, GreptimedbTCPPort}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_greptimedb);
                _ ->
                    {skip, no_greptimedb}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

t_lifecycle(Config) ->
    Host = ?config(greptimedb_tcp_host, Config),
    Port = ?config(greptimedb_tcp_port, Config),
    perform_lifecycle_check(
        <<"emqx_bridge_greptimedb_connector_SUITE">>,
        greptimedb_connector_config(Host, Port)
    ).

perform_lifecycle_check(PoolName, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?GREPTIMEDB_RESOURCE_MOD, InitialConfig),
    % We need to add a write_syntax to the config since the connector
    % expects this
    FullConfig = CheckedConfig#{write_syntax => greptimedb_write_syntax()},
    {ok, #{
        id := ResourceId,
        state := #{client := #{pool := ReturnedPoolName}} = State,
        status := InitialStatus
    }} = emqx_resource:create_local(
        PoolName,
        ?CONNECTOR_RESOURCE_GROUP,
        ?GREPTIMEDB_RESOURCE_MOD,
        FullConfig,
        #{}
    ),
    ?assertEqual(InitialStatus, connected),
    % Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
    %% install actions to the connector
    ActionConfig = greptimedb_action_config(),
    ChannelId = <<"test_channel">>,
    ?assertEqual(ok, emqx_resource_manager:add_channel(ResourceId, ChannelId, ActionConfig)),
    ?assertMatch(#{status := connected}, emqx_resource:channel_health_check(ResourceId, ChannelId)),
    % % Perform query as further check that the resource is working as expected
    ?assertMatch({ok, _}, emqx_resource:query(PoolName, test_query(ChannelId))),
    ?assertEqual(ok, emqx_resource:stop(PoolName)),
    % Resource will be listed still, but state will be changed and healthcheck will fail
    % as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(PoolName)),
    % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(ReturnedPoolName)),
    % Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(PoolName)),
    % Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(PoolName)),
    % async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
    ?assertEqual(ok, emqx_resource_manager:add_channel(ResourceId, ChannelId, ActionConfig)),
    ?assertMatch(#{status := connected}, emqx_resource:channel_health_check(ResourceId, ChannelId)),
    ?assertMatch({ok, _}, emqx_resource:query(PoolName, test_query(ChannelId))),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(PoolName)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(ReturnedPoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(PoolName)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

greptimedb_connector_config(Host, Port) ->
    Server = list_to_binary(io_lib:format("~s:~b", [Host, Port])),
    ResourceConfig = #{
        <<"dbname">> => <<"public">>,
        <<"server">> => Server,
        <<"username">> => <<"greptime_user">>,
        <<"password">> => <<"greptime_pwd">>
    },
    #{<<"config">> => ResourceConfig}.

greptimedb_action_config() ->
    #{
        parameters => #{
            write_syntax => greptimedb_write_syntax(),
            precision => ms
        }
    }.

greptimedb_write_syntax() ->
    [
        #{
            measurement => "${topic}",
            tags => [{"clientid", "${clientid}"}],
            fields => [{"payload", "${payload}"}],
            timestamp => undefined
        }
    ].

test_query(ChannelId) ->
    {ChannelId, #{
        <<"clientid">> => <<"something">>,
        <<"payload">> => #{bool => true},
        <<"topic">> => <<"connector_test">>,
        <<"timestamp">> => 1678220316257
    }}.
