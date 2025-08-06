%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_arrow_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DATALAYERS_RESOURCE_MOD, emqx_bridge_datalayers_connector).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    DatalayersTCPHost = os:getenv("DATALAYERS_GRPC_HOST", "toxiproxy"),
    DatalayersTCPPort = list_to_integer(os:getenv("DATALAYERS_GRPC_PORT", "8360")),
    DatalayersTLSHost = os:getenv("DATALAYERS_GRPCS_HOST", "toxiproxy"),
    DatalayersTLSPort = list_to_integer(os:getenv("DATALAYERS_GRPCS_PORT", "8362")),
    Servers = [{DatalayersTCPHost, DatalayersTCPPort}, {DatalayersTLSHost, DatalayersTLSPort}],
    case emqx_common_test_helpers:is_all_tcp_servers_available(Servers) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx_conf,
                    emqx_bridge_datalayers,
                    emqx_bridge
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            NewConfig =
                [
                    {apps, Apps},
                    {datalayers_host, DatalayersTCPHost},
                    {datalayers_port, DatalayersTCPPort},
                    {datalayers_tls_host, DatalayersTLSHost},
                    {datalayers_tls_port, DatalayersTLSPort}
                    | Config
                ],
            NewConfig;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_datalayers_arrow);
                _ ->
                    {skip, no_datalayers_arrow}
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

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_lifecycle(Config) ->
    Host = ?config(datalayers_host, Config),
    Port = ?config(datalayers_port, Config),
    perform_lifecycle_check(
        atom_to_binary(?FUNCTION_NAME),
        datalayers_connector_config(Host, Port, false)
    ).

t_lifecycle_tls_valid(Config) ->
    Host = ?config(datalayers_tls_host, Config),
    Port = ?config(datalayers_tls_port, Config),
    perform_lifecycle_check(
        atom_to_binary(?FUNCTION_NAME),
        datalayers_connector_config(Host, Port, true)
    ),
    ok.

%% verify_none not allowed
t_tls_verify_none_not_supported(Config) ->
    PoolName = atom_to_binary(?FUNCTION_NAME),
    Host = ?config(datalayers_tls_host, Config),
    Port = ?config(datalayers_tls_port, Config),
    InvalidConfig = datalayers_connector_config(Host, Port, true, <<"verify_none">>),
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?DATALAYERS_RESOURCE_MOD, InvalidConfig),
    ?assertMatch(
        {ok, #{
            error := verify_none_not_supported,
            config := #{ssl := #{enable := _SslEnabled, verify := verify_none}},
            status := _Status
        }},
        emqx_resource:create_local(
            PoolName,
            ?CONNECTOR_RESOURCE_GROUP,
            ?DATALAYERS_RESOURCE_MOD,
            CheckedConfig,
            #{}
        )
    ),
    ok.

perform_lifecycle_check(PoolName, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?DATALAYERS_RESOURCE_MOD, InitialConfig),
    {ok, #{
        id := ResourceId,
        state := State,
        status := InitialStatus
    }} = emqx_resource:create_local(
        PoolName,
        ?CONNECTOR_RESOURCE_GROUP,
        ?DATALAYERS_RESOURCE_MOD,
        CheckedConfig,
        #{spawn_buffer_workers => true}
    ),
    ?assertEqual(connected, InitialStatus),
    %% Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
    %% install actions to the connector
    ActionConfig = datalayers_action_config(),
    ChannelId = <<"test_channel">>,
    ?assertEqual(
        ok,
        emqx_resource_manager:add_channel(
            ResourceId, ChannelId, ActionConfig
        )
    ),
    ?assertMatch(#{status := connected}, emqx_resource:channel_health_check(ResourceId, ChannelId)),
    %% Perform query as further check that the resource is working as expected
    ?assertEqual(ok, emqx_resource:stop(PoolName)),
    %% Resource will be listed still, but state will be changed and healthcheck will fail
    %% as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(PoolName)),
    %% Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(PoolName)),
    %% Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(PoolName)),
    %% async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
    ?assertEqual(
        ok,
        emqx_resource_manager:add_channel(
            ResourceId, ChannelId, ActionConfig
        )
    ),
    ?assertMatch(#{status := connected}, emqx_resource:channel_health_check(ResourceId, ChannelId)),
    %% Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(PoolName)),
    %% Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(PoolName)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

datalayers_connector_config(Host, Port, SslEnabled) ->
    datalayers_connector_config(Host, Port, SslEnabled, <<"verify_peer">>).

datalayers_connector_config(Host, Port, SslEnabled, Verify) ->
    Server = list_to_binary(io_lib:format("~s:~b", [Host, Port])),
    Dir = code:lib_dir(emqx_bridge_datalayers),
    %% XXX:
    %% in CI, same as `.ci/docker-compose-file/certs/ca.crt`
    Cacertfile = filename:join([Dir, <<"test/data/certs">>, <<"ca.crt">>]),
    ConnectorConf = #{
        <<"parameters">> => #{
            <<"username">> => <<"admin">>,
            <<"password">> => <<"public">>,
            <<"driver_type">> => <<"arrow_flight">>,
            %% Not used for now
            <<"database">> => <<"common_cest_db">>
        },
        <<"server">> => Server,
        <<"ssl">> => #{
            <<"enable">> => SslEnabled,
            <<"verify">> => Verify,
            <<"cacertfile">> => Cacertfile
        }
    },
    #{<<"config">> => ConnectorConf}.

datalayers_action_config() ->
    #{
        parameters => #{
            sql => datalayers_sql_template()
        }
    }.

datalayers_sql_template() ->
    "INSERT INTO connector_test (ts, clientid, payload_bool) VALUES (${timestamp}, ${clientid}, ${payload.bool})".
