%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_connector_SUITE).

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
    DatalayersTCPHost = os:getenv("DATALAYERS_TCP_HOST", "toxiproxy"),
    DatalayersTCPPort = list_to_integer(os:getenv("DATALAYERS_TCP_PORT", "8361")),
    DatalayersTLSHost = os:getenv("DATALAYERS_TLS_HOST", "toxiproxy"),
    DatalayersTLSPort = list_to_integer(os:getenv("DATALAYERS_TLS_PORT", "8362")),
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
            EHttpcPoolNameBin = <<(atom_to_binary(?MODULE))/binary, "_apiv1">>,
            EHttpcPoolName = binary_to_atom(EHttpcPoolNameBin),
            EHttpcPoolOpts = [
                {host, DatalayersTCPHost},
                {port, DatalayersTCPPort},
                {pool_size, 1},
                {transport, tcp},
                {transport_opts, []}
            ],

            {ok, _} = ehttpc_sup:start_pool(EHttpcPoolName, EHttpcPoolOpts),

            NewConfig =
                [
                    {apps, Apps},
                    {datalayers_host, DatalayersTCPHost},
                    {datalayers_port, DatalayersTCPPort},
                    {datalayers_tls_host, DatalayersTLSHost},
                    {datalayers_tls_port, DatalayersTLSPort},
                    {ehttpc_pool_name, EHttpcPoolName}
                    | Config
                ],
            emqx_bridge_datalayers_SUITE:ensure_database(NewConfig),
            NewConfig;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_datalayers);
                _ ->
                    {skip, no_datalayers}
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
    Host = ?config(datalayers_host, Config),
    Port = ?config(datalayers_port, Config),
    perform_lifecycle_check(
        <<"emqx_bridge_datalayers_connector_SUITE">>,
        datalayers_connector_config(Host, Port, false, <<"verify_none">>)
    ).

perform_lifecycle_check(PoolName, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?DATALAYERS_RESOURCE_MOD, InitialConfig),
    % We need to add a write_syntax to the config since the connector
    % expects this
    FullConfig = CheckedConfig#{write_syntax => datalayers_write_syntax()},
    {ok, #{
        id := ResourceId,
        state := #{client := #{pool := ReturnedPoolName}} = State,
        status := InitialStatus
    }} = emqx_resource:create_local(
        PoolName,
        ?CONNECTOR_RESOURCE_GROUP,
        ?DATALAYERS_RESOURCE_MOD,
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
    ActionConfig = datalayers_action_config(),
    ChannelId = <<"test_channel">>,
    ?assertEqual(
        ok,
        emqx_resource_manager:add_channel(
            ResourceId, ChannelId, ActionConfig
        )
    ),
    ?assertMatch(#{status := connected}, emqx_resource:channel_health_check(ResourceId, ChannelId)),
    % % Perform query as further check that the resource is working as expected
    ?assertMatch({ok, 204, _}, emqx_resource:query(PoolName, test_query(ChannelId))),
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
    ChannelId = <<"test_channel">>,
    ?assertEqual(
        ok,
        emqx_resource_manager:add_channel(
            ResourceId, ChannelId, ActionConfig
        )
    ),
    ?assertMatch(#{status := connected}, emqx_resource:channel_health_check(ResourceId, ChannelId)),
    ?assertMatch({ok, 204, _}, emqx_resource:query(PoolName, test_query(ChannelId))),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(PoolName)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(ReturnedPoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(PoolName)).

t_tls_verify_none(Config) ->
    PoolName = <<"testpool-1">>,
    Host = ?config(datalayers_tls_host, Config),
    Port = ?config(datalayers_tls_port, Config),
    InitialConfig = datalayers_connector_config(Host, Port, true, <<"verify_none">>),
    ValidStatus = perform_tls_opts_check(PoolName, InitialConfig, valid),
    ?assertEqual(connected, ValidStatus),
    InvalidStatus = perform_tls_opts_check(PoolName, InitialConfig, fail),
    ?assertEqual(disconnected, InvalidStatus),
    ok.

t_tls_verify_peer(Config) ->
    PoolName = <<"testpool-2">>,
    Host = ?config(datalayers_tls_host, Config),
    Port = ?config(datalayers_tls_port, Config),
    InitialConfig = datalayers_connector_config(Host, Port, true, <<"verify_peer">>),
    %% This works without a CA-cert & friends since we are using a mock
    ValidStatus = perform_tls_opts_check(PoolName, InitialConfig, valid),
    ?assertEqual(connected, ValidStatus),
    InvalidStatus = perform_tls_opts_check(PoolName, InitialConfig, fail),
    ?assertEqual(disconnected, InvalidStatus),
    ok.

perform_tls_opts_check(PoolName, InitialConfig, VerifyReturn) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?DATALAYERS_RESOURCE_MOD, InitialConfig),
    % Meck handling of TLS opt handling so that we can inject custom
    % verification returns
    meck:new(emqx_tls_lib, [passthrough, no_link]),
    meck:expect(
        emqx_tls_lib,
        to_client_opts,
        fun(Opts) ->
            Verify = {verify_fun, {custom_verify(), {return, VerifyReturn}}},
            [
                Verify,
                {cacerts, public_key:cacerts_get()}
                | meck:passthrough([Opts])
            ]
        end
    ),
    try
        % We need to add a write_syntax to the config since the connector
        % expects this
        FullConfig = CheckedConfig#{write_syntax => datalayers_write_syntax()},
        {ok, #{
            config := #{ssl := #{enable := SslEnabled}},
            status := Status
        }} = emqx_resource:create_local(
            PoolName,
            ?CONNECTOR_RESOURCE_GROUP,
            ?DATALAYERS_RESOURCE_MOD,
            FullConfig,
            #{}
        ),
        ?assert(SslEnabled),
        ?assert(meck:validate(emqx_tls_lib)),
        % Stop and remove the resource in one go.
        ?assertEqual(ok, emqx_resource:remove_local(PoolName)),
        Status
    after
        meck:unload(emqx_tls_lib)
    end.

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

datalayers_connector_config(Host, Port, SslEnabled, Verify) ->
    Server = list_to_binary(io_lib:format("~s:~b", [Host, Port])),
    ConnectorConf = #{
        <<"parameters">> => #{
            <<"driver_type">> => <<"influxdb_v1">>,
            <<"database">> => <<"mqtt">>,
            <<"username">> => <<"admin">>,
            <<"password">> => <<"public">>
        },
        <<"server">> => Server,
        <<"ssl">> => #{
            <<"enable">> => SslEnabled,
            <<"verify">> => Verify
        }
    },
    #{<<"config">> => ConnectorConf}.

datalayers_action_config() ->
    #{
        parameters => #{
            write_syntax => datalayers_write_syntax(),
            precision => ms
        }
    }.

custom_verify() ->
    fun
        (_, {bad_cert, unknown_ca} = Event, {return, Return} = UserState) ->
            ct:pal("Call to custom verify fun. Event: ~p UserState: ~p", [Event, UserState]),
            {Return, UserState};
        (_, Event, UserState) ->
            ct:pal("Unexpected call to custom verify fun. Event: ~p UserState: ~p", [
                Event, UserState
            ]),
            {fail, unexpected_call_to_verify_fun}
    end.

datalayers_write_syntax() ->
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
