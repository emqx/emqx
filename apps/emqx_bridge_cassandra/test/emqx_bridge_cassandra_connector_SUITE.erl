%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_cassandra_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("emqx_bridge_cassandra.hrl").
-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/assert.hrl").

%% To run this test locally:
%%   ./scripts/ct/run.sh --app apps/emqx_bridge_cassandra --only-up
%%   PROFILE=emqx-enterprise PROXY_HOST=localhost CASSA_TLS_HOST=localhost \
%%     CASSA_TLS_PORT=9142 CASSA_TCP_HOST=localhost CASSA_TCP_NO_AUTH_HOST=localhost \
%%     CASSA_TCP_PORT=19042 CASSA_TCP_NO_AUTH_PORT=19043 \
%%     ./rebar3 ct --name 'test@127.0.0.1' -v --suite \
%%     apps/emqx_bridge_cassandra/test/emqx_bridge_cassandra_connector_SUITE

-define(CASSANDRA_RESOURCE_MOD, emqx_bridge_cassandra_connector).

%% Cassandra default username & password once enable `authenticator: PasswordAuthenticator`
%% in cassandra config
-define(CASSA_USERNAME, <<"cassandra">>).
-define(CASSA_PASSWORD, <<"cassandra">>).

all() ->
    [
        {group, auth},
        {group, noauth}
    ].

groups() ->
    [
        {auth, [t_lifecycle, t_start_passfile]},
        {noauth, [t_lifecycle]}
    ].

cassandra_servers(CassandraHost, CassandraPort) ->
    lists:map(
        fun(#{hostname := Host, port := Port}) ->
            {Host, Port}
        end,
        emqx_schema:parse_servers(
            iolist_to_binary([CassandraHost, ":", erlang:integer_to_list(CassandraPort)]),
            #{default_port => CassandraPort}
        )
    ).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_cassandra,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(Group, Config) ->
    {CassandraHost, CassandraPort, AuthOpts} =
        case Group of
            auth ->
                TcpHost = os:getenv("CASSA_TCP_HOST", "toxiproxy"),
                TcpPort = list_to_integer(os:getenv("CASSA_TCP_PORT", "9042")),
                {TcpHost, TcpPort, [{username, ?CASSA_USERNAME}, {password, ?CASSA_PASSWORD}]};
            noauth ->
                TcpHost = os:getenv("CASSA_TCP_NO_AUTH_HOST", "toxiproxy"),
                TcpPort = list_to_integer(os:getenv("CASSA_TCP_NO_AUTH_PORT", "9043")),
                {TcpHost, TcpPort, []}
        end,
    case emqx_common_test_helpers:is_tcp_server_available(CassandraHost, CassandraPort) of
        true ->
            %% keyspace `mqtt` must be created in advance
            {ok, Conn} =
                ecql:connect([
                    {nodes, cassandra_servers(CassandraHost, CassandraPort)},
                    {keyspace, "mqtt"}
                    | AuthOpts
                ]),
            ecql:close(Conn),
            [
                {cassa_host, CassandraHost},
                {cassa_port, CassandraPort},
                {cassa_auth_opts, AuthOpts}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_cassandra);
                _ ->
                    {skip, no_cassandra}
            end
    end.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_lifecycle(Config) ->
    perform_lifecycle_check(
        <<?MODULE_STRING>>,
        cassandra_config(Config)
    ).

t_start_passfile(Config) ->
    ResourceID = atom_to_binary(?FUNCTION_NAME),
    PasswordFilename = filename:join(?config(priv_dir, Config), "passfile"),
    ok = file:write_file(PasswordFilename, ?CASSA_PASSWORD),
    InitialConfig = emqx_utils_maps:deep_merge(
        cassandra_config(Config),
        #{
            <<"config">> => #{
                password => iolist_to_binary(["file://", PasswordFilename])
            }
        }
    ),
    ?assertMatch(
        #{status := connected},
        create_local_resource(ResourceID, check_config(InitialConfig))
    ),
    ?assertEqual(
        ok,
        emqx_resource:remove_local(ResourceID)
    ).

perform_lifecycle_check(ResourceId, InitialConfig) ->
    CheckedConfig = check_config(InitialConfig),
    #{
        state := #{pool_name := PoolName} = State,
        status := InitialStatus
    } = create_local_resource(ResourceId, CheckedConfig),
    ?assertEqual(InitialStatus, connected),
    % Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    % % Perform query as further check that the resource is working as expected
    (fun() ->
        erlang:display({pool_name, ResourceId}),
        QueryNoParamsResWrapper = emqx_resource:query(ResourceId, test_query_no_params()),
        ?assertMatch({ok, _}, QueryNoParamsResWrapper)
    end)(),
    ?assertEqual(ok, emqx_resource:stop(ResourceId)),
    % Resource will be listed still, but state will be changed and healthcheck will fail
    % as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceId)),
    % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(ResourceId)),
    % Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(ResourceId)),
    % async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    (fun() ->
        QueryNoParamsResWrapper =
            emqx_resource:query(ResourceId, test_query_no_params()),
        ?assertMatch({ok, _}, QueryNoParamsResWrapper)
    end)(),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(ResourceId)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(ResourceId)).

%%--------------------------------------------------------------------
%% utils
%%--------------------------------------------------------------------

check_config(Config) ->
    {ok, #{config := CheckedConfig}} = emqx_resource:check_config(?CASSANDRA_RESOURCE_MOD, Config),
    CheckedConfig.

create_local_resource(ResourceId, CheckedConfig) ->
    {ok, Bridge} =
        emqx_resource:create_local(
            ResourceId,
            ?CONNECTOR_RESOURCE_GROUP,
            ?CASSANDRA_RESOURCE_MOD,
            CheckedConfig,
            #{}
        ),
    Bridge.

cassandra_config(Config) ->
    Host = ?config(cassa_host, Config),
    Port = ?config(cassa_port, Config),
    AuthOpts = maps:from_list(?config(cassa_auth_opts, Config)),
    CassConfig =
        AuthOpts#{
            auto_reconnect => true,
            keyspace => <<"mqtt">>,
            pool_size => 8,
            servers => iolist_to_binary(
                io_lib:format(
                    "~s:~b",
                    [
                        Host,
                        Port
                    ]
                )
            )
        },
    #{<<"config">> => CassConfig}.

test_query_no_params() ->
    {query, <<"SELECT count(1) AS T FROM system.local">>}.
