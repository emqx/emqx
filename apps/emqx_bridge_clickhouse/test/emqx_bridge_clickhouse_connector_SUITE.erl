%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_clickhouse_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-define(APP, emqx_bridge_clickhouse).
-define(CLICKHOUSE_RESOURCE_MOD, emqx_bridge_clickhouse_connector).
-define(CLICKHOUSE_PASSWORD, "public").

%% This test SUITE requires a running clickhouse instance. If you don't want to
%% bring up the whole CI infrastuctucture with the `scripts/ct/run.sh` script
%% you can create a clickhouse instance with the following command (execute it
%% from root of the EMQX directory.). You also need to set ?CLICKHOUSE_HOST and
%% ?CLICKHOUSE_PORT to appropriate values.
%%
%% docker run \
%%    -d \
%%    -p 18123:8123 \
%%    -p 19000:9000 \
%%    --name some-clickhouse-server \
%%    --ulimit nofile=262144:262144 \
%%    -v "`pwd`/.ci/docker-compose-file/clickhouse/users.xml:/etc/clickhouse-server/users.xml" \
%%    -v "`pwd`/.ci/docker-compose-file/clickhouse/config.xml:/etc/clickhouse-server/config.xml" \
%%    clickhouse/clickhouse-server

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Host = emqx_bridge_clickhouse_SUITE:clickhouse_host(),
    Port = list_to_integer(emqx_bridge_clickhouse_SUITE:clickhouse_port()),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_clickhouse,
                    emqx_connector,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            %% Create the db table
            {ok, Conn} =
                clickhouse:start_link([
                    {url, emqx_bridge_clickhouse_SUITE:clickhouse_url()},
                    {user, <<"default">>},
                    {key, ?CLICKHOUSE_PASSWORD},
                    {pool, tmp_pool}
                ]),
            {ok, _, _} = clickhouse:query(Conn, <<"CREATE DATABASE IF NOT EXISTS mqtt">>, #{}),
            clickhouse:stop(Conn),
            [{apps, Apps} | Config];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_clickhouse);
                _ ->
                    {skip, no_clickhouse}
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

t_lifecycle(_Config) ->
    perform_lifecycle_check(
        <<"emqx_connector_clickhouse_SUITE">>,
        clickhouse_config()
    ).

t_start_passfile(Config) ->
    ResourceID = atom_to_binary(?FUNCTION_NAME),
    PasswordFilename = filename:join(?config(priv_dir, Config), "passfile"),
    ok = file:write_file(PasswordFilename, <<?CLICKHOUSE_PASSWORD>>),
    InitialConfig = clickhouse_config(#{
        password => iolist_to_binary(["file://", PasswordFilename])
    }),
    {ok, #{config := ResourceConfig}} =
        emqx_resource:check_config(?CLICKHOUSE_RESOURCE_MOD, InitialConfig),
    ?assertMatch(
        {ok, #{status := connected}},
        emqx_resource:create_local(
            ResourceID,
            ?CONNECTOR_RESOURCE_GROUP,
            ?CLICKHOUSE_RESOURCE_MOD,
            ResourceConfig,
            #{}
        )
    ),
    ?assertEqual(
        ok,
        emqx_resource:remove_local(ResourceID)
    ),
    ok.

show(X) ->
    erlang:display(X),
    X.

show(Label, What) ->
    erlang:display({Label, What}),
    What.

perform_lifecycle_check(ResourceID, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?CLICKHOUSE_RESOURCE_MOD, InitialConfig),
    {ok, #{
        state := #{pool_name := PoolName} = State,
        status := InitialStatus
    }} =
        emqx_resource:create_local(
            ResourceID,
            ?CONNECTOR_RESOURCE_GROUP,
            ?CLICKHOUSE_RESOURCE_MOD,
            CheckedConfig,
            #{}
        ),
    ?assertEqual(InitialStatus, connected),
    % Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(ResourceID),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceID)),
    % % Perform query as further check that the resource is working as expected
    (fun() ->
        QueryNoParamsResWrapper = emqx_resource:query(ResourceID, test_query_no_params()),
        ?assertMatch({ok, _}, QueryNoParamsResWrapper),
        {_, QueryNoParamsRes} = QueryNoParamsResWrapper,
        ?assertMatch(<<"1">>, string:trim(QueryNoParamsRes))
    end)(),
    ?assertEqual(ok, emqx_resource:stop(ResourceID)),
    % Resource will be listed still, but state will be changed and healthcheck will fail
    % as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} =
        emqx_resource:get_instance(ResourceID),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceID)),
    % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(ResourceID)),
    % Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(ResourceID)),
    % async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(ResourceID),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceID)),
    (fun() ->
        QueryNoParamsResWrapper =
            emqx_resource:query(ResourceID, test_query_no_params()),
        ?assertMatch({ok, _}, QueryNoParamsResWrapper),
        {_, QueryNoParamsRes} = QueryNoParamsResWrapper,
        ?assertMatch(<<"1">>, string:trim(QueryNoParamsRes))
    end)(),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(ResourceID)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(ResourceID)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

clickhouse_config() ->
    clickhouse_config(#{}).

clickhouse_config(Overrides) ->
    Config =
        #{
            auto_reconnect => true,
            database => <<"mqtt">>,
            username => <<"default">>,
            password => <<?CLICKHOUSE_PASSWORD>>,
            pool_size => 8,
            url => emqx_bridge_clickhouse_SUITE:clickhouse_url(),
            connect_timeout => <<"10s">>
        },
    #{<<"config">> => maps:merge(Config, Overrides)}.

test_query_no_params() ->
    {query, <<"SELECT 1">>}.
