%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_mysql_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(MYSQL_HOST, "toxiproxy").
-define(MYSQL_RESOURCE, <<"emqx_authn_mysql_connector_SUITE">>).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_mysql], #{
        work_dir => ?config(priv_dir, Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_, Config) ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_, _Config) ->
    snabbkaffe:stop(),
    _ = emqx_resource:remove_local(?MYSQL_RESOURCE),
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create_simple(_Config) ->
    ?assertMatch(
        {ok, _}, create_resource(?MYSQL_RESOURCE, mysql_config())
    ),
    ?assertMatch(
        ok,
        remove_resource(?MYSQL_RESOURCE)
    ).

t_create_unavailable_and_recover(_Config) ->
    emqx_common_test_helpers:with_failure(down, "mysql_tcp", ?PROXY_HOST, ?PROXY_PORT, fun() ->
        {ok, _} = create_resource(?MYSQL_RESOURCE, mysql_config()),
        ?assertEqual({ok, disconnected}, emqx_resource:health_check(?MYSQL_RESOURCE))
    end),
    ?retry(
        200,
        10,
        ?assertMatch({ok, connected}, emqx_resource:health_check(?MYSQL_RESOURCE))
    ).

t_query(_Config) ->
    {ok, _} = create_resource(?MYSQL_RESOURCE, mysql_config()),
    ?assertMatch(
        {ok, _, [[1]]}, emqx_resource:simple_sync_query(?MYSQL_RESOURCE, {query, <<"SELECT 1">>})
    ).

t_query_arguments(_Config) ->
    {ok, _} = create_resource(?MYSQL_RESOURCE, mysql_config()),
    ?assertMatch(
        {ok, _, [[123]]},
        emqx_resource:simple_sync_query(?MYSQL_RESOURCE, {query, <<"SELECT ?">>, [123]})
    ).

t_execute(_Config) ->
    {ok, _} = create_resource(
        ?MYSQL_RESOURCE, mysql_config(#{prepare_statements => #{test => <<"SELECT 123">>}})
    ),
    ?assertMatch(
        {ok, _, [[123]]},
        emqx_resource:simple_sync_query(?MYSQL_RESOURCE, {prepared_query, test})
    ).

t_execute_arguments(_Config) ->
    {ok, _} = create_resource(
        ?MYSQL_RESOURCE, mysql_config(#{prepare_statements => #{test => <<"SELECT ?">>}})
    ),
    ?assertMatch(
        {ok, _, [[123]]},
        emqx_resource:simple_sync_query(?MYSQL_RESOURCE, {prepared_query, test, [123]})
    ).

t_execute_key_not_found(_Config) ->
    {ok, _} = create_resource(
        ?MYSQL_RESOURCE, mysql_config(#{prepare_statements => #{test => <<"SELECT 123">>}})
    ),
    ?assertMatch(
        {error, {unrecoverable_error, {prepared_statement_not_found, nonexistent}}},
        emqx_resource:simple_sync_query(?MYSQL_RESOURCE, {prepared_query, nonexistent})
    ).

t_emulate_prepared_statements(_Config) ->
    {ok, _} = create_resource(
        ?MYSQL_RESOURCE,
        mysql_config(#{
            prepare_statements => #{test => <<"SELECT 123">>}, emulate_prepared_statements => true
        })
    ),
    ?assertMatch(
        {ok, _, [[123]]},
        emqx_resource:simple_sync_query(?MYSQL_RESOURCE, {prepared_query, test})
    ).

t_unavailable_query(_Config) ->
    {ok, _} = create_resource(?MYSQL_RESOURCE, mysql_config()),
    {ok, connected} = emqx_resource:health_check(?MYSQL_RESOURCE),
    emqx_common_test_helpers:with_failure(
        timeout,
        "mysql_tcp",
        ?PROXY_HOST,
        ?PROXY_PORT,
        fun() ->
            ?assertMatch(
                {error, _},
                emqx_resource:simple_sync_query(
                    ?MYSQL_RESOURCE, {query, <<"SELECT 1">>, #{timeout => 200}}
                )
            )
        end
    ).

t_unavailable_health_check(_Config) ->
    {ok, _} = create_resource(?MYSQL_RESOURCE, mysql_config()),
    {ok, connected} = emqx_resource:health_check(?MYSQL_RESOURCE),
    emqx_common_test_helpers:with_failure(
        timeout,
        "mysql_tcp",
        ?PROXY_HOST,
        ?PROXY_PORT,
        fun() ->
            ?retry(
                200,
                10,
                ?assertMatch({ok, disconnected}, emqx_resource:health_check(?MYSQL_RESOURCE))
            )
        end
    ).

t_cannot_prepare_statement(_Config) ->
    {ok, _} = create_resource(
        ?MYSQL_RESOURCE,
        mysql_config(#{prepare_statements => #{test => <<"SELECT * FROM non_existent_table">>}})
    ),
    {ok, disconnected} = emqx_resource:health_check(?MYSQL_RESOURCE).

t_table_removed(_Config) ->
    {ok, _} = create_resource(<<"admin_conn">>, mysql_config()),
    emqx_resource:simple_sync_query(
        <<"admin_conn">>, {query, <<"CREATE TABLE IF NOT EXISTS test_table (id INT)">>}
    ),
    {ok, _} = create_resource(
        ?MYSQL_RESOURCE,
        mysql_config(#{prepare_statements => #{test => <<"SELECT * FROM test_table WHERE id = ?">>}})
    ),
    ?assertMatch(
        {ok, connected},
        emqx_resource:health_check(?MYSQL_RESOURCE)
    ),
    emqx_resource:simple_sync_query(<<"admin_conn">>, {query, <<"DROP TABLE test_table">>}),
    ?assertMatch(
        {ok, disconnected},
        emqx_resource:health_check(?MYSQL_RESOURCE)
    ),
    emqx_resource:simple_sync_query(
        <<"admin_conn">>, {query, <<"CREATE TABLE IF NOT EXISTS test_table (id INT)">>}
    ),
    ?retry(
        1000,
        10,
        ?assertMatch({ok, connected}, emqx_resource:health_check(?MYSQL_RESOURCE))
    ),
    ok.

t_cannot_start_due_to_invalid_prepare_statement(_Config) ->
    {ok, _} = create_resource(
        ?MYSQL_RESOURCE,
        mysql_config(#{prepare_statements => #{test => <<"SELECT * FROM non_existent_table">>}})
    ),
    ?assertMatch(
        {ok, disconnected},
        emqx_resource:health_check(?MYSQL_RESOURCE)
    ).

%%------------------------------------------------------------------------------
%% Helper Functions
%%------------------------------------------------------------------------------

create_resource(Id, Config) ->
    create_resource(Id, Config, #{
        health_check_interval => 1000,
        start_timeout => 500,
        start_after_created => true
    }).

create_resource(Id, Config, Opts) ->
    emqx_resource:create_local(
        Id,
        ?AUTHN_RESOURCE_GROUP,
        emqx_auth_mysql_connector,
        Config,
        Opts
    ).

remove_resource(Id) ->
    emqx_resource:remove_local(Id).

mysql_config(Config) ->
    maps:merge(mysql_config(), Config).

mysql_config() ->
    #{
        auto_reconnect => true,
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 8,
        server => <<?MYSQL_HOST, ":3306">>,
        ssl => #{enable => false}
    }.
