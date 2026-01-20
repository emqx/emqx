%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_postgresql_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(PGSQL_HOST, "toxiproxy").
-define(PGSQL_RESOURCE, <<"emqx_auth_postgresql_connector_SUITE">>).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_postgresql], #{
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
    _ = emqx_resource:remove_local(?PGSQL_RESOURCE),
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create_simple(_Config) ->
    ?assertMatch(
        {ok, _}, create_resource(?PGSQL_RESOURCE, pgsql_config())
    ),
    ?assertMatch(
        ok,
        remove_resource(?PGSQL_RESOURCE)
    ).

t_query(_Config) ->
    {ok, _} = create_resource(?PGSQL_RESOURCE, pgsql_config()),
    ?assertMatch(
        {ok, _, [{1}]},
        emqx_resource:simple_sync_query(?PGSQL_RESOURCE, {query, <<"SELECT 1">>})
    ).

t_query_arguments(_Config) ->
    {ok, _} = create_resource(?PGSQL_RESOURCE, pgsql_config()),
    ?assertMatch(
        {ok, _, [{123}]},
        emqx_resource:simple_sync_query(?PGSQL_RESOURCE, {query, <<"SELECT $1::int">>, [123]})
    ).

t_prepared_query(_Config) ->
    {ok, _} = create_resource(
        ?PGSQL_RESOURCE, pgsql_config(#{prepare_statements => #{test => <<"SELECT 123">>}})
    ),
    ?assertMatch(
        {ok, _, [{123}]},
        emqx_resource:simple_sync_query(?PGSQL_RESOURCE, {prepared_query, test, []})
    ).

t_prepared_query_arguments(_Config) ->
    {ok, _} = create_resource(
        ?PGSQL_RESOURCE, pgsql_config(#{prepare_statements => #{test => <<"SELECT $1::int">>}})
    ),
    ?assertMatch(
        {ok, _, [{123}]},
        emqx_resource:simple_sync_query(?PGSQL_RESOURCE, {prepared_query, test, [123]})
    ).

t_emulate_prepared_statements(_Config) ->
    {ok, _} = create_resource(
        ?PGSQL_RESOURCE,
        pgsql_config(#{
            prepare_statements => #{test => <<"SELECT $1::int">>},
            emulate_prepared_statements => true
        })
    ),
    ?assertMatch(
        {ok, _, [{456}]},
        emqx_resource:simple_sync_query(?PGSQL_RESOURCE, {prepared_query, test, [456]})
    ).

t_prepared_query_key_not_found(_Config) ->
    {ok, _} = create_resource(
        ?PGSQL_RESOURCE, pgsql_config(#{prepare_statements => #{test => <<"SELECT 1">>}})
    ),
    ?assertMatch(
        {error, {unrecoverable_error, prepared_statement_not_found}},
        emqx_resource:simple_sync_query(?PGSQL_RESOURCE, {prepared_query, nonexistent, []})
    ).

t_create_unavailable_and_recover(_Config) ->
    emqx_common_test_helpers:with_failure(down, "pgsql_tcp", ?PROXY_HOST, ?PROXY_PORT, fun() ->
        {ok, _} = create_resource(?PGSQL_RESOURCE, pgsql_config()),
        ?assertEqual({ok, disconnected}, emqx_resource:health_check(?PGSQL_RESOURCE))
    end),
    ?retry(
        200,
        10,
        ?assertMatch({ok, connected}, emqx_resource:health_check(?PGSQL_RESOURCE))
    ).

t_unavailable_query(_Config) ->
    {ok, _} = create_resource(?PGSQL_RESOURCE, pgsql_config()),
    {ok, connected} = emqx_resource:health_check(?PGSQL_RESOURCE),
    emqx_common_test_helpers:with_failure(
        timeout,
        "pgsql_tcp",
        ?PROXY_HOST,
        ?PROXY_PORT,
        fun() ->
            ?assertMatch(
                {error, _},
                emqx_resource:simple_sync_query(
                    ?PGSQL_RESOURCE, {query, <<"SELECT 1">>}
                )
            )
        end
    ).

t_unavailable_health_check(_Config) ->
    {ok, _} = create_resource(?PGSQL_RESOURCE, pgsql_config()),
    {ok, connected} = emqx_resource:health_check(?PGSQL_RESOURCE),
    emqx_common_test_helpers:with_failure(
        timeout,
        "pgsql_tcp",
        ?PROXY_HOST,
        ?PROXY_PORT,
        fun() ->
            ?retry(
                200,
                10,
                ?assertMatch({ok, disconnected}, emqx_resource:health_check(?PGSQL_RESOURCE))
            )
        end
    ).

t_cannot_prepare_statement(_Config) ->
    {ok, _} = create_resource(
        ?PGSQL_RESOURCE,
        pgsql_config(#{prepare_statements => #{test => <<"SELECT * FROM non_existent_table">>}})
    ),
    {ok, disconnected} = emqx_resource:health_check(?PGSQL_RESOURCE).

t_table_removed(_Config) ->
    {ok, _} = create_resource(<<"admin_conn">>, pgsql_config()),
    emqx_resource:simple_sync_query(
        <<"admin_conn">>, {query, <<"CREATE TABLE IF NOT EXISTS test_table (id INT)">>}
    ),
    {ok, _} = create_resource(
        ?PGSQL_RESOURCE,
        pgsql_config(#{
            prepare_statements => #{test => <<"SELECT * FROM test_table WHERE id = $1">>}
        })
    ),
    ?assertMatch(
        {ok, connected},
        emqx_resource:health_check(?PGSQL_RESOURCE)
    ),
    emqx_resource:simple_sync_query(<<"admin_conn">>, {query, <<"DROP TABLE test_table">>}),
    ?assertMatch(
        {ok, disconnected},
        emqx_resource:health_check(?PGSQL_RESOURCE)
    ),
    emqx_resource:simple_sync_query(
        <<"admin_conn">>, {query, <<"CREATE TABLE IF NOT EXISTS test_table (id INT)">>}
    ),
    ?retry(
        1000,
        10,
        ?assertMatch({ok, connected}, emqx_resource:health_check(?PGSQL_RESOURCE))
    ),
    ok.

t_cannot_start_due_to_invalid_prepare_statement(_Config) ->
    {ok, _} = create_resource(
        ?PGSQL_RESOURCE,
        pgsql_config(#{prepare_statements => #{test => <<"SELECT * FROM non_existent_table">>}})
    ),
    ?assertMatch(
        {ok, disconnected},
        emqx_resource:health_check(?PGSQL_RESOURCE)
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
        emqx_auth_postgresql_connector,
        Config,
        Opts
    ).

remove_resource(Id) ->
    emqx_resource:remove_local(Id).

pgsql_config(Config) ->
    maps:merge(pgsql_config(), Config).

pgsql_config() ->
    #{
        auto_reconnect => true,
        connect_timeout => 500,
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 8,
        server => <<?PGSQL_HOST>>,
        ssl => #{enable => false}
    }.
