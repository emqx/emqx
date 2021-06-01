-module(emqx_connector_mysql).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

-emqx_resource_api_path("connectors/mysql").

-export([ fields/1
        , on_config_to_file/1
        ]).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        ]).

-export([connect/1]).

-export([do_health_check/1]).

%%=====================================================================
fields("config") ->
    emqx_connector_schema_lib:relational_db_fields() ++
    emqx_connector_schema_lib:ssl_fields().

on_config_to_file(#{server := Server} = Config) ->
    Config#{server => emqx_connector_schema_lib:ip_port_to_string(Server)}.

%% ===================================================================

on_start(InstId, #{server := {Host, Port},
                   database := DB,
                   user := User,
                   password := Password,
                   auto_reconnect := AutoReconn,
                   pool_size := PoolSize} = Config) ->
    logger:info("starting mysql connector: ~p, config: ~p", [InstId, Config]),
    SslOpts = case maps:get(ssl, Config) of
        true ->
            [{ssl, [{server_name_indication, disable} |
                    emqx_plugin_libs_ssl:save_files_return_opts(Config, "connectors", InstId)]}];
        false ->
            []
    end,
    Options = [{host, Host},
               {port, Port},
               {user, User},
               {password, Password},
               {database, DB},
               {auto_reconnect, reconn_interval(AutoReconn)},
               {pool_size, PoolSize}],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    _ = emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options ++ SslOpts),
    {ok, #{poolname => PoolName}}.

on_stop(InstId, #{poolname := PoolName}) ->
    logger:info("stopping mysql connector: ~p", [InstId]),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {sql, SQL}, AfterQuery, #{poolname := PoolName} = State) ->
    logger:debug("mysql connector ~p received sql query: ~p, at state: ~p", [InstId, SQL, State]),
    case Result = ecpool:pick_and_do(PoolName, {mysql, query, [SQL]}, no_handover) of
        {error, Reason} ->
            logger:debug("mysql connector ~p do sql query failed, sql: ~p, reason: ~p", [InstId, SQL, Reason]),
            emqx_resource:query_failed(AfterQuery);
        _ ->
            emqx_resource:query_success(AfterQuery)
    end,
    Result.

on_health_check(_InstId, #{poolname := PoolName} = State) ->
    emqx_plugin_libs_pool:health_check(PoolName, fun ?MODULE:do_health_check/1, State).

do_health_check(Conn) ->
    ok == element(1, mysql:query(Conn, <<"SELECT count(1) AS T">>)).

%% ===================================================================
reconn_interval(true) -> 15;
reconn_interval(false) -> false.

connect(Options) ->
    mysql:start_link(Options).
