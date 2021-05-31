-module(emqx_connector_mysql).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

-emqx_resource_api_path("connectors/mysql").

-export([fields/1]).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        ]).

-export([do_health_check/1]).

fields("config") ->
    emqx_connector_schema_lib:relational_db_fields() ++
    emqx_connector_schema_lib:ssl_fields().

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

on_query(InstId, Request, AfterQuery, State) ->
    io:format("== the demo log tracer ~p received request: ~p~nstate: ~p~n",
        [InstId, Request, State]),
    emqx_resource:query_success(AfterQuery),
    "this is a demo log messages...".

on_health_check(_InstId, #{poolname := PoolName} = State) ->
    emqx_plugin_libs_pool:health_check(PoolName, fun ?MODULE:do_health_check/1, State).

do_health_check(Conn) ->
    ok == element(1, mysql:query(Conn, <<"SELECT count(1) AS T">>)).

%% ===================================================================
reconn_interval(true) -> 15;
reconn_interval(false) -> false.
