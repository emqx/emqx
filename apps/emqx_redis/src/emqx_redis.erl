%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_redis).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-export([namespace/0, roots/0, fields/1, redis_fields/0, desc/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

-export([do_get_status/1]).

-export([connect/1]).

-export([do_cmd/3]).

%% internal exports for redis-based modules
-export([is_auth_error/1]).

-ifdef(TEST).
-export([map_result/1]).
-endif.

%% redis host don't need parse
-define(REDIS_HOST_OPTIONS, #{
    default_port => ?REDIS_DEFAULT_PORT
}).
-define(SENTINEL_VALIDATION_CLIENT, sentinel_validation_client).

%%=====================================================================
namespace() -> "redis".

roots() ->
    [
        {config, #{
            type => hoconsc:union(
                [
                    ?R_REF(redis_cluster),
                    ?R_REF(redis_single),
                    ?R_REF(redis_sentinel)
                ]
            )
        }}
    ].

fields(redis_single) ->
    fields(redis_single_connector) ++
        emqx_connector_schema_lib:ssl_fields();
fields(redis_single_connector) ->
    [
        {server, server()},
        redis_type(single)
    ] ++ redis_fields();
fields(redis_cluster) ->
    fields(redis_cluster_connector) ++
        emqx_connector_schema_lib:ssl_fields();
fields(redis_cluster_connector) ->
    [
        {servers, servers()},
        redis_type(cluster)
    ] ++ lists:keydelete(database, 1, redis_fields());
fields(redis_sentinel) ->
    fields(redis_sentinel_connector) ++
        emqx_connector_schema_lib:ssl_fields();
fields(redis_sentinel_connector) ->
    [
        {servers, servers()},
        redis_type(sentinel),
        {sentinel, #{
            type => string(),
            required => true,
            desc => ?DESC("sentinel_desc")
        }},
        {sentinel_username, fun sentinel_username/1},
        {sentinel_password,
            emqx_connector_schema_lib:password_field(#{desc => ?DESC("sentinel_password")})}
    ] ++ redis_fields().

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?REDIS_HOST_OPTIONS).

servers() ->
    Meta = #{desc => ?DESC("servers")},
    emqx_schema:servers_sc(Meta, ?REDIS_HOST_OPTIONS).

desc(redis_cluster_connector) ->
    ?DESC(redis_cluster_connector);
desc(redis_single_connector) ->
    ?DESC(redis_single_connector);
desc(redis_sentinel_connector) ->
    ?DESC(redis_sentinel_connector);
desc(_) ->
    undefined.

%% ===================================================================

redis_type(Type) ->
    {redis_type, #{
        type => Type,
        default => Type,
        required => false,
        desc => ?DESC(Type)
    }}.

resource_type() -> redis.

callback_mode() -> always_sync.

on_start(InstId, Config0) ->
    ?SLOG(info, #{
        msg => "starting_redis_connector",
        connector => InstId,
        config => emqx_utils:redact(Config0)
    }),
    Config = config(Config0),
    #{pool_size := PoolSize, ssl := SSL, redis_type := Type} = Config,
    BaseOptions = ssl_options(SSL) ++ [{sentinel, maps:get(sentinel, Config, undefined)}],
    Options = runtime_options(Type, InstId, BaseOptions),
    Opts =
        [
            {username, maps:get(username, Config, undefined)},
            {password, maps:get(password, Config, "")},
            {servers, servers(Config)},
            {options, Options},
            {pool_size, PoolSize},
            {auto_reconnect, ?AUTO_RECONNECT_INTERVAL}
        ] ++ sentinel_auth(Config) ++ database(Config),
    HCTimeout =
        case Config of
            #{resource_opts := #{health_check_timeout := HCTimeout0}} ->
                HCTimeout0;
            #{} ->
                emqx_resource_pool:health_check_timeout()
        end,
    State = #{
        health_check_timeout => HCTimeout,
        pool_name => InstId,
        type => Type
    },
    ok = emqx_resource:allocate_resource(InstId, ?MODULE, type, Type),
    ok = emqx_resource:allocate_resource(InstId, ?MODULE, pool_name, InstId),
    case validate_sentinel_connection(Type, InstId, Config, BaseOptions) of
        ok ->
            do_start(Type, InstId, Opts, State);
        {error, Reason} ->
            {error, Reason}
    end.

do_start(cluster, InstId, Opts, State) ->
    case eredis_cluster:start_pool(InstId, Opts) of
        {ok, _} ->
            {ok, State};
        {ok, _, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
do_start(_Type, InstId, Opts, State) ->
    case emqx_resource_pool:start(InstId, ?MODULE, Opts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            ok = stop_sentinel_manager(State),
            {error, Reason}
    end.

runtime_options(sentinel, InstId, Options) ->
    [{sentinel_manager_ref, sentinel_manager_ref(InstId)} | Options];
runtime_options(_Type, _InstId, Options) ->
    Options.

sentinel_manager_ref(InstId) ->
    {?MODULE, InstId}.

validate_sentinel_connection(sentinel, InstId, Config, Options) ->
    Servers = servers(Config),
    Sentinel = maps:get(sentinel, Config),
    SentinelOptions = sentinel_client_options(Config, Options),
    validate_sentinel_servers(InstId, Servers, Sentinel, SentinelOptions, []);
validate_sentinel_connection(_Type, _InstId, _Config, _Options) ->
    ok.

validate_sentinel_servers(_InstId, [], _Sentinel, _Options, Errors) ->
    {error, {sentinel_error, sentinel_error_reason(Errors)}};
validate_sentinel_servers(InstId, [{Host, Port} | Rest], Sentinel, Options, Errors) ->
    case eredis_sentinel_client:start_link(Host, Port, Options) of
        {ok, Pid} ->
            ok = emqx_resource:allocate_resource(InstId, ?MODULE, ?SENTINEL_VALIDATION_CLIENT, Pid),
            try query_sentinel_master(Pid, Sentinel) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    validate_sentinel_servers(InstId, Rest, Sentinel, Options, [Reason | Errors])
            after
                ok = cleanup_sentinel_validation_client(InstId, Pid)
            end;
        {error, Reason} ->
            validate_sentinel_servers(InstId, Rest, Sentinel, Options, [Reason | Errors])
    end.

sentinel_error_reason([Reason | _]) ->
    Reason;
sentinel_error_reason([]) ->
    sentinel_unreachable.

query_sentinel_master(Pid, Sentinel) ->
    Command = ["SENTINEL", "get-master-addr-by-name", Sentinel],
    try sentinel_master_response(eredis:q(Pid, Command)) of
        Result ->
            Result
    catch
        _:_ ->
            {error, sentinel_unreachable}
    end.

sentinel_master_response({ok, [_HostBin, PortBin]}) when is_binary(PortBin) ->
    _ = binary_to_integer(PortBin),
    {ok, connected};
sentinel_master_response({ok, undefined}) ->
    {error, sentinel_master_unknown};
sentinel_master_response({error, <<"IDONTKNOW", _Rest/binary>>}) ->
    {error, sentinel_master_unreachable};
sentinel_master_response({error, Reason}) ->
    {error, Reason};
sentinel_master_response(_) ->
    {error, sentinel_unreachable}.

sentinel_client_options(Config, Options) ->
    Options0 = lists:keydelete(sentinel, 1, Options),
    Options1 = prepend_option(password, maps:get(sentinel_password, Config, ""), Options0),
    prepend_option(username, maps:get(sentinel_username, Config, undefined), Options1).

prepend_option(_Key, undefined, Options) ->
    Options;
prepend_option(Key, Value, Options) ->
    [{Key, Value} | lists:keydelete(Key, 1, Options)].

cleanup_sentinel_validation_client(InstId, Pid) ->
    ok = stop_sentinel_validation_client(Pid),
    ok = emqx_resource:deallocate_resource(InstId, ?SENTINEL_VALIDATION_CLIENT).

stop_sentinel_validation_client(#{?SENTINEL_VALIDATION_CLIENT := Pid}) ->
    stop_sentinel_validation_client(Pid);
stop_sentinel_validation_client(Pid) when is_pid(Pid) ->
    eredis_sentinel_client:stop(Pid),
    ok;
stop_sentinel_validation_client(_Resources) ->
    ok.

ssl_options(SSL = #{enable := true}) ->
    [
        {ssl, true},
        {ssl_options, emqx_tls_lib:to_client_opts(SSL)}
    ];
ssl_options(#{enable := false}) ->
    [{ssl, false}].

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_redis_connector",
        connector => InstId
    }),
    Resources = emqx_resource:get_allocated_resources(InstId),
    ok = stop_sentinel_validation_client(Resources),
    Res = stop_pool(Resources),
    ok = stop_sentinel_manager(Resources),
    ?tp("redis_connector_stop", #{instance_id => InstId}),
    Res.

stop_pool(Resources) ->
    case Resources of
        #{pool_name := PoolName, type := cluster} ->
            case eredis_cluster:stop_pool(PoolName) of
                {error, not_found} -> ok;
                ok -> ok;
                Error -> Error
            end;
        #{pool_name := PoolName, type := _} ->
            emqx_resource_pool:stop(PoolName);
        _ ->
            ok
    end.

stop_sentinel_manager(#{pool_name := PoolName, type := sentinel}) ->
    eredis:stop_sentinel_manager(sentinel_manager_ref(PoolName));
stop_sentinel_manager(_Resources) ->
    ok.

on_query(InstId, {cmd, _} = Query, State) ->
    do_query(InstId, Query, State);
on_query(InstId, {cmds, _} = Query, State) ->
    do_query(InstId, Query, State).

do_query(InstId, Query, #{pool_name := PoolName, type := Type} = State) ->
    ?TRACE(
        "QUERY",
        "redis_connector_received",
        #{connector => InstId, query => Query, state => State}
    ),
    Result =
        case Type of
            cluster -> do_cmd(PoolName, cluster, Query);
            _ -> ecpool:pick_and_do(PoolName, {?MODULE, do_cmd, [Type, Query]}, no_handover)
        end,
    case Result of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "redis_connector_do_query_failed",
                connector => InstId,
                query => Query,
                reason => Reason
            });
        _ ->
            ok
    end,
    map_result(Result).

map_result(Results0) when is_list(Results0) ->
    {Results, Tag} = lists:mapfoldl(
        fun(Res0, Tag0) ->
            case map_result1(Res0) of
                {error, _} = Res ->
                    {Res, error};
                Res ->
                    {Res, Tag0}
            end
        end,
        ok,
        Results0
    ),
    {Tag, Results};
map_result(Result) ->
    map_result1(Result).

map_result1(ok) ->
    ok;
map_result1({ok, _} = Result) ->
    Result;
map_result1({error, Reason}) ->
    map_error(Reason).

map_error(Results) when is_list(Results) ->
    [map_error1(R) || R <- Results];
map_error(Reason) ->
    map_error1(Reason).

map_error1(Reason) ->
    case is_unrecoverable_error(Reason) of
        true ->
            {error, {unrecoverable_error, Reason}};
        false ->
            {error, {recoverable_error, Reason}}
    end.

is_unrecoverable_error({error, {unrecoverable_error, _}}) ->
    true;
is_unrecoverable_error({error, {recoverable_error, _}}) ->
    false;
is_unrecoverable_error({error, Reason}) ->
    is_unrecoverable_error(Reason);
is_unrecoverable_error(<<"ERR unknown command ", _/binary>>) ->
    true;
is_unrecoverable_error(invalid_cluster_command) ->
    true;
is_unrecoverable_error(<<"ERR ", _/binary>>) ->
    true;
is_unrecoverable_error(<<"WRONGTYPE ", _/binary>>) ->
    true;
is_unrecoverable_error(<<"NOPERM ", _/binary>>) ->
    true;
is_unrecoverable_error(<<"WRONGPASS ", _/binary>>) ->
    true;
is_unrecoverable_error(_) ->
    false.

on_get_status(_InstId, #{type := cluster, pool_name := PoolName}) ->
    case eredis_cluster:pool_exists(PoolName) of
        true ->
            %% eredis_cluster has null slot even pool_exists when emqx start before redis cluster.
            %% we need restart eredis_cluster pool when pool_worker(slot) is empty.
            %% If the pool is empty, it means that there are no workers attempting to reconnect.
            %% In this case, we can directly consider it as a disconnect and then proceed to reconnect.
            case eredis_cluster_monitor:get_all_pools(PoolName) of
                [] ->
                    ?status_disconnected;
                [_ | _] ->
                    do_cluster_status_check(PoolName)
            end;
        false ->
            ?status_disconnected
    end;
on_get_status(_InstId, ConnState) ->
    #{
        health_check_timeout := HCTimeout,
        pool_name := PoolName
    } = ConnState,
    HealthCheckResoults = emqx_resource_pool:health_check_workers(
        PoolName,
        fun ?MODULE:do_get_status/1,
        HCTimeout,
        #{return_values => true}
    ),
    case HealthCheckResoults of
        {ok, Results} ->
            sum_worker_results(Results);
        Error ->
            {?status_disconnected, Error}
    end.

do_cluster_status_check(Pool) ->
    Pongs = eredis_cluster:qa(Pool, [<<"PING">>]),
    sum_worker_results(Pongs).

do_get_status(Conn) ->
    eredis:q(Conn, ["PING"]).

sum_worker_results([]) ->
    ?status_connected;
sum_worker_results([{error, Reason} = Error | _Rest]) when is_binary(Reason) ->
    case is_auth_error(Reason) of
        true ->
            ?tp(emqx_redis_auth_required_error, #{}),
            %% This requires user action to fix so we set the status to disconnected
            {?status_disconnected, {unhealthy_target, Error}};
        false ->
            ?SLOG(
                warning,
                #{
                    msg => "emqx_redis_check_status_error",
                    error => Error
                }
            ),
            {?status_disconnected, Error}
    end;
sum_worker_results([{ok, _} | Rest]) ->
    sum_worker_results(Rest);
sum_worker_results([Error | _Rest]) ->
    ?SLOG(
        warning,
        #{
            msg => "emqx_redis_check_status_error",
            error => Error
        }
    ),
    {?status_disconnected, Error}.

do_cmd(PoolName, cluster, {cmd, Command}) ->
    eredis_cluster:q(PoolName, Command);
do_cmd(Conn, _Type, {cmd, Command}) ->
    eredis:q(Conn, Command);
do_cmd(PoolName, cluster, {cmds, Commands}) ->
    % TODO
    % Cluster mode is currently incompatible with batching.
    [eredis_cluster:q(PoolName, Command) || Command <- Commands];
do_cmd(Conn, _Type, {cmds, Commands}) ->
    eredis:qp(Conn, Commands).

%% ===================================================================
%% parameters for connector
config(#{parameters := #{} = Param} = Config) ->
    maps:merge(maps:remove(parameters, Config), Param);
%% is for authn/authz
config(Config) ->
    Config.

servers(#{server := Server}) ->
    servers(Server);
servers(#{servers := Servers}) ->
    servers(Servers);
servers(Servers) ->
    lists:map(
        fun(#{hostname := Host, port := Port}) ->
            {Host, Port}
        end,
        emqx_schema:parse_servers(Servers, ?REDIS_HOST_OPTIONS)
    ).

database(#{redis_type := cluster}) -> [];
database(#{database := Database}) -> [{database, Database}].

connect(Opts) ->
    eredis:start_link(Opts).

redis_fields() ->
    [
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun emqx_connector_schema_lib:username/1},
        {password, emqx_connector_schema_lib:password_field()},
        {database, #{
            type => non_neg_integer(),
            default => 0,
            desc => ?DESC("database")
        }},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

sentinel_username(type) -> binary();
sentinel_username(desc) -> ?DESC("sentinel_username");
sentinel_username(required) -> false;
sentinel_username(_) -> undefined.

sentinel_auth(#{redis_type := sentinel} = Config) ->
    [
        {sentinel_username, maps:get(sentinel_username, Config, undefined)},
        {sentinel_password, maps:get(sentinel_password, Config, "")}
    ];
sentinel_auth(_) ->
    [].

%% Sometimes, eredis_client might return an error during authentication phase (hence
%% tagged authentication_error) that's not actually an authentication error, such as
%% `{unexpected_data, {error, timeout}}`.
is_auth_error(<<"-", Rest/binary>>) ->
    is_auth_error1(Rest);
is_auth_error(Reason) when is_binary(Reason) ->
    is_auth_error1(Reason);
is_auth_error(_) ->
    false.

is_auth_error1(<<"WRONGPASS ", _/binary>>) ->
    true;
is_auth_error1(<<"NOAUTH ", _/binary>>) ->
    true;
is_auth_error1(<<"ERR AUTH ", _/binary>>) ->
    true;
is_auth_error1(<<"NOPERM ", _/binary>>) ->
    true;
is_auth_error1(<<"ERR invalid password", _/binary>>) ->
    true;
is_auth_error1(<<"ERR client sent AUTH, but", _/binary>>) ->
    true;
is_auth_error1(_) ->
    false.
