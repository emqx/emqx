%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_connector).

-behaviour(emqx_resource).

-include_lib("jose/include/jose_jwk.hrl").
-include_lib("emqx_connector/include/emqx_connector_tables.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2
]).
-export([reply_delegator/3]).

-export([get_jwt_authorization_header/1]).

-type service_account_json() :: emqx_bridge_gcp_pubsub:service_account_json().
-type config() :: #{
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()},
    service_account_json := service_account_json(),
    any() => term()
}.
-type state() :: #{
    connect_timeout := timer:time(),
    jwt_config := emqx_connector_jwt:jwt_config(),
    max_retries := non_neg_integer(),
    pool_name := binary(),
    project_id := binary(),
    request_ttl := infinity | timer:time()
}.
-type headers() :: [{binary(), iodata()}].
-type body() :: iodata().
-type status_code() :: 100..599.
-type method() :: post.
-type path() :: binary().
-type prepared_request() :: {method(), path(), body()}.

-export_type([service_account_json/0, state/0, headers/0, body/0, status_code/0]).

-define(DEFAULT_PIPELINE_SIZE, 100).

%%-------------------------------------------------------------------------------------------------
%% emqx_resource API
%%-------------------------------------------------------------------------------------------------

callback_mode() -> async_if_possible.

-spec on_start(resource_id(), config()) -> {ok, state()} | {error, term()}.
on_start(
    ResourceId,
    #{
        connect_timeout := ConnectTimeout,
        max_retries := MaxRetries,
        pool_size := PoolSize,
        resource_opts := #{request_ttl := RequestTTL}
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_gcp_pubsub_bridge",
        connector => ResourceId,
        config => Config
    }),
    %% emulating the emulator behavior
    %% https://cloud.google.com/pubsub/docs/emulator
    HostPort = os:getenv("PUBSUB_EMULATOR_HOST", "pubsub.googleapis.com:443"),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(HostPort, #{default_port => 443}),
    PoolType = random,
    Transport = tls,
    TransportOpts = emqx_tls_lib:to_client_opts(#{enable => true, verify => verify_none}),
    NTransportOpts = emqx_utils:ipv6_probe(TransportOpts),
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, ConnectTimeout},
        {keepalive, 30_000},
        {pool_type, PoolType},
        {pool_size, PoolSize},
        {transport, Transport},
        {transport_opts, NTransportOpts},
        {enable_pipelining, maps:get(enable_pipelining, Config, ?DEFAULT_PIPELINE_SIZE)}
    ],
    #{
        jwt_config := JWTConfig,
        project_id := ProjectId
    } = parse_jwt_config(ResourceId, Config),
    State = #{
        connect_timeout => ConnectTimeout,
        jwt_config => JWTConfig,
        max_retries => MaxRetries,
        pool_name => ResourceId,
        project_id => ProjectId,
        request_ttl => RequestTTL
    },
    ?tp(
        gcp_pubsub_on_start_before_starting_pool,
        #{
            resource_id => ResourceId,
            pool_name => ResourceId,
            pool_opts => PoolOpts
        }
    ),
    ?tp(gcp_pubsub_starting_ehttpc_pool, #{pool_name => ResourceId}),
    case ehttpc_sup:start_pool(ResourceId, PoolOpts) of
        {ok, _} ->
            {ok, State};
        {error, {already_started, _}} ->
            ?tp(gcp_pubsub_ehttpc_pool_already_started, #{pool_name => ResourceId}),
            {ok, State};
        {error, Reason} ->
            ?tp(gcp_pubsub_ehttpc_pool_start_failure, #{
                pool_name => ResourceId,
                reason => Reason
            }),
            {error, Reason}
    end.

-spec on_stop(resource_id(), state() | undefined) -> ok | {error, term()}.
on_stop(ResourceId, _State) ->
    ?tp(gcp_pubsub_stop, #{resource_id => ResourceId}),
    ?SLOG(info, #{
        msg => "stopping_gcp_pubsub_bridge",
        connector => ResourceId
    }),
    ok = emqx_connector_jwt:delete_jwt(?JWT_TABLE, ResourceId),
    case ehttpc_sup:stop_pool(ResourceId) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        Error ->
            Error
    end.

-spec on_query(
    resource_id(),
    {prepared_request, prepared_request()},
    state()
) ->
    {ok, status_code(), headers()}
    | {ok, status_code(), headers(), body()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
on_query(ResourceId, {prepared_request, PreparedRequest = {_Method, _Path, _Body}}, State) ->
    ?TRACE(
        "QUERY_SYNC",
        "gcp_pubsub_received",
        #{requests => PreparedRequest, connector => ResourceId, state => State}
    ),
    do_send_requests_sync(State, {prepared_request, PreparedRequest}, ResourceId).

-spec on_query_async(
    resource_id(),
    {prepared_request, prepared_request()},
    {ReplyFun :: function(), Args :: list()},
    state()
) -> {ok, pid()}.
on_query_async(
    ResourceId,
    {prepared_request, PreparedRequest = {_Method, _Path, _Body}},
    ReplyFunAndArgs,
    State
) ->
    ?TRACE(
        "QUERY_ASYNC",
        "gcp_pubsub_received",
        #{requests => PreparedRequest, connector => ResourceId, state => State}
    ),
    do_send_requests_async(State, {prepared_request, PreparedRequest}, ReplyFunAndArgs, ResourceId).

-spec on_get_status(resource_id(), state()) -> connected | disconnected.
on_get_status(ResourceId, #{connect_timeout := Timeout} = State) ->
    case do_get_status(ResourceId, Timeout) of
        true ->
            connected;
        false ->
            ?SLOG(error, #{
                msg => "gcp_pubsub_bridge_get_status_failed",
                state => State
            }),
            disconnected
    end.

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

-spec parse_jwt_config(resource_id(), config()) ->
    #{
        jwt_config := emqx_connector_jwt:jwt_config(),
        project_id := binary()
    }.
parse_jwt_config(ResourceId, #{
    service_account_json := ServiceAccountJSON
}) ->
    #{
        project_id := ProjectId,
        private_key_id := KId,
        private_key := PrivateKeyPEM,
        client_email := ServiceAccountEmail
    } = ServiceAccountJSON,
    %% fixed for pubsub; trailing slash is important.
    Aud = <<"https://pubsub.googleapis.com/">>,
    ExpirationMS = timer:hours(1),
    Alg = <<"RS256">>,
    JWK =
        try jose_jwk:from_pem(PrivateKeyPEM) of
            JWK0 = #jose_jwk{} ->
                %% Don't wrap the JWK with `emqx_secret:wrap' here;
                %% this is stored in mnesia and synchronized among the
                %% nodes, and will easily become a bad fun.
                JWK0;
            [] ->
                ?tp(error, gcp_pubsub_connector_startup_error, #{error => empty_key}),
                throw("empty private in service account json");
            {error, Reason} ->
                Error = {invalid_private_key, Reason},
                ?tp(error, gcp_pubsub_connector_startup_error, #{error => Error}),
                throw("invalid private key in service account json");
            Error0 ->
                Error = {invalid_private_key, Error0},
                ?tp(error, gcp_pubsub_connector_startup_error, #{error => Error}),
                throw("invalid private key in service account json")
        catch
            Kind:Reason ->
                Error = {Kind, Reason},
                ?tp(error, gcp_pubsub_connector_startup_error, #{error => Error}),
                throw("invalid private key in service account json")
        end,
    JWTConfig = #{
        jwk => emqx_secret:wrap(JWK),
        resource_id => ResourceId,
        expiration => ExpirationMS,
        table => ?JWT_TABLE,
        iss => ServiceAccountEmail,
        sub => ServiceAccountEmail,
        aud => Aud,
        kid => KId,
        alg => Alg
    },
    #{
        jwt_config => JWTConfig,
        project_id => ProjectId
    }.

-spec get_jwt_authorization_header(emqx_connector_jwt:jwt_config()) -> [{binary(), binary()}].
get_jwt_authorization_header(JWTConfig) ->
    JWT = emqx_connector_jwt:ensure_jwt(JWTConfig),
    [{<<"Authorization">>, <<"Bearer ", JWT/binary>>}].

-spec do_send_requests_sync(
    state(),
    {prepared_request, prepared_request()},
    resource_id()
) ->
    {ok, status_code(), headers()}
    | {ok, status_code(), headers(), body()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
do_send_requests_sync(State, {prepared_request, {Method, Path, Body}}, ResourceId) ->
    #{
        jwt_config := JWTConfig,
        pool_name := PoolName,
        max_retries := MaxRetries,
        request_ttl := RequestTTL
    } = State,
    ?tp(
        gcp_pubsub_bridge_do_send_requests,
        #{
            query_mode => sync,
            resource_id => ResourceId
        }
    ),
    Headers = get_jwt_authorization_header(JWTConfig),
    Request = {Path, Headers, Body},
    case
        ehttpc:request(
            PoolName,
            Method,
            Request,
            RequestTTL,
            MaxRetries
        )
    of
        {error, Reason} when
            Reason =:= econnrefused;
            %% this comes directly from `gun'...
            Reason =:= {closed, "The connection was lost."};
            Reason =:= timeout
        ->
            ?tp(
                warning,
                gcp_pubsub_request_failed,
                #{
                    reason => Reason,
                    query_mode => sync,
                    recoverable_error => true,
                    connector => ResourceId
                }
            ),
            {error, {recoverable_error, Reason}};
        {error, Reason} = Result ->
            ?tp(
                error,
                gcp_pubsub_request_failed,
                #{
                    reason => Reason,
                    query_mode => sync,
                    recoverable_error => false,
                    connector => ResourceId
                }
            ),
            Result;
        {ok, StatusCode, _} = Result when StatusCode >= 200 andalso StatusCode < 300 ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => Result,
                    query_mode => sync,
                    connector => ResourceId
                }
            ),
            Result;
        {ok, StatusCode, _, _} = Result when StatusCode >= 200 andalso StatusCode < 300 ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => Result,
                    query_mode => sync,
                    connector => ResourceId
                }
            ),
            Result;
        {ok, StatusCode, RespHeaders} = _Result ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => _Result,
                    query_mode => sync,
                    connector => ResourceId
                }
            ),
            ?SLOG(error, #{
                msg => "gcp_pubsub_error_response",
                request => Request,
                connector => ResourceId,
                status_code => StatusCode
            }),
            {error, #{status_code => StatusCode, headers => RespHeaders}};
        {ok, StatusCode, RespHeaders, RespBody} = _Result ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => _Result,
                    query_mode => sync,
                    connector => ResourceId
                }
            ),
            ?SLOG(error, #{
                msg => "gcp_pubsub_error_response",
                request => Request,
                connector => ResourceId,
                status_code => StatusCode
            }),
            {error, #{status_code => StatusCode, headers => RespHeaders, body => RespBody}}
    end.

-spec do_send_requests_async(
    state(),
    {prepared_request, prepared_request()},
    {ReplyFun :: function(), Args :: list()},
    resource_id()
) -> {ok, pid()}.
do_send_requests_async(
    State, {prepared_request, {Method, Path, Body}}, ReplyFunAndArgs, ResourceId
) ->
    #{
        jwt_config := JWTConfig,
        pool_name := PoolName,
        request_ttl := RequestTTL
    } = State,
    ?tp(
        gcp_pubsub_bridge_do_send_requests,
        #{
            query_mode => async,
            resource_id => ResourceId
        }
    ),
    Headers = get_jwt_authorization_header(JWTConfig),
    Request = {Path, Headers, Body},
    Worker = ehttpc_pool:pick_worker(PoolName),
    ok = ehttpc:request_async(
        Worker,
        Method,
        Request,
        RequestTTL,
        {fun ?MODULE:reply_delegator/3, [ResourceId, ReplyFunAndArgs]}
    ),
    {ok, Worker}.

-spec reply_delegator(
    resource_id(),
    {ReplyFun :: function(), Args :: list()},
    term() | {error, econnrefused | timeout | term()}
) -> ok.
reply_delegator(_ResourceId, ReplyFunAndArgs, Result) ->
    case Result of
        {error, Reason} when
            Reason =:= econnrefused;
            %% this comes directly from `gun'...
            Reason =:= {closed, "The connection was lost."};
            Reason =:= timeout
        ->
            ?tp(
                gcp_pubsub_request_failed,
                #{
                    reason => Reason,
                    query_mode => async,
                    recoverable_error => true,
                    connector => _ResourceId
                }
            ),
            Result1 = {error, {recoverable_error, Reason}},
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result1);
        _ ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => Result,
                    query_mode => async,
                    connector => _ResourceId
                }
            ),
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result)
    end.

-spec do_get_status(resource_id(), timer:time()) -> boolean().
do_get_status(ResourceId, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(ResourceId)],
    DoPerWorker =
        fun(Worker) ->
            case ehttpc:health_check(Worker, Timeout) of
                ok ->
                    true;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "ehttpc_health_check_failed",
                        connector => ResourceId,
                        reason => Reason,
                        worker => Worker
                    }),
                    false
            end
        end,
    try emqx_utils:pmap(DoPerWorker, Workers, Timeout) of
        [_ | _] = Status ->
            lists:all(fun(St) -> St =:= true end, Status);
        [] ->
            false
    catch
        exit:timeout ->
            false
    end.
