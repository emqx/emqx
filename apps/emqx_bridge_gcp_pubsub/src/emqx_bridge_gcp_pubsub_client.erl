%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_client).

-include_lib("jose/include/jose_jwk.hrl").
-include_lib("emqx_connector_jwt/include/emqx_connector_jwt_tables.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start/2,
    stop/1,
    query_sync/2,
    query_async/3,
    get_status/1
]).
-export([reply_delegator/3]).

-export([get_topic/3]).

-export([get_jwt_authorization_header/1]).

-type service_account_json() :: map().
-type project_id() :: binary().
-type duration() :: non_neg_integer().
-type config() :: #{
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    resource_opts := #{atom() => term()},
    service_account_json := service_account_json(),
    any() => term()
}.
-opaque state() :: #{
    connect_timeout := duration(),
    jwt_config := emqx_connector_jwt:jwt_config(),
    max_retries := non_neg_integer(),
    pool_name := binary(),
    project_id := project_id()
}.
-type headers() :: [{binary(), iodata()}].
-type body() :: iodata().
-type status_code() :: 100..599.
-type method() :: get | post | put | patch.
-type path() :: binary().
-type prepared_request() :: {method(), path(), body()}.
-type request_opts() :: #{request_ttl := emqx_schema:duration_ms() | infinity}.
-type topic() :: binary().

-export_type([
    service_account_json/0,
    state/0,
    headers/0,
    body/0,
    status_code/0,
    project_id/0,
    topic/0
]).

-define(DEFAULT_PIPELINE_SIZE, 100).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

-spec start(resource_id(), config()) -> {ok, state()} | {error, term()}.
start(
    ResourceId,
    #{
        connect_timeout := ConnectTimeout,
        max_retries := MaxRetries,
        pool_size := PoolSize
    } = Config
) ->
    {Transport, HostPort} = get_transport(),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(HostPort, #{default_port => 443}),
    PoolType = random,
    TransportOpts =
        case Transport of
            tls -> emqx_tls_lib:to_client_opts(#{enable => true, verify => verify_none});
            tcp -> []
        end,
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
        {enable_pipelining, maps:get(pipelining, Config, ?DEFAULT_PIPELINE_SIZE)}
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
        project_id => ProjectId
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
            ?tp(gcp_pubsub_ehttpc_pool_started, #{pool_name => ResourceId}),
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

-spec stop(resource_id()) -> ok | {error, term()}.
stop(ResourceId) ->
    ?tp(gcp_pubsub_stop, #{instance_id => ResourceId, resource_id => ResourceId}),
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

-spec query_sync(
    {prepared_request, prepared_request(), request_opts()},
    state()
) ->
    {ok, map()} | {error, {recoverable_error, term()} | term()}.
query_sync({prepared_request, PreparedRequest = {_Method, _Path, _Body}, ReqOpts}, State) ->
    PoolName = maps:get(pool_name, State),
    ?TRACE(
        "QUERY_SYNC",
        "gcp_pubsub_received",
        #{requests => PreparedRequest, connector => PoolName, state => State}
    ),
    do_send_requests_sync(State, {prepared_request, PreparedRequest, ReqOpts}).

-spec query_async(
    {prepared_request, prepared_request(), request_opts()},
    {ReplyFun :: function(), Args :: list()},
    state()
) -> {ok, pid()} | {error, no_pool_worker_available}.
query_async(
    {prepared_request, PreparedRequest = {_Method, _Path, _Body}, ReqOpts},
    ReplyFunAndArgs,
    State
) ->
    PoolName = maps:get(pool_name, State),
    ?TRACE(
        "QUERY_ASYNC",
        "gcp_pubsub_received",
        #{requests => PreparedRequest, connector => PoolName, state => State}
    ),
    do_send_requests_async(State, {prepared_request, PreparedRequest, ReqOpts}, ReplyFunAndArgs).

-spec get_status(state()) -> ?status_connected | {?status_disconnected, term()}.
get_status(#{connect_timeout := Timeout, pool_name := PoolName} = State) ->
    case do_get_status(PoolName, Timeout) of
        ok ->
            ?status_connected;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "gcp_pubsub_bridge_get_status_failed",
                state => State,
                reason => Reason
            }),
            {?status_disconnected, Reason}
    end.

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

-spec get_topic(topic(), state(), request_opts()) -> {ok, map()} | {error, term()}.
get_topic(Topic, ClientState, ReqOpts) ->
    #{project_id := ProjectId} = ClientState,
    Method = get,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
    Body = <<>>,
    PreparedRequest = {prepared_request, {Method, Path, Body}, ReqOpts},
    ?MODULE:query_sync(PreparedRequest, ClientState).

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
        <<"project_id">> := ProjectId,
        <<"private_key_id">> := KId,
        <<"private_key">> := PrivateKeyPEM,
        <<"client_email">> := ServiceAccountEmail
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
            error:function_clause ->
                %% Function clause error inside `jose_jwk', nothing much to do...
                %% Possibly `base64:mime_decode_binary/5' while trying to decode an
                %% invalid private key that would probably not appear under normal
                %% conditions...
                ?tp(error, gcp_pubsub_connector_startup_error, #{error => invalid_private_key}),
                throw("invalid private key in service account json");
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
    {prepared_request, prepared_request(), request_opts()}
) ->
    {ok, map()} | {error, {recoverable_error, term()} | term()}.
do_send_requests_sync(State, {prepared_request, {Method, Path, Body}, ReqOpts}) ->
    #{
        pool_name := PoolName,
        max_retries := MaxRetries
    } = State,
    #{request_ttl := RequestTTL} = ReqOpts,
    ?tp(
        gcp_pubsub_bridge_do_send_requests,
        #{
            request => {prepared_request, {Method, Path, Body}, ReqOpts},
            query_mode => sync,
            resource_id => PoolName
        }
    ),
    Request = to_ehttpc_request(State, Method, Path, Body),
    Response = ehttpc:request(
        PoolName,
        Method,
        Request,
        RequestTTL,
        MaxRetries
    ),
    handle_response(Response, PoolName, _QueryMode = sync).

-spec do_send_requests_async(
    state(),
    {prepared_request, prepared_request(), request_opts()},
    {ReplyFun :: function(), Args :: list()}
) -> {ok, pid()} | {error, no_pool_worker_available}.
do_send_requests_async(
    State, {prepared_request, {Method, Path, Body}, ReqOpts}, ReplyFunAndArgs
) ->
    #{pool_name := PoolName} = State,
    #{request_ttl := RequestTTL} = ReqOpts,
    ?tp(
        gcp_pubsub_bridge_do_send_requests,
        #{
            request => {prepared_request, {Method, Path, Body}, ReqOpts},
            query_mode => async,
            resource_id => PoolName
        }
    ),
    Request = to_ehttpc_request(State, Method, Path, Body),
    %% `ehttpc_pool'/`gproc_pool' might return `false' if there are no workers...
    case ehttpc_pool:pick_worker(PoolName) of
        false ->
            {error, no_pool_worker_available};
        Worker ->
            ok = ehttpc:request_async(
                Worker,
                Method,
                Request,
                RequestTTL,
                {fun ?MODULE:reply_delegator/3, [PoolName, ReplyFunAndArgs]}
            ),
            {ok, Worker}
    end.

to_ehttpc_request(State, Method, Path, Body) ->
    #{jwt_config := JWTConfig} = State,
    Headers = get_jwt_authorization_header(JWTConfig),
    case {Method, Body} of
        {get, <<>>} -> {Path, Headers};
        {delete, <<>>} -> {Path, Headers};
        _ -> {Path, Headers, Body}
    end.

-spec handle_response(term(), resource_id(), sync | async) -> {ok, map()} | {error, term()}.
handle_response(Result, ResourceId, QueryMode) ->
    case Result of
        {error, Reason} ->
            ?tp(
                gcp_pubsub_request_failed,
                #{
                    reason => Reason,
                    query_mode => QueryMode,
                    connector => ResourceId
                }
            ),
            {error, Reason};
        {ok, StatusCode, RespHeaders} when StatusCode >= 200 andalso StatusCode < 300 ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => Result,
                    query_mode => QueryMode,
                    connector => ResourceId
                }
            ),
            {ok, #{status_code => StatusCode, headers => RespHeaders}};
        {ok, StatusCode, RespHeaders, RespBody} when
            StatusCode >= 200 andalso StatusCode < 300
        ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => Result,
                    query_mode => QueryMode,
                    connector => ResourceId
                }
            ),
            {ok, #{status_code => StatusCode, headers => RespHeaders, body => RespBody}};
        {ok, StatusCode, RespHeaders} = _Result ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => _Result,
                    query_mode => QueryMode,
                    connector => ResourceId
                }
            ),
            {error, #{status_code => StatusCode, headers => RespHeaders}};
        {ok, StatusCode, RespHeaders, RespBody} = _Result ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => _Result,
                    query_mode => QueryMode,
                    connector => ResourceId
                }
            ),
            {error, #{status_code => StatusCode, headers => RespHeaders, body => RespBody}}
    end.

-spec reply_delegator(
    resource_id(),
    {ReplyFun :: function(), Args :: list()},
    term() | {error, econnrefused | timeout | term()}
) -> ok.
reply_delegator(ResourceId, ReplyFunAndArgs, Response) ->
    Result = handle_response(Response, ResourceId, _QueryMode = async),
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

-spec do_get_status(resource_id(), duration()) -> ok | {error, term()}.
do_get_status(ResourceId, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(ResourceId)],
    DoPerWorker =
        fun(Worker) ->
            case ehttpc:health_check(Worker, Timeout) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "gcp_pubsub_ehttpc_health_check_failed",
                        connector => ResourceId,
                        reason => Reason,
                        worker => Worker,
                        wait_time => Timeout
                    }),
                    {error, Reason}
            end
        end,
    try emqx_utils:pmap(DoPerWorker, Workers, Timeout) of
        [_ | _] = Status ->
            Errors = lists:filter(
                fun
                    (ok) -> false;
                    (_) -> true
                end,
                Status
            ),
            case Errors of
                [{error, FirstReason} | _] ->
                    {error, FirstReason};
                [] ->
                    ok
            end;
        [] ->
            {error, no_workers_alive}
    catch
        exit:timeout ->
            {error, timeout}
    end.

-spec get_transport() -> {tls | tcp, string()}.
get_transport() ->
    %% emulating the emulator behavior
    %% https://cloud.google.com/pubsub/docs/emulator
    case os:getenv("PUBSUB_EMULATOR_HOST") of
        false ->
            {tls, "pubsub.googleapis.com:443"};
        HostPort0 ->
            %% The emulator is plain HTTP...
            Transport0 = persistent_term:get({?MODULE, transport}, tcp),
            {Transport0, HostPort0}
    end.
