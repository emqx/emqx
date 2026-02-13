%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    stop/3,
    query_sync/2,
    query_async/3,
    get_status/1
]).
-export([reply_delegator/3]).

-export([
    get_project_id/1,
    pubsub_get_topic/3
]).

%% Only for tests.
-export([get_transport/1]).

-type service_account_json() :: map().
-type project_id() :: binary().
-type duration() :: non_neg_integer().
-type config() :: #{
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    max_inactive := non_neg_integer(),
    resource_opts := #{atom() => term()},
    authentication := authentication_config(),
    any() => term()
}.
-type authentication_config() :: service_account_json_auth_config().
-type service_account_json_auth_config() :: #{
    type := service_account_json,
    service_account_json := binary()
}.
-opaque state() :: #{
    connect_timeout := duration(),
    auth_config := auth_state(),
    max_retries := non_neg_integer(),
    pool_name := binary(),
    project_id := project_id()
}.
-type auth_state() :: service_account_json_auth_state().
-type service_account_json_auth_state() :: #{
    type := service_account_json,
    jwt_config := emqx_connector_jwt:jwt_config()
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
    authentication_config/0,
    service_account_json/0,
    state/0,
    headers/0,
    body/0,
    status_code/0,
    project_id/0,
    topic/0
]).

-define(DEFAULT_PIPELINE_SIZE, 100).
-define(DEFAULT_MAX_INACTIVE, 10_000).

-define(TOKEN_ROW(RES_ID, TOKEN), {RES_ID, TOKEN}).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

-spec start(resource_id(), config()) -> {ok, state()} | {error, term()}.
start(
    ResourceId,
    #{
        connect_timeout := ConnectTimeout,
        max_retries := MaxRetries
    } = Config
) ->
    case maybe_initialize_auth_resources(ResourceId, Config) of
        {ok, AuthCtx} ->
            #{
                auth_config := AuthConfig,
                project_id := ProjectId
            } = AuthCtx,
            State = #{
                connect_timeout => ConnectTimeout,
                auth_config => AuthConfig,
                max_retries => MaxRetries,
                pool_name => ResourceId,
                project_id => ProjectId
            },
            do_start_pool(ResourceId, State, Config);
        {error, Reason} ->
            {error, Reason}
    end.

do_start_pool(ResourceId, State, Config) ->
    #{
        connect_timeout := ConnectTimeout,
        pool_size := PoolSize,
        transport := Transport,
        host := Host,
        port := Port
    } = Config,
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
        {max_inactive, maps:get(max_inactive, Config, ?DEFAULT_MAX_INACTIVE)},
        {enable_pipelining, maps:get(pipelining, Config, ?DEFAULT_PIPELINE_SIZE)}
    ],
    ?tp(
        gcp_on_start_before_starting_pool,
        #{
            resource_id => ResourceId,
            pool_name => ResourceId,
            pool_opts => PoolOpts
        }
    ),
    ?tp(gcp_starting_ehttpc_pool, #{pool_name => ResourceId}),
    case ehttpc_sup:start_pool(ResourceId, PoolOpts) of
        {ok, _} ->
            ?tp(gcp_ehttpc_pool_started, #{pool_name => ResourceId}),
            {ok, State};
        {error, {already_started, _}} ->
            ?tp(gcp_ehttpc_pool_already_started, #{pool_name => ResourceId}),
            _ = ehttpc_sup:stop_pool(ResourceId),
            ok = emqx_connector_jwt:delete_jwt(?JWT_TABLE, ResourceId),
            case ehttpc_sup:start_pool(ResourceId, PoolOpts) of
                {ok, _} ->
                    {ok, State};
                {error, Reason} ->
                    ?tp(gcp_ehttpc_pool_start_failure, #{
                        pool_name => ResourceId,
                        reason => Reason
                    }),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?tp(gcp_ehttpc_pool_start_failure, #{
                pool_name => ResourceId,
                reason => Reason
            }),
            {error, Reason}
    end.

-spec stop(state()) -> ok | {error, term()}.
stop(Client) ->
    #{pool_name := ResourceId} = Client,
    {Sup, Tab} =
        case Client of
            #{auth_config := #{type := wif, token_table := Tab0, supervisor := Sup0}} ->
                {Sup0, Tab0};
            _ ->
                {undefined, undefined}
        end,
    stop(ResourceId, Sup, Tab).

-spec stop(resource_id(), undefined | supervisor:sup_ref(), undefined | ets:table()) ->
    ok | {error, term()}.
stop(ResourceId, Sup, Tab) ->
    ?tp(gcp_client_stop, #{instance_id => ResourceId, resource_id => ResourceId}),
    ?SLOG(info, #{
        msg => "stopping_gcp_client",
        connector => ResourceId
    }),
    ok = emqx_connector_jwt:delete_jwt(?JWT_TABLE, ResourceId),
    maybe
        true ?= Sup /= undefined,
        true ?= Tab /= undefined,
        ok = stop_worker_and_clear_token(ResourceId, Sup, Tab)
    end,
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
    {ok, map()} | {error, term()}.
query_sync({prepared_request, PreparedRequest = {_Method, _Path, _Body}, ReqOpts}, State) ->
    PoolName = maps:get(pool_name, State),
    ?TRACE(
        "QUERY_SYNC",
        "gcp_client_received",
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
        "gcp_client_received",
        #{requests => PreparedRequest, connector => PoolName, state => State}
    ),
    do_send_requests_async(State, {prepared_request, PreparedRequest, ReqOpts}, ReplyFunAndArgs).

-spec get_status(state()) -> ?status_connected | {?status_disconnected, term()}.
get_status(#{connect_timeout := _, pool_name := _} = State) ->
    case do_get_status(State) of
        ok ->
            ?status_connected;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "gcp_client_get_status_failed",
                state => State,
                reason => Reason
            }),
            {?status_disconnected, Reason}
    end.

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

get_project_id(#{authentication := #{type := service_account_json} = AuthConfig}) ->
    #{service_account_json := ServiceAccountJSON0} = AuthConfig,
    #{<<"project_id">> := ProjectId} = emqx_utils_json:decode(ServiceAccountJSON0),
    ProjectId;
get_project_id(#{authentication := #{type := wif} = AuthConfig}) ->
    #{gcp_project_id := ProjectId} = AuthConfig,
    ProjectId.

-spec pubsub_get_topic(topic(), state(), request_opts()) -> {ok, map()} | {error, term()}.
pubsub_get_topic(Topic, ClientState, ReqOpts) ->
    #{project_id := ProjectId} = ClientState,
    Method = get,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
    Body = <<>>,
    PreparedRequest = {prepared_request, {Method, Path, Body}, ReqOpts},
    ?MODULE:query_sync(PreparedRequest, ClientState).

%%-------------------------------------------------------------------------------------------------
%% Only for tests
%%-------------------------------------------------------------------------------------------------

-spec get_transport(pubsub | bigquery) -> {tls | tcp, string()}.
get_transport(Type) ->
    %% emulating the emulator behavior
    %% https://cloud.google.com/pubsub/docs/emulator
    {Env, RealURL} =
        case Type of
            pubsub ->
                {"PUBSUB_EMULATOR_HOST", "pubsub.googleapis.com:443"};
            bigquery ->
                {"BIGQUERY_EMULATOR_HOST", "bigquery.googleapis.com:443"}
        end,
    case os:getenv(Env) of
        false ->
            {tls, RealURL};
        HostPort0 ->
            %% The emulator is plain HTTP...
            Transport0 = persistent_term:get(
                {emqx_bridge_gcp_pubsub_client, Type, transport}, tcp
            ),
            {Transport0, HostPort0}
    end.

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

maybe_initialize_auth_resources(
    ResourceId, #{authentication := #{type := service_account_json}} = Config
) ->
    {ok, parse_jwt_config(ResourceId, Config)};
maybe_initialize_auth_resources(ResourceId, #{authentication := #{type := wif}} = Config) ->
    #{authentication := #{gcp_project_id := ProjectId} = AuthConfig0} = Config,
    #{
        supervisor := Sup,
        token_table := Tab
    } = Config,
    ChildSpec = prepare_wif_worker_spec(ResourceId, Tab, AuthConfig0),
    case supervisor:start_child(Sup, ChildSpec) of
        {ok, _} ->
            case do_check_token_exists(Tab, ResourceId) of
                ok ->
                    AuthConfig1 = maps:with([type, gcp_project_id], AuthConfig0),
                    AuthConfig = AuthConfig1#{
                        resource_id => ResourceId,
                        token_table => Tab,
                        supervisor => Sup
                    },
                    {ok, #{auth_config => AuthConfig, project_id => ProjectId}};
                {error, Reason} ->
                    _ = supervisor:terminate_child(Sup, ResourceId),
                    _ = supervisor:delete_child(Sup, ResourceId),
                    {error, {failed_to_wait_for_initial_token, Reason}}
            end;
        {error, {already_started, _}} ->
            _ = supervisor:terminate_child(Sup, ResourceId),
            _ = supervisor:delete_child(Sup, ResourceId),
            maybe_initialize_auth_resources(ResourceId, Config);
        {error, Reason} ->
            {error, Reason}
    end.

-spec parse_jwt_config(resource_id(), config()) -> map().
parse_jwt_config(ResourceId, #{
    jwt_opts := #{aud := Aud},
    authentication := #{service_account_json := ServiceAccountJSON0}
}) ->
    ServiceAccountJSON = emqx_utils_json:decode(ServiceAccountJSON0),
    #{
        <<"project_id">> := ProjectId,
        <<"private_key_id">> := KId,
        <<"private_key">> := PrivateKeyPEM,
        <<"client_email">> := ServiceAccountEmail
    } = ServiceAccountJSON,
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
                ?tp(error, gcp_client_startup_error, #{error => empty_key}),
                throw("empty private in service account json");
            {error, Reason} ->
                Error = {invalid_private_key, Reason},
                ?tp(error, gcp_client_startup_error, #{error => Error}),
                throw("invalid private key in service account json");
            Error0 ->
                Error = {invalid_private_key, Error0},
                ?tp(error, gcp_client_startup_error, #{error => Error}),
                throw("invalid private key in service account json")
        catch
            error:function_clause ->
                %% Function clause error inside `jose_jwk', nothing much to do...
                %% Possibly `base64:mime_decode_binary/5' while trying to decode an
                %% invalid private key that would probably not appear under normal
                %% conditions...
                ?tp(error, gcp_client_startup_error, #{error => invalid_private_key}),
                throw("invalid private key in service account json");
            Kind:Reason ->
                Error = {Kind, Reason},
                ?tp(error, gcp_client_startup_error, #{error => Error}),
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
        auth_config => #{type => service_account_json, jwt_config => JWTConfig},
        project_id => ProjectId
    }.

prepare_wif_worker_spec(ResourceId, Tab, AuthConfig) ->
    Opts = prepare_wif_worker_steps(AuthConfig, ResourceId, Tab),
    emqx_bridge_gcp_pubsub_auth_wif_worker:child_spec(ResourceId, Opts).

prepare_wif_worker_steps(#{type := wif} = AuthConfig, ResourceId, Tab) ->
    #{
        initial_token := InitialTokenConfig,
        service_account_email := ServiceAccountEmail,
        gcp_project_number := ProjectNumber,
        gcp_wif_pool_id := WIFPoolId,
        gcp_wif_pool_provider_id := WIFPoolProviderId
    } = AuthConfig,
    Step1Name = initial_token,
    Step1 = prepare_initial_step(Step1Name, InitialTokenConfig),
    Step2Name = gcp_access_token,
    Step2 = #{
        name => Step2Name,
        method => post,
        lifetime => timer:hours(1),
        url => fun(_StepContext) -> <<"https://sts.googleapis.com/v1/token">> end,
        body => fun(#{{step, Step1Name} := #{token := InitialToken}}) ->
            Audience = fmt(
                <<
                    "//iam.googleapis.com/projects/${gcp_project_number}"
                    "/locations/global/workloadIdentityPools/${gcp_wif_pool_id}"
                    "/providers/${gcp_wif_pool_provider_id}"
                >>,
                #{
                    gcp_project_number => ProjectNumber,
                    gcp_wif_pool_id => WIFPoolId,
                    gcp_wif_pool_provider_id => WIFPoolProviderId
                }
            ),
            emqx_utils_json:encode(#{
                <<"grantType">> => <<"urn:ietf:params:oauth:grant-type:token-exchange">>,
                <<"audience">> => Audience,
                <<"scope">> => <<"https://www.googleapis.com/auth/cloud-platform">>,
                <<"requestedTokenType">> => <<"urn:ietf:params:oauth:token-type:access_token">>,
                <<"subjectTokenType">> => <<"urn:ietf:params:oauth:token-type:jwt">>,
                <<"subjectToken">> => emqx_secret:unwrap(InitialToken)
            })
        end,
        headers => fun(_StepContext) ->
            [{<<"Content-Type">>, <<"application/json">>}]
        end,
        extract_result => fun(#{body := RespBody}) ->
            case emqx_utils_json:safe_decode(RespBody) of
                {ok, #{<<"access_token">> := Token}} ->
                    {ok, #{token => Token}};
                Error ->
                    {error, {bad_token_response, Error}}
            end
        end
    },
    Step3 = #{
        name => gcp_impersonate_service_account,
        method => post,
        lifetime => timer:hours(1),
        url => fun(_StepContext) ->
            Name = fmt(
                <<"projects/-/serviceAccounts/${service_account_email}">>,
                #{service_account_email => ServiceAccountEmail}
            ),
            fmt(
                <<"https://iamcredentials.googleapis.com/v1/${name}:generateAccessToken">>,
                #{name => Name}
            )
        end,
        body => fun(_StepContext) ->
            emqx_utils_json:encode(#{
                <<"scope">> => [
                    <<"https://www.googleapis.com/auth/cloud-platform">>,
                    <<"https://www.googleapis.com/auth/userinfo.email">>,
                    <<"https://www.googleapis.com/auth/userinfo.profile">>,
                    <<"https://www.googleapis.com/auth/admin.directory.user">>,
                    <<"https://www.googleapis.com/auth/admin.directory.group">>
                ],
                <<"lifetime">> => <<"3600s">>
            })
        end,
        headers => fun(#{{step, Step2Name} := #{token := WIFToken0}}) ->
            WIFToken = emqx_secret:unwrap(WIFToken0),
            [
                {<<"Content-Type">>, <<"application/json">>},
                {<<"Authorization">>, <<"Bearer ", WIFToken/binary>>}
            ]
        end,
        extract_result => fun(#{body := RespBody}) ->
            case emqx_utils_json:safe_decode(RespBody) of
                {ok, #{<<"accessToken">> := Token}} ->
                    {ok, #{token => Token}};
                Error ->
                    {error, {bad_token_response, Error}}
            end
        end
    },
    Steps = [Step1, Step2, Step3],
    InsertFn = {fun(FinalToken) -> ets:insert(Tab, ?TOKEN_ROW(ResourceId, FinalToken)) end, []},
    #{
        resource_id => ResourceId,
        steps => Steps,
        insert_fn => InsertFn
    }.

prepare_initial_step(Step1Name, #{type := oidc_client_credentials} = InitialTokenConfig) ->
    #{
        endpoint_uri := EndpointURI,
        client_id := ClientId,
        client_secret := ClientSecret,
        scope := Scope
    } = InitialTokenConfig,
    #{
        name => Step1Name,
        method => post,
        lifetime => timer:hours(1),
        url => fun(_StepContext) -> EndpointURI end,
        body => fun(_StepContext) ->
            uri_string:compose_query([
                {<<"grant_type">>, <<"client_credentials">>},
                {<<"client_id">>, ClientId},
                {<<"client_secret">>, emqx_secret:unwrap(ClientSecret)},
                {<<"scope">>, Scope}
            ])
        end,
        headers => fun(_StepContext) ->
            [{<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}]
        end,
        extract_result => fun(#{body := RespBody}) ->
            case emqx_utils_json:safe_decode(RespBody) of
                {ok, #{<<"access_token">> := Token}} ->
                    {ok, #{token => Token}};
                Error ->
                    {error, {bad_token_response, Error}}
            end
        end
    }.

stop_worker_and_clear_token(ResourceId, Sup, Tab) ->
    _ = ets:delete(Tab, ResourceId),
    _ = supervisor:terminate_child(Sup, ResourceId),
    _ = supervisor:delete_child(Sup, ResourceId),
    ok.

fmt(FmtStr, Context) ->
    Template = emqx_template:parse(FmtStr),
    iolist_to_binary(emqx_template:render_strict(Template, Context)).

-spec get_authorization_header(auth_state()) -> [{binary(), binary()}].
get_authorization_header(#{type := service_account_json, jwt_config := JWTConfig}) ->
    JWT = emqx_connector_jwt:ensure_jwt(JWTConfig),
    [{<<"Authorization">>, <<"Bearer ", JWT/binary>>}];
get_authorization_header(#{type := wif, resource_id := ResourceId, token_table := Tab}) ->
    [?TOKEN_ROW(_, JWT)] = ets:lookup(Tab, ResourceId),
    [{<<"Authorization">>, <<"Bearer ", JWT/binary>>}].

-spec do_send_requests_sync(
    state(),
    {prepared_request, prepared_request(), request_opts()}
) ->
    {ok, map()} | {error, term()}.
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
    #{auth_config := AuthConfig} = State,
    Headers = get_authorization_header(AuthConfig),
    case {Method, Body} of
        {get, <<>>} -> {Path, Headers};
        {delete, <<>>} -> {Path, Headers};
        _ -> {Path, Headers, Body}
    end.

-spec handle_response(term(), resource_id(), sync | async) -> {ok, map()} | {error, term()}.
handle_response(Result, ResourceId, QueryMode) ->
    case Result of
        {error, {shutdown, Reason}} ->
            {error, Reason};
        {error, Reason} when
            Reason =:= econnrefused;
            %% this comes directly from `gun'...
            Reason =:= {closed, "The connection was lost."};
            Reason =:= closed;
            %% The normal reason happens when the HTTP connection times out before
            %% the request has been fully processed
            Reason =:= normal;
            Reason =:= timeout
        ->
            ?tp(
                warning,
                gcp_client_request_failed,
                #{
                    reason => Reason,
                    recoverable_error => true,
                    connector => ResourceId
                }
            ),
            {error, {recoverable_error, Reason}};
        {error, Reason} ->
            {error, Reason};
        {ok, StatusCode, RespHeaders} ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => Result,
                    query_mode => QueryMode,
                    connector => ResourceId
                }
            ),
            Status = response_status(StatusCode),
            {Status, #{status_code => StatusCode, headers => RespHeaders}};
        {ok, StatusCode, RespHeaders, RespBody} ->
            ?tp(
                gcp_pubsub_response,
                #{
                    response => Result,
                    query_mode => QueryMode,
                    connector => ResourceId
                }
            ),
            Status = response_status(StatusCode),
            {Status, #{status_code => StatusCode, headers => RespHeaders, body => RespBody}}
    end.

response_status(StatusCode) when StatusCode >= 200 andalso StatusCode < 300 ->
    ok;
response_status(_StatusCode) ->
    error.

-spec reply_delegator(
    resource_id(),
    {ReplyFun :: function(), Args :: list()},
    term() | {error, econnrefused | timeout | term()}
) -> ok.
reply_delegator(ResourceId, ReplyFunAndArgs, Response) ->
    Result = handle_response(Response, ResourceId, _QueryMode = async),
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

-spec do_get_status(state()) -> ok | {error, term()}.
do_get_status(State) ->
    case check_token_exists(State) of
        ok ->
            #{connect_timeout := Timeout, pool_name := PoolName} = State,
            do_get_status_pool(PoolName, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

do_get_status_pool(ResourceId, Timeout) ->
    case ehttpc:check_pool_integrity(ResourceId) of
        ok ->
            do_get_status_pool1(ResourceId, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

do_get_status_pool1(ResourceId, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(ResourceId)],
    DoPerWorker =
        fun(Worker) ->
            case ehttpc:health_check(Worker, Timeout) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "gcp_client_ehttpc_health_check_failed",
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

check_token_exists(#{auth_config := #{type := wif} = AuthConfig} = _State) ->
    #{token_table := Tab, resource_id := ResourceId} = AuthConfig,
    do_check_token_exists(Tab, ResourceId);
check_token_exists(_State) ->
    ok.

do_check_token_exists(Tab, ResourceId) ->
    case ets:lookup(Tab, ResourceId) of
        [?TOKEN_ROW(ResourceId, _)] ->
            ok;
        [] ->
            Timeout = 10_000,
            emqx_bridge_gcp_pubsub_auth_wif_worker:ensure_token(ResourceId, Timeout)
    end.
