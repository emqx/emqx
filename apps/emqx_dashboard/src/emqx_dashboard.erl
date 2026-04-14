%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard).

-moduledoc "Dashboard listener management, authorization, and Minirest wiring.".

-export([
    start_listeners/0,
    start_listeners/1,
    stop_listeners/1,
    stop_listeners/0,
    list_listeners/0,
    listeners_status/0,
    regenerate_dispatch_after_config_update/0
]).

%% Authorization
-export([authorize/2]).
-export([get_namespace/1]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx/include/emqx_release.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(EMQX_MIDDLE, emqx_dashboard_middleware).

-type listener_name() :: atom().
-type listener_configs() :: #{listener_name() => emqx_config:config()}.

%% See `minirest_handler:do_authorize`.
-type handler_info() :: #{
    method := atom(),
    module := module(),
    function := atom()
}.
%% Todo: refine keys/values.
-type request() :: map().

-type auth_meta() :: #{
    auth_type := jwt_token | api_key,
    source := binary(),
    namespace := ?global_ns | binary(),
    actor := emqx_dashboard_rbac:actor_context()
}.

-export_type([listener_name/0, listener_configs/0, handler_info/0, request/0]).

%%--------------------------------------------------------------------
%% Start/Stop Listeners
%%--------------------------------------------------------------------

-spec start_listeners() -> ok | {error, [listener_name()]}.
start_listeners() ->
    start_listeners(listeners()).

-spec stop_listeners() -> ok.
stop_listeners() ->
    stop_listeners(listeners()).

-spec start_listeners(listener_configs()) ->
    ok | {error, [listener_name()]}.
start_listeners(Listeners) ->
    %% NOTE
    %% Before starting the listeners, we do not generate the full dispatch upfront,
    %% because the listeners may fail to start and the generation may appear useless.
    InitDispatch = init_dispatch(),
    {OkListeners, ErrListeners} =
        lists:foldl(
            fun({Name, Protocol, Bind, RanchOptions, ProtoOpts}, {OkAcc, ErrAcc}) ->
                Options = #{
                    dispatch => InitDispatch,
                    swagger_support => emqx:get_config([dashboard, swagger_support], true),
                    protocol => Protocol,
                    protocol_options => ProtoOpts
                },
                Minirest = minirest_option(Options),
                case minirest:start(Name, RanchOptions, Minirest) of
                    {ok, _} ->
                        ?ULOG("Listener ~ts on ~ts started.~n", [
                            Name, emqx_listeners:format_bind(Bind)
                        ]),
                        {[Name | OkAcc], ErrAcc};
                    {error, _Reason} ->
                        %% NOTE
                        %% Don't record the reason because minirest already does(too much logs noise).
                        {OkAcc, [Name | ErrAcc]}
                end
            end,
            {[], []},
            listeners(ensure_ssl_cert(Listeners))
        ),
    ok = emqx_dashboard_dispatch:regenerate_dispatch(OkListeners),
    case ErrListeners of
        [] ->
            ok;
        _ ->
            {error, ErrListeners}
    end.

-spec stop_listeners(listener_configs()) -> ok.
stop_listeners(Listeners) ->
    lists:foreach(
        fun({Name, _, Bind, _, _}) ->
            case minirest:stop(Name) of
                ok ->
                    ?ULOG("Stop listener ~ts on ~ts successfully.~n", [
                        Name, emqx_listeners:format_bind(Bind)
                    ]);
                {error, not_found} ->
                    ?SLOG(warning, #{msg => "stop_listener_failed", name => Name, bind => Bind})
            end
        end,
        listeners(Listeners)
    ).

-spec listeners_status() -> #{started := [listener_name()], stopped := [listener_name()]}.
listeners_status() ->
    ListenerNames = [
        Name
     || {Name, _Protocol, _Bind, _RanchOptions, _ProtoOpts} <- list_listeners()
    ],
    {Started, Stopped} = lists:partition(fun is_listener_started/1, ListenerNames),
    #{started => Started, stopped => Stopped}.

-spec regenerate_dispatch_after_config_update() -> ok.
regenerate_dispatch_after_config_update() ->
    #{started := Listeners} = listeners_status(),
    ok = emqx_dashboard_dispatch:regenerate_dispatch_after_config_update(Listeners).

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------

is_listener_started(Name) ->
    try ranch_server:get_listener_sup(Name) of
        _ -> true
    catch
        error:badarg -> false
    end.

init_dispatch() ->
    static_dispatch(emqx:get_config([dashboard, swagger_support], true)) ++ dynamic_dispatch().

minirest_option(Options) ->
    Authorization = {?MODULE, authorize},
    GlobalSpec = #{
        openapi => "3.0.0",
        info => #{title => emqx_api_name(), version => emqx_release_version()},
        servers => [#{url => emqx_dashboard_swagger:base_path()}],
        components => #{
            schemas => #{},
            'securitySchemes' => #{
                'basicAuth' => #{
                    type => http,
                    scheme => basic,
                    description =>
                        <<"Authorize with [API Keys](https://www.emqx.io/docs/en/v5.0/admin/api.html#api-keys)">>
                },
                'bearerAuth' => #{
                    type => http,
                    scheme => bearer,
                    description => <<"Authorize with Bearer Token">>
                }
            }
        }
    },
    Base =
        #{
            base_path => emqx_dashboard_swagger:base_path(),
            modules => minirest_api:find_api_modules(apps()),
            authorization => Authorization,
            log => audit_log_fun(),
            security => [#{'basicAuth' => []}, #{'bearerAuth' => []}],
            swagger_global_spec => GlobalSpec,
            dispatch => static_dispatch(maps:get(swagger_support, Options, true)),
            middlewares => [?EMQX_MIDDLE, cowboy_router, cowboy_handler],
            swagger_support => true
        },
    maps:merge(Base, Options).

apps() ->
    [
        App
     || {App, _, _} <- application:loaded_applications(),
        case re:run(atom_to_list(App), "^emqx") of
            {match, [{0, 4}]} -> true;
            _ -> false
        end
    ].

listeners(Listeners) ->
    lists:filtermap(
        fun
            ({_Protocol, #{bind := 0}}) ->
                false;
            ({Protocol, Conf = #{}}) ->
                {Conf1, Bind} = ip_port(Conf),
                {true, {
                    listener_name(Protocol),
                    Protocol,
                    Bind,
                    ranch_opts(Conf1),
                    proto_opts(Conf1)
                }}
        end,
        maps:to_list(Listeners)
    ).

list_listeners() ->
    listeners(listeners()).

ip_port(Opts) -> ip_port(maps:take(bind, Opts), Opts).

ip_port(error, Opts) -> {Opts#{port => 18083}, 18083};
ip_port({Port, Opts}, _) when is_integer(Port) -> {Opts#{port => Port}, Port};
ip_port({{IP, Port}, Opts}, _) -> {Opts#{port => Port, ip => IP}, {IP, Port}}.

ranch_opts(Options) ->
    Keys = [
        handshake_timeout,
        connection_type,
        max_connections,
        num_acceptors,
        shutdown,
        socket
    ],
    RanchOpts = maps:with(Keys, Options),
    SocketOpts = maps:fold(
        fun filter_false/3,
        [],
        maps:without([inet6, ipv6_v6only, proxy_header, user_lookup_fun | Keys], Options)
    ),
    InetOpts =
        case Options of
            #{inet6 := true, ipv6_v6only := true} ->
                [inet6, {ipv6_v6only, true}];
            #{inet6 := true, ipv6_v6only := false} ->
                [inet6];
            #{ip := {_, _, _, _, _, _, _, _}} ->
                %% IPv6 bind address implies inet6 even if not explicitly set
                [inet6];
            _ ->
                [inet]
        end,
    RanchOpts#{socket_opts => InetOpts ++ SocketOpts}.

init_proto_opts() ->
    %% cowboy_stream_h is required by default
    %% will integrate cowboy_telemetry_h when OTEL trace is ready
    #{stream_handlers => [cowboy_stream_h]}.

proto_opts(Opts) ->
    Init = init_proto_opts(),
    proxy_header_opt(Init, Opts).

proxy_header_opt(Init, #{proxy_header := ProxyHeader}) ->
    Init#{proxy_header => ProxyHeader};
proxy_header_opt(Init, _Opts) ->
    Init.

filter_false(_K, false, S) -> S;
filter_false(K, V, S) -> [{K, V} | S].

listener_name(Protocol) ->
    list_to_atom(atom_to_list(Protocol) ++ ":dashboard").

audit_log_fun() ->
    emqx_dashboard_audit:log_fun().

-spec authorize(request(), handler_info()) -> {ok, auth_meta()} | {integer(), term(), term()}.
authorize(Req, HandlerInfo) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Username, Password} ->
            api_key_authorize(Req, HandlerInfo, Username, Password);
        {bearer, Token} ->
            jwt_token_bearer_authorize(Req, HandlerInfo, Token);
        _ ->
            %% Fallback to emqx_auth cookie (set by dashboard for plugin UI iframes).
            case is_plugin_api(Req) of
                true ->
                    cookie_authorize(Req, HandlerInfo);
                false ->
                    return_unauthorized(
                        <<"AUTHORIZATION_HEADER_ERROR">>,
                        <<
                            "Missing authorization header. "
                            "Use Basic auth with API key/secret, "
                            "or Bearer token from POST /api/v5/login. "
                            "API keys can be bootstrapped from config "
                            "(api_key.bootstrap_file) or created via POST /api/v5/api_key"
                        >>
                    )
            end
    end.

is_plugin_api(Req) ->
    case cowboy_req:path(Req) of
        <<"/api/v5/plugin_api/", _/binary>> -> true;
        _ -> false
    end.

cookie_authorize(Req, HandlerInfo) ->
    case cowboy_req:match_cookies([{emqx_auth, [], undefined}], Req) of
        #{emqx_auth := Token} when is_binary(Token), Token =/= <<>> ->
            jwt_token_bearer_authorize(Req, HandlerInfo, Token);
        _ ->
            return_unauthorized(
                <<"AUTHORIZATION_HEADER_ERROR">>,
                <<
                    "Missing authorization header. "
                    "Use Basic auth with API key/secret, "
                    "or Bearer token from POST /api/v5/login. "
                    "API keys can be bootstrapped from config "
                    "(api_key.bootstrap_file) or created via POST /api/v5/api_key"
                >>
            )
    end.

return_unauthorized(Code, Message) ->
    {401,
        #{
            <<"WWW-Authenticate">> =>
                <<"Basic Realm=\"emqx-dashboard\"">>
        },
        #{code => Code, message => Message}}.

get_namespace(#{auth_meta := #{?namespace := Namespace}} = _Request) when is_binary(Namespace) ->
    Namespace;
get_namespace(#{} = _Request) ->
    ?global_ns.

listeners() ->
    emqx_conf:get([dashboard, listeners], #{}).

api_key_authorize(Req, HandlerInfo, Key, Secret) ->
    case emqx_mgmt_auth:authorize(HandlerInfo, Req, Key, Secret) of
        {ok, ActorContext} ->
            AuthnMeta = #{
                auth_type => api_key,
                source => Key,
                namespace => maps:get(?namespace, ActorContext, ?global_ns),
                actor => ActorContext
            },
            {ok, AuthnMeta};
        {error, <<"not_allowed">>, Resource} ->
            return_unauthorized(
                ?API_KEY_NOT_ALLOW,
                <<"Please use bearer Token instead, using API key/secret in ", Resource/binary,
                    " path is not permitted">>
            );
        {error, unauthorized_role} ->
            {403, 'UNAUTHORIZED_ROLE', ?API_KEY_NOT_ALLOW_MSG};
        {error, _} ->
            return_unauthorized(
                ?BAD_API_KEY_OR_SECRET,
                <<"Check api_key/api_secret">>
            )
    end.

jwt_token_bearer_authorize(Req, HandlerInfo, Token) ->
    case emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token) of
        {ok, #{actor := Username} = ActorContext} ->
            AuthnMeta = #{
                auth_type => jwt_token,
                source => Username,
                namespace => maps:get(?namespace, ActorContext, ?global_ns),
                actor => ActorContext
            },
            {ok, AuthnMeta};
        {error, token_timeout} ->
            {401, 'TOKEN_TIME_OUT', <<
                "Token expired. "
                "Consider using API key (Basic auth) instead of bearer tokens "
                "to avoid expiration. "
                "Otherwise get a new token by POST /api/v5/login"
            >>};
        {error, not_found} ->
            {401, 'BAD_TOKEN', <<
                "Invalid bearer token. "
                "Use API key (Basic auth) for persistent access, "
                "or get a new bearer token by POST /api/v5/login. "
                "API keys can be bootstrapped from config "
                "(api_key.bootstrap_file) or created via POST /api/v5/api_key"
            >>};
        {error, unauthorized_role} ->
            {403, 'UNAUTHORIZED_ROLE', <<"You don't have permission to access this resource">>}
    end.

ensure_ssl_cert(Listeners = #{https := Https0 = #{ssl_options := SslOpts}}) ->
    SslOpt1 = maps:from_list(emqx_tls_lib:to_server_opts(tls, SslOpts)),
    Https1 = maps:remove(ssl_options, Https0),
    Listeners#{https => maps:merge(Https1, SslOpt1)};
ensure_ssl_cert(Listeners) ->
    Listeners.

static_dispatch(SwaggerSupport) ->
    StaticFiles = ["/editor.worker.js", "/json.worker.js", "/version"],
    [
        {"/", cowboy_static, {priv_file, emqx_dashboard, "www/index.html"}},
        {"/static/[...]", cowboy_static, {priv_dir, emqx_dashboard, "www/static"}}
    ] ++
        [{Path, cowboy_static, {priv_file, emqx_dashboard, "www" ++ Path}} || Path <- StaticFiles] ++
        maybe_swagger_dispatch(SwaggerSupport).

maybe_swagger_dispatch(true) ->
    [
        {"/api-spec.html", cowboy_static, {priv_file, emqx_dashboard, "api-spec.html"}}
        | api_spec_dispatch()
    ];
maybe_swagger_dispatch(false) ->
    [].

%% Routes for the focused API spec endpoints (no auth required).
%% These are added as raw cowboy routes (not minirest trails) so they do not
%% appear in the swagger spec they serve and bypass minirest auth middleware.
api_spec_dispatch() ->
    [
        {"/api-spec.md", emqx_dashboard_api_spec_handler, #{}},
        {"/api-spec.json", emqx_dashboard_api_spec_handler, #{}},
        {"/api-spec/:tag", emqx_dashboard_api_spec_handler, #{}},
        {"/api-spec/:tag/:name", emqx_dashboard_api_spec_handler, #{}}
    ].

dynamic_dispatch() ->
    [
        {emqx_mgmt_api_status:path(), emqx_mgmt_api_status, []},
        {'_', emqx_dashboard_not_found, []}
    ].

emqx_api_name() ->
    emqx_release:description() ++ " API".

emqx_release_version() ->
    emqx_release:version().
