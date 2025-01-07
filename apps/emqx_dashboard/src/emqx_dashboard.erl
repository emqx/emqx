%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard).

-export([
    start_listeners/0,
    start_listeners/1,
    stop_listeners/1,
    stop_listeners/0,
    list_listeners/0,
    wait_for_listeners/0
]).

%% Authorization
-export([authorize/1]).

-export([save_dispatch_eterm/1]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/http_api.hrl").
-include_lib("emqx/include/emqx_release.hrl").
-dialyzer({[no_opaque, no_match, no_return], [init_cache_dispatch/2, start_listeners/1]}).

-define(EMQX_MIDDLE, emqx_dashboard_middleware).
-define(DISPATCH_FILE, "dispatch.eterm").

%%--------------------------------------------------------------------
%% Start/Stop Listeners
%%--------------------------------------------------------------------

start_listeners() ->
    start_listeners(listeners()).

stop_listeners() ->
    stop_listeners(listeners()).

start_listeners(Listeners) ->
    {ok, _} = application:ensure_all_started(minirest),
    SwaggerSupport = emqx:get_config([dashboard, swagger_support], true),
    InitDispatch = dispatch(),
    {OkListeners, ErrListeners} =
        lists:foldl(
            fun({Name, Protocol, Bind, RanchOptions, ProtoOpts}, {OkAcc, ErrAcc}) ->
                ok = init_cache_dispatch(Name, InitDispatch),
                Options = #{
                    dispatch => InitDispatch,
                    swagger_support => SwaggerSupport,
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
                        %% Don't record the reason because minirest already does(too much logs noise).
                        {OkAcc, [Name | ErrAcc]}
                end
            end,
            {[], []},
            listeners(ensure_ssl_cert(Listeners))
        ),
    case ErrListeners of
        [] ->
            optvar:set(emqx_dashboard_listeners_ready, OkListeners),
            ok;
        _ ->
            {error, ErrListeners}
    end.

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
            dispatch => static_dispatch(),
            middlewares => [?EMQX_MIDDLE, cowboy_router, cowboy_handler],
            swagger_support => true
        },
    maps:merge(Base, Options).

%% save dispatch to priv dir.
save_dispatch_eterm(SchemaMod) ->
    Dir = code:priv_dir(emqx_dashboard),
    emqx_config:put([dashboard], #{i18n_lang => en, swagger_support => false}),
    os:putenv("SCHEMA_MOD", atom_to_list(SchemaMod)),
    DispatchFile = filename:join([Dir, ?DISPATCH_FILE]),
    io:format(user, "===< Generating: ~s~n", [DispatchFile]),
    #{dispatch := Dispatch} = generate_dispatch(),
    IoData = io_lib:format("~p.~n", [Dispatch]),
    ok = file:write_file(DispatchFile, IoData),
    {ok, [SaveDispatch]} = file:consult(DispatchFile),
    SaveDispatch =/= Dispatch andalso erlang:error("bad dashboard dispatch.eterm file generated"),
    ok.

stop_listeners(Listeners) ->
    optvar:unset(emqx_dashboard_listeners_ready),
    [
        begin
            case minirest:stop(Name) of
                ok ->
                    ?ULOG("Stop listener ~ts on ~ts successfully.~n", [
                        Name, emqx_listeners:format_bind(Bind)
                    ]);
                {error, not_found} ->
                    ?SLOG(warning, #{msg => "stop_listener_failed", name => Name, bind => Bind})
            end
        end
     || {Name, _, Bind, _, _} <- listeners(Listeners)
    ],
    ok.

wait_for_listeners() ->
    optvar:read(emqx_dashboard_listeners_ready).

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------

init_cache_dispatch(Name, Dispatch0) ->
    Dispatch1 = [{_, _, Rules}] = trails:single_host_compile(Dispatch0),
    FileName = filename:join(code:priv_dir(emqx_dashboard), ?DISPATCH_FILE),
    Dispatch2 =
        case file:consult(FileName) of
            {ok, [[{Host, Path, CacheRules}]]} ->
                Trails = trails:trails([{cowboy_swagger_handler, #{server => 'http:dashboard'}}]),
                [{_, _, SwaggerRules}] = trails:single_host_compile(Trails),
                [{Host, Path, CacheRules ++ SwaggerRules ++ Rules}];
            {error, _} ->
                Dispatch1
        end,
    persistent_term:put(Name, Dispatch2).

generate_dispatch() ->
    Options = #{
        dispatch => [],
        swagger_support => false,
        protocol => http,
        protocol_options => proto_opts(#{})
    },
    Minirest = minirest_option(Options),
    minirest:generate_dispatch(Minirest).

dispatch() ->
    static_dispatch() ++ dynamic_dispatch().

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
        maps:without([inet6, ipv6_v6only, proxy_header | Keys], Options)
    ),
    InetOpts =
        case Options of
            #{inet6 := true, ipv6_v6only := true} ->
                [inet6, {ipv6_v6only, true}];
            #{inet6 := true, ipv6_v6only := false} ->
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

-dialyzer({no_match, [audit_log_fun/0]}).

audit_log_fun() ->
    case emqx_release:edition() of
        ee -> emqx_dashboard_audit:log_fun();
        ce -> undefined
    end.

-if(?EMQX_RELEASE_EDITION =/= ee).

%% dialyzer complains about the `unauthorized_role' clause...
-dialyzer({no_match, [authorize/1, api_key_authorize/3]}).

-endif.

authorize(Req) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Username, Password} ->
            api_key_authorize(Req, Username, Password);
        {bearer, Token} ->
            case emqx_dashboard_admin:verify_token(Req, Token) of
                {ok, Username} ->
                    {ok, #{auth_type => jwt_token, source => Username}};
                {error, token_timeout} ->
                    {401, 'TOKEN_TIME_OUT', <<"Token expired, get new token by POST /login">>};
                {error, not_found} ->
                    {401, 'BAD_TOKEN', <<"Get a token by POST /login">>};
                {error, unauthorized_role} ->
                    {403, 'UNAUTHORIZED_ROLE',
                        <<"You don't have permission to access this resource">>}
            end;
        _ ->
            return_unauthorized(
                <<"AUTHORIZATION_HEADER_ERROR">>,
                <<"Support authorization: basic/bearer ">>
            )
    end.

return_unauthorized(Code, Message) ->
    {401,
        #{
            <<"WWW-Authenticate">> =>
                <<"Basic Realm=\"emqx-dashboard\"">>
        },
        #{code => Code, message => Message}}.

listeners() ->
    emqx_conf:get([dashboard, listeners], #{}).

api_key_authorize(Req, Key, Secret) ->
    Path = cowboy_req:path(Req),
    case emqx_mgmt_auth:authorize(Path, Req, Key, Secret) of
        ok ->
            {ok, #{auth_type => api_key, source => Key}};
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

ensure_ssl_cert(Listeners = #{https := Https0 = #{ssl_options := SslOpts}}) ->
    SslOpt1 = maps:from_list(emqx_tls_lib:to_server_opts(tls, SslOpts)),
    Https1 = maps:remove(ssl_options, Https0),
    Listeners#{https => maps:merge(Https1, SslOpt1)};
ensure_ssl_cert(Listeners) ->
    Listeners.

static_dispatch() ->
    StaticFiles = ["/editor.worker.js", "/json.worker.js", "/version"],
    [
        {"/", cowboy_static, {priv_file, emqx_dashboard, "www/index.html"}},
        {"/static/[...]", cowboy_static, {priv_dir, emqx_dashboard, "www/static"}}
    ] ++
        [{Path, cowboy_static, {priv_file, emqx_dashboard, "www" ++ Path}} || Path <- StaticFiles].

dynamic_dispatch() ->
    [
        {emqx_mgmt_api_status:path(), emqx_mgmt_api_status, []},
        {'_', emqx_dashboard_not_found, []}
    ].

emqx_api_name() ->
    emqx_release:description() ++ " API".

emqx_release_version() ->
    emqx_release:version().
