%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/http_api.hrl").
-include_lib("emqx/include/emqx_release.hrl").

-define(BASE_PATH, "/api/v5").

-define(EMQX_MIDDLE, emqx_dashboard_middleware).

%%--------------------------------------------------------------------
%% Start/Stop Listeners
%%--------------------------------------------------------------------

start_listeners() ->
    start_listeners(listeners()).

stop_listeners() ->
    stop_listeners(listeners()).

start_listeners(Listeners) ->
    {ok, _} = application:ensure_all_started(minirest),
    Authorization = {?MODULE, authorize},
    GlobalSpec = #{
        openapi => "3.0.0",
        info => #{title => "EMQX API", version => ?EMQX_API_VERSION},
        servers => [#{url => ?BASE_PATH}],
        components => #{
            schemas => #{},
            'securitySchemes' => #{
                'basicAuth' => #{
                    type => http,
                    scheme => basic,
                    description =>
                        <<"Authorize with [API Keys](https://www.emqx.io/docs/en/v5.0/admin/api.html#api-keys)">>
                }
            }
        }
    },
    Dispatch = [
        {"/", cowboy_static, {priv_file, emqx_dashboard, "www/index.html"}},
        {"/static/[...]", cowboy_static, {priv_dir, emqx_dashboard, "www/static"}},
        {emqx_mgmt_api_status:path(), emqx_mgmt_api_status, []},
        {?BASE_PATH ++ "/[...]", emqx_dashboard_bad_api, []},
        {'_', cowboy_static, {priv_file, emqx_dashboard, "www/index.html"}}
    ],
    BaseMinirest = #{
        base_path => ?BASE_PATH,
        modules => minirest_api:find_api_modules(apps()),
        authorization => Authorization,
        security => [#{'basicAuth' => []}, #{'bearerAuth' => []}],
        swagger_global_spec => GlobalSpec,
        dispatch => Dispatch,
        middlewares => [?EMQX_MIDDLE, cowboy_router, cowboy_handler]
    },
    {OkListeners, ErrListeners} =
        lists:foldl(
            fun({Name, Protocol, Bind, RanchOptions, ProtoOpts}, {OkAcc, ErrAcc}) ->
                Minirest = BaseMinirest#{protocol => Protocol, protocol_options => ProtoOpts},
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
            listeners(Listeners)
        ),
    case ErrListeners of
        [] ->
            optvar:set(emqx_dashboard_listeners_ready, OkListeners),
            ok;
        _ ->
            {error, ErrListeners}
    end.

stop_listeners(Listeners) ->
    optvar:unset(emqx_dashboard_listeners_ready),
    [
        begin
            case minirest:stop(Name) of
                ok ->
                    ?ULOG("Stop listener ~ts on ~ts successfully.~n", [
                        Name, emqx_listeners:format_bind(Port)
                    ]);
                {error, not_found} ->
                    ?SLOG(warning, #{msg => "stop_listener_failed", name => Name, port => Port})
            end
        end
     || {Name, _, Port, _, _} <- listeners(Listeners)
    ],
    ok.

wait_for_listeners() ->
    optvar:read(emqx_dashboard_listeners_ready).

%%--------------------------------------------------------------------
%% internal

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
        fun({Protocol, Conf}) ->
            maps:get(enable, Conf) andalso
                begin
                    {Conf1, Bind} = ip_port(Conf),
                    {true, {
                        listener_name(Protocol),
                        Protocol,
                        Bind,
                        ranch_opts(Conf1),
                        proto_opts(Conf1)
                    }}
                end
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
        maps:without([enable, inet6, ipv6_v6only, proxy_header | Keys], Options)
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

proto_opts(Options) ->
    maps:with([proxy_header], Options).

filter_false(_K, false, S) -> S;
filter_false(K, V, S) -> [{K, V} | S].

listener_name(Protocol) ->
    list_to_atom(atom_to_list(Protocol) ++ ":dashboard").

authorize(Req) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Username, Password} ->
            api_key_authorize(Req, Username, Password);
        {bearer, Token} ->
            case emqx_dashboard_admin:verify_token(Token) of
                ok ->
                    ok;
                {error, token_timeout} ->
                    {401, 'TOKEN_TIME_OUT', <<"Token expired, get new token by POST /login">>};
                {error, not_found} ->
                    {401, 'BAD_TOKEN', <<"Get a token by POST /login">>}
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
                <<"Basic Realm=\"minirest-server\"">>
        },
        #{code => Code, message => Message}}.

listeners() ->
    emqx_conf:get([dashboard, listeners], #{}).

api_key_authorize(Req, Key, Secret) ->
    Path = cowboy_req:path(Req),
    case emqx_mgmt_auth:authorize(Path, Key, Secret) of
        ok ->
            ok;
        {error, <<"not_allowed">>} ->
            return_unauthorized(
                ?BAD_API_KEY_OR_SECRET,
                <<"Not allowed, Check api_key/api_secret">>
            );
        {error, _} ->
            return_unauthorized(
                ?BAD_API_KEY_OR_SECRET,
                <<"Check api_key/api_secret">>
            )
    end.
