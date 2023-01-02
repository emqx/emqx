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

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-import(proplists, [get_value/3]).

-export([ start_listeners/0
        , stop_listeners/0
        , start_listener/1
        , stop_listener/1
        ]).

%% for minirest
-export([ filter/1
        , is_authorized/1
        ]).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Start/Stop listeners.
%%--------------------------------------------------------------------

start_listeners() ->
    lists:foreach(fun(Listener) -> start_listener(Listener) end, listeners()).


%% Start HTTP(S) Listener
start_listener({Proto, Port, Options}) ->
    Dispatch = [{"/", cowboy_static, {priv_file, emqx_dashboard, "www/index.html"}},
                {"/static/[...]", cowboy_static, {priv_dir, emqx_dashboard, "www/static"}},
                {"/api/v4/[...]", minirest, http_handlers()}],
    Server = listener_name(Proto),
    RanchOpts = ranch_opts(Port, Options),
    case Proto of
        http -> minirest:start_http(Server, RanchOpts, Dispatch);
        https -> minirest:start_https(Server, RanchOpts, Dispatch)
    end.

ranch_opts(Bind, Options0) ->
    IpPort = ip_port(Bind),
    NumAcceptors = get_value(num_acceptors, Options0, 4),
    MaxConnections = get_value(max_connections, Options0, 512),
    Options = lists:foldl(fun({K, _V}, Acc) when K =:= max_connections orelse K =:= num_acceptors ->
                              Acc;
                             ({inet6, true}, Acc) -> [inet6 | Acc];
                             ({inet6, false}, Acc) -> Acc;
                             ({ipv6_v6only, true}, Acc) -> [{ipv6_v6only, true} | Acc];
                             ({ipv6_v6only, false}, Acc) -> Acc;
                             ({K, V}, Acc)->
                              [{K, V} | Acc]
                          end, [], Options0),
    #{num_acceptors => NumAcceptors,
      max_connections => MaxConnections,
      socket_opts => IpPort ++  Options}.

ip_port({IpStr, Port}) ->
    {ok, Ip} = inet:parse_address(IpStr),
    [{ip, Ip}, {port, Port}];
ip_port(Port) when is_integer(Port) ->
    [{port, Port}].

stop_listeners() ->
    lists:foreach(fun(Listener) -> stop_listener(Listener) end, listeners()).

stop_listener({Proto, _Port, _}) ->
    minirest:stop_http(listener_name(Proto)).

listeners() ->
    application:get_env(?APP, listeners, []).

listener_name(Proto) ->
    %% NOTE: this name has referenced by emqx_dashboard.appup.src.
    %% Please don't change it except you have got how to handle it in hot-upgrade
    list_to_atom(atom_to_list(Proto) ++ ":dashboard").

%%--------------------------------------------------------------------
%% HTTP Handlers and Dispatcher
%%--------------------------------------------------------------------

http_handlers() ->
    Plugins = lists:map(fun(Plugin) -> Plugin#plugin.name end, emqx_plugins:list()),
    [{"/api/v4/",
      minirest:handler(#{apps => Plugins ++ [emqx_modules, emqx_plugin_libs],
                         filter => fun ?MODULE:filter/1}),
      [{authorization, fun ?MODULE:is_authorized/1}]}].

%%--------------------------------------------------------------------
%% Basic Authorization
%%--------------------------------------------------------------------

is_authorized(Req) ->
    is_authorized(
      iolist_to_binary(string:uppercase(cowboy_req:method(Req))),
      iolist_to_binary(cowboy_req:path(Req)),
      Req).

is_authorized(<<"GET">>, <<"/api/v4/emqx_prometheus">>, _Req) ->
    true;
is_authorized(<<"POST">>, <<"/api/v4/auth">>, _Req) ->
    true;
is_authorized(_Method, _Path, Req) ->
    try
        {basic, Username, Password} = cowboy_req:parse_header(<<"authorization">>, Req),
        case emqx_dashboard_admin:check(iolist_to_binary(Username), iolist_to_binary(Password)) of
            ok -> true;
            {error, Reason} ->
                ?LOG(error, "[Dashboard] Authorization Failure: username=~s, reason=~p",
                    [Username, Reason]),
                false
        end
    catch _:_ -> %% bad authorization header will crash.
        false
    end.

filter(#{app := emqx_plugin_libs}) -> true;
filter(#{app := emqx_modules}) -> true;
filter(#{app := App}) ->
    case emqx_plugins:find_plugin(App) of
        false -> false;
        Plugin -> Plugin#plugin.active
    end.
