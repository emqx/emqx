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

-module(emqx_mgmt_http).

-export([ start_listeners/0
        , handle_request/2
        , stop_listeners/0
        , start_listener/1
        , stop_listener/1
        ]).

-export([init/2]).

-export([ filter/1
        , authorize_appid/1
        ]).

-include_lib("emqx/include/emqx.hrl").

-define(APP, emqx_management).
-define(EXCEPT_PLUGIN, [emqx_dashboard]).
-ifdef(TEST).
-define(EXCEPT, []).
-else.
-define(EXCEPT, [add_app, del_app, list_apps, lookup_app, update_app]).
-endif.

%%--------------------------------------------------------------------
%% Start/Stop Listeners
%%--------------------------------------------------------------------

start_listeners() ->
    lists:foreach(fun start_listener/1, listeners()).

stop_listeners() ->
    lists:foreach(fun stop_listener/1, listeners()).

start_listener({Proto, Port, Options}) when Proto == http ->
    Dispatch = [{"/status", emqx_mgmt_http, []},
                {"/api/v4/[...]", minirest, http_handlers()}],
    minirest:start_http(listener_name(Proto), ranch_opts(Port, Options), Dispatch, proto_opts(Options));

start_listener({Proto, Port, Options}) when Proto == https ->
    Dispatch = [{"/status", emqx_mgmt_http, []},
                {"/api/v4/[...]", minirest, http_handlers()}],
    minirest:start_https(listener_name(Proto), ranch_opts(Port, Options), Dispatch, proto_opts(Options)).

ranch_opts(Port, Options0) ->
    NumAcceptors = proplists:get_value(num_acceptors, Options0, 4),
    MaxConnections = proplists:get_value(max_connections, Options0, 512),
    Options = lists:foldl(fun({K, _V}, Acc) when K =:= max_connections orelse K =:= num_acceptors ->
                                 Acc;
                             ({inet6, true}, Acc) -> [inet6 | Acc];
                             ({inet6, false}, Acc) -> Acc;
                             ({ipv6_v6only, true}, Acc) -> [{ipv6_v6only, true} | Acc];
                             ({ipv6_v6only, false}, Acc) -> Acc;
                             ({proxy_header, _}, Acc) -> Acc;
                             ({K, V}, Acc)->
                                 [{K, V} | Acc]
                          end, [], Options0),

    Res = #{num_acceptors => NumAcceptors,
            max_connections => MaxConnections,
            socket_opts => [{port, Port} | Options]},
    Res.

proto_opts(Options) ->
    maps:with([proxy_header], maps:from_list(Options)).

stop_listener({Proto, Port, _}) ->
    io:format("Stop http:management listener on ~s successfully.~n",[format(Port)]),
    minirest:stop_http(listener_name(Proto)).

listeners() ->
    application:get_env(?APP, listeners, []).

listener_name(Proto) ->
    %% NOTE: this name has referenced by emqx_management.appup.src.
    %% Please don't change it except you have got how to handle it in hot-upgrade
    list_to_atom(atom_to_list(Proto) ++ ":management").

http_handlers() ->
    Plugins = lists:map(fun(Plugin) -> Plugin#plugin.name end, emqx_plugins:list()),
    [{"/api/v4", minirest:handler(#{apps   => (Plugins ++
                                        [emqx_plugin_libs, emqx_modules]) -- ?EXCEPT_PLUGIN,
                                    except => ?EXCEPT,
                                    filter => fun ?MODULE:filter/1}),
                 [{authorization, fun ?MODULE:authorize_appid/1}]}].

%%--------------------------------------------------------------------
%% Handle 'status' request
%%--------------------------------------------------------------------
init(Req, Opts) ->
    Req1 = handle_request(cowboy_req:path(Req), Req),
    {ok, Req1, Opts}.

handle_request(Path, Req) ->
    handle_request(cowboy_req:method(Req), Path, Req).

handle_request(<<"GET">>, <<"/status">>, Req) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    AppStatus = case lists:keysearch(emqx, 1, application:which_applications()) of
        false         -> not_running;
        {value, _Val} -> running
    end,
    Status = io_lib:format("Node ~s is ~s~nemqx is ~s",
                            [node(), InternalStatus, AppStatus]),
    StatusCode = case AppStatus of
                     running -> 200;
                     not_running -> 503
                 end,
    cowboy_req:reply(StatusCode, #{<<"content-type">> => <<"text/plain">>}, Status, Req);

handle_request(_Method, _Path, Req) ->
    cowboy_req:reply(400, #{<<"content-type">> => <<"text/plain">>}, <<"Not found.">>, Req).

authorize_appid(Req) ->
    authorize_appid(
      iolist_to_binary(string:uppercase(cowboy_req:method(Req))),
      iolist_to_binary(cowboy_req:path(Req)),
      Req).

authorize_appid(<<"GET">>, <<"/api/v4/emqx_prometheus">>, _Req) ->
    true;
authorize_appid(_Method, _Path, Req) ->
    try
        {basic, AppId, AppSecret} = cowboy_req:parse_header(<<"authorization">>, Req),
        emqx_mgmt_auth:is_authorized(AppId, AppSecret)
    catch _:_ -> false
    end.

-ifdef(EMQX_ENTERPRISE).
filter(#{module := Module} = Route) ->
    %% true if anything goes wrong
    try
        case erlang:function_exported(Module, filter, 1) of
            true -> apply(Module, filter, [Route]);
            false -> true
        end
    catch _:_ ->
        true
    end;
filter(_Route) ->
    true.
-else.
filter(#{app := emqx_modules}) -> true;
filter(#{app := emqx_plugin_libs}) -> true;
filter(#{app := App}) ->
    case emqx_plugins:find_plugin(App) of
        false -> false;
        Plugin -> Plugin#plugin.active
    end.
-endif.

format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).
