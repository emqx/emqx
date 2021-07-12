%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , stop_listeners/0
        , start_listener/1
        , stop_listener/1]).

%% Authorization
-export([authorize_appid/1]).

-include_lib("emqx/include/emqx.hrl").

-define(APP, emqx_management).

-define(BASE_PATH, "/api/v5").

%%--------------------------------------------------------------------
%% Start/Stop Listeners
%%--------------------------------------------------------------------

start_listeners() ->
    lists:foreach(fun start_listener/1, listeners()).

stop_listeners() ->
    lists:foreach(fun stop_listener/1, listeners()).

start_listener({Proto, Port, Options}) ->
    {ok, _} = application:ensure_all_started(minirest),
    Authorization = {?MODULE, authorize_appid},
    RanchOptions = ranch_opts(Port, Options),
    GlobalSpec = #{
        swagger => "2.0",
        info => #{title => "EMQ X API", version => "5.0.0"},
        basePath => ?BASE_PATH,
        securityDefinitions => #{
            application => #{
                type => apiKey,
                name => "authorization",
                in => header}}},
    Minirest = #{
        protocol => Proto,
        base_path => ?BASE_PATH,
        apps => apps(),
        authorization => Authorization,
        security => [#{application => []}],
        swagger_global_spec => GlobalSpec},
    MinirestOptions = maps:merge(Minirest, RanchOptions),
    minirest:start(listener_name(Proto), MinirestOptions).

apps() ->
    Apps = [App || {App, _, _} <- application:loaded_applications(),
        case re:run(atom_to_list(App), "^emqx") of
            {match,[{0,4}]} -> true;
            _ -> false
        end],
    Plugins = lists:map(fun(Plugin) -> Plugin#plugin.name end, emqx_plugins:list()),
    Apps ++ Plugins.

ranch_opts(Port, Options0) ->
    Options = lists:foldl(
                  fun
                      ({K, _V}, Acc) when K =:= max_connections orelse K =:= num_acceptors -> Acc;
                      ({inet6, true}, Acc) -> [inet6 | Acc];
                      ({inet6, false}, Acc) -> Acc;
                      ({ipv6_v6only, true}, Acc) -> [{ipv6_v6only, true} | Acc];
                      ({ipv6_v6only, false}, Acc) -> Acc;
                      ({K, V}, Acc)->
                          [{K, V} | Acc]
                  end, [], Options0),
    maps:from_list([{port, Port} | Options]).

stop_listener({Proto, Port, _}) ->
    io:format("Stop http:management listener on ~s successfully.~n",[format(Port)]),
    minirest:stop(listener_name(Proto)).

listeners() ->
    [{Protocol, Port, maps:to_list(maps:without([protocol, port], Map))}
        || Map = #{protocol := Protocol,port := Port}
        <- emqx_config:get([emqx_management, listeners], [])].

listener_name(Proto) ->
    list_to_atom(atom_to_list(Proto) ++ ":management").

authorize_appid(Req) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, AppId, AppSecret} ->
            case emqx_mgmt_auth:is_authorized(AppId, AppSecret) of
                true -> ok;
                false -> {401}
            end;
        _ ->
            {401}
    end.

format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).
