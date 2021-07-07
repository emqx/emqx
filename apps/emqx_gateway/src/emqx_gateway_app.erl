%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_app).

-behaviour(application).

-include_lib("emqx/include/logger.hrl").

-logger_header("[Gateway]").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_gateway_sup:start_link(),
    emqx_gateway_cli:load(),
    load_default_gateway_applications(),
    create_gateway_by_default(),
    {ok, Sup}.

stop(_State) ->
    emqx_gateway_cli:unload(),
    ok.

%%--------------------------------------------------------------------
%% Internal funcs

load_default_gateway_applications() ->
    Apps = gateway_type_searching(),
    ?LOG(info, "Starting the default gateway types: ~p", [Apps]),
    lists:foreach(fun load/1, Apps).

gateway_type_searching() ->
    %% FIXME: Hardcoded apps
    [emqx_stomp_impl].

load(Mod) ->
    try
        Mod:load(),
        ?LOG(info, "Load ~s gateway application successfully!", [Mod])
    catch
        Class : Reason ->
            ?LOG(error, "Load ~s gateway application failed: {~p, ~p}",
                        [Mod, Class, Reason])
    end.

create_gateway_by_default() ->
    create_gateway_by_default(zipped_confs()).

create_gateway_by_default([]) ->
    ok;
create_gateway_by_default([{Type, Name, Confs}|More]) ->
    case emqx_gateway_registry:lookup(Type) of
        undefined ->
            ?LOG(error, "Skip to start ~p#~p: not_registred_type",
                        [Type, Name]);
        _ ->
            case emqx_gateway:create(Type,
                                     atom_to_binary(Name, utf8),
                                     <<>>,
                                     Confs) of
                {ok, _} ->
                    ?LOG(debug, "Start ~p#~p successfully!", [Type, Name]);
                {error, Reason} ->
                    ?LOG(error, "Start ~p#~p failed: ~0p",
                                [Type, Name, Reason])
            end
    end,
    create_gateway_by_default(More).

zipped_confs() ->
    All = maps:to_list(emqx_config:get([emqx_gateway])),
    lists:append(lists:foldr(
      fun({Type, Gws}, Acc) ->
        {Names, Confs} = lists:unzip(maps:to_list(Gws)),
        Types = [ Type || _ <- lists:seq(1, length(Names))],
        [lists:zip3(Types, Names, Confs) | Acc]
      end, [], All)).
