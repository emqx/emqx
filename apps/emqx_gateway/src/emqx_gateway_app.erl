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
    load_default_gateway_applications(),
    {ok, Sup}.

stop(_State) ->
    %% XXX: ?
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
        case erlang:function_exported(Mod, load, 0) of
            true ->
                Mod:load(),
                ?LOG(info, "Load ~s gateway application successfully!", [Mod]);
            false -> ok
        end
    catch
        Class : Reason ->
            ?LOG(error, "Load ~s gateway application failed: {~p, ~p}",
                        [Mod, Class, Reason])
    end.
