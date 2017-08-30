%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

%% @doc Hot Configuration
%%
%% TODO: How to persist the configuration?
%% 
%% 1. Store in mnesia database?
%% 2. Store in dets?
%% 3. Store in data/app.config?
%%

-module(emqttd_config).

-export([read/1, dump/2, reload/1, get/2, get/3, set/3]).

-type(env() :: {atom(), term()}).

%% @doc Read the configuration of an application.
-spec(read(atom()) -> {ok, list(env())} | {error, term()}).
read(_App) ->
    %% TODO: 
    %% 1. Read the app.conf from etc folder
    %% 2. Cuttlefish to read the conf
    %% 3. Return the terms and schema
    {error, unsupported}.

%% @doc Reload configuration of an application.
-spec(reload(atom()) -> ok | {error, term()}).
reload(_App) ->
    %% TODO
    %% 1. Read the app.conf from etc folder
    %% 2. Cuttlefish to generate config terms.
    %% 3. set/3 to apply the config
    ok.

-spec(dump(atom(), list(env())) -> ok | {error, term()}).
dump(_App, _Terms) ->
    %% TODO
    ok.

-spec(set(atom(), list(), list()) -> ok).
set(App, Par, Val) ->
    emqttd_cli_config:run(["config",
                            "set",
                            lists:concat([Par, "=", Val]),
                            lists:concat(["--app=", App])]).

-spec(get(atom(), list()) -> undefined | {ok, term()}).
get(App, Par) ->
    case emqttd_cli_config:get_cfg(App, Par) of
        undefined -> undefined;
        Val -> {ok, Val}
    end.

-spec(get(atom(), list(), atom()) -> term()).
get(App, Par, Def) ->
    emqttd_cli_config:get_cfg(App, Par, Def).

