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
-module(emqx_config).

-compile({no_auto_import, [get/0, get/1]}).

-export([ get/0
        , get/2
        , put/1
        , deep_get/3
        ]).

-spec get() -> term().
get() ->
    persistent_term:get(?MODULE, #{}).

-spec get([atom()], term()) -> term().
get(KeyPath, Default) ->
    deep_get(KeyPath, get(), Default).

deep_get([], Map, _Default) ->
    Map;
deep_get([Key | KeyPath], Map, Default) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, SubMap} -> deep_get(KeyPath, SubMap, Default);
        error -> Default
    end;
deep_get([_Key | _KeyPath], _Map, Default) ->
    Default.

put(Config) ->
    persistent_term:put(?MODULE, Config).
