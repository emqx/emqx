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
-module(emqx_config_zones).

-behaviour(emqx_config_handler).

%% API
-export([add_handler/0, remove_handler/0, pre_config_update/3]).

-define(ZONES, [zones]).

add_handler() ->
    ok = emqx_config_handler:add_handler(?ZONES, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?ZONES),
    ok.

%% replace the old config with the new config
pre_config_update(?ZONES, NewRaw, _OldRaw) ->
    {ok, NewRaw}.
