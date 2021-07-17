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
-module(emqx_data_bridge_app).

-behaviour(application).

-behaviour(emqx_config_handler).

-export([start/2, stop/1, handle_update_config/2]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_data_bridge_sup:start_link(),
    ok = emqx_data_bridge:load_bridges(),
    emqx_config_handler:add_handler(emqx_data_bridge:config_key_path(), ?MODULE),
    {ok, Sup}.

stop(_State) ->
    ok.

%% internal functions
handle_update_config({update, Bridge = #{<<"name">> := Name}}, OldConf) ->
    [Bridge | remove_bridge(Name, OldConf)];
handle_update_config({delete, Name}, OldConf) ->
    remove_bridge(Name, OldConf);
handle_update_config(NewConf, _OldConf) when is_list(NewConf) ->
    %% overwrite the entire config!
    NewConf.

remove_bridge(_Name, undefined) ->
    [];
remove_bridge(Name, OldConf) ->
    [B || B = #{<<"name">> := Name0} <- OldConf, Name0 =/= Name].
