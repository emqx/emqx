%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_app).

-include("rule_engine.hrl").

-behaviour(application).

-export([start/2]).

-export([stop/1]).

start(_Type, _Args) ->
    _ = ets:new(?RULE_TAB, [named_table, public, set, {read_concurrency, true}]),
    ok = emqx_rule_events:reload(),
    SupRet = emqx_rule_engine_sup:start_link(),
    ok = emqx_rule_engine:load_rules(),
    emqx_conf:add_handler(emqx_rule_engine:config_key_path(), emqx_rule_engine),
    emqx_rule_engine_cli:load(),
    SupRet.

stop(_State) ->
    emqx_rule_engine_cli:unload(),
    emqx_conf:remove_handler(emqx_rule_engine:config_key_path()),
    ok = emqx_rule_events:unload().
