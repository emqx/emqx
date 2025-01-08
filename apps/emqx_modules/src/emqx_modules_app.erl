%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    ok = mria:wait_for_tables(emqx_delayed:create_tables()),
    {ok, Sup} = emqx_modules_sup:start_link(),
    maybe_enable_modules(),
    {ok, Sup}.

stop(_State) ->
    maybe_disable_modules(),
    ok.

maybe_enable_modules() ->
    emqx_conf:get([delayed, enable], true) andalso emqx_delayed:load(),
    emqx_observer_cli:enable(),
    emqx_conf_cli:load(),
    ok = emqx_rewrite:enable(),
    emqx_topic_metrics:enable(),
    emqx_modules_conf:load().

maybe_disable_modules() ->
    emqx_conf:get([delayed, enable], true) andalso emqx_delayed:unload(),
    emqx_conf:get([observer_cli, enable], true) andalso emqx_observer_cli:disable(),
    emqx_rewrite:disable(),
    emqx_conf_cli:unload(),
    emqx_topic_metrics:disable(),
    emqx_modules_conf:unload().
