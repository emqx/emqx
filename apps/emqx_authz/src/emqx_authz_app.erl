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

%%%-------------------------------------------------------------------
%% @doc emqx_authz public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_authz_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_authz_mnesia:init_tables(),
    {ok, Sup} = emqx_authz_sup:start_link(),
    ok = emqx_authz:init(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_authz:deinit(),
    ok.
