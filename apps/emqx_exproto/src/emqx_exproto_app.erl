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

-module(emqx_exproto_app).

-behaviour(application).

-emqx_plugin(extension).

-export([start/2, prep_stop/1, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_exproto_sup:start_link(),
    emqx_exproto:start_servers(),
    emqx_exproto:start_listeners(),
    {ok, Sup}.

prep_stop(State) ->
    emqx_exproto:stop_servers(),
    emqx_exproto:stop_listeners(),
    State.

stop(_State) ->
    ok.
