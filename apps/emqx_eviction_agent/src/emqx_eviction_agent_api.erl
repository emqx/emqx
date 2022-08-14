%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_api).

-include_lib("emqx/include/logger.hrl").

-rest_api(#{name   => node_eviction_status,
            method => 'GET',
            path   => "/node_eviction/status",
            func   => status,
            descr  => "Get node eviction status"}).

-export([status/2]).

status(_Bindings, _Params) ->
    case emqx_eviction_agent:status() of
        disabled ->
            {ok, #{status => disabled}};
        {enabled, Stats} ->
            {ok, #{status => enabled,
                   stats => Stats}}
    end.
