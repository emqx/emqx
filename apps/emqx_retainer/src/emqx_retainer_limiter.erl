%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_limiter).

-include("emqx_retainer.hrl").

-export([
    create/0,
    delete/0,
    update/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

create() ->
    emqx_limiter_shared:create_group(?RETAINER_LIMITER_GROUP, limiter_configs()).

delete() ->
    emqx_limiter_shared:delete_group(?RETAINER_LIMITER_GROUP).

update() ->
    emqx_limiter_shared:update_group_configs(?RETAINER_LIMITER_GROUP, limiter_configs()).

limiter_configs() ->
    DispatcherLimiterConfig =
        case emqx:get_config([retainer, flow_control, batch_deliver_limiter], undefined) of
            undefined ->
                emqx_limiter:config_unlimited();
            Rate ->
                emqx_limiter:config_from_rate(Rate)
        end,
    [{?DISPATCHER_LIMITER_NAME, DispatcherLimiterConfig}].
