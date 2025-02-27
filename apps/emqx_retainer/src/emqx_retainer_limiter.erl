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

-spec create() -> ok | {error, term()}.
create() ->
    emqx_limiter:create_group(shared, ?RETAINER_LIMITER_GROUP, limiter_configs()).

-spec delete() -> ok | {error, term()}.
delete() ->
    emqx_limiter:delete_group(?RETAINER_LIMITER_GROUP).

-spec update() -> ok.
update() ->
    emqx_limiter:update_group(?RETAINER_LIMITER_GROUP, limiter_configs()).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

limiter_configs() ->
    DispatcherLimiterConfig =
        case emqx:get_config([retainer, flow_control, batch_deliver_limiter], infinity) of
            infinity ->
                emqx_limiter:config_unlimited();
            Rate ->
                emqx_limiter:config_from_rate(Rate)
        end,
    PublisherLimiterConfig = emqx_limiter:config(max_publish, emqx_config:get([retainer])),
    [
        {?DISPATCHER_LIMITER_NAME, DispatcherLimiterConfig},
        {?PUBLISHER_LIMITER_NAME, PublisherLimiterConfig}
    ].
