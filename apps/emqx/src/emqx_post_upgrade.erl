%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_post_upgrade).

%% PR#12765
-export([
    pr12765_update_stats_timer/1,
    pr12765_revert_stats_timer/1
]).

-include("logger.hrl").

%%------------------------------------------------------------------------------
%% Hot Upgrade Callback Functions.
%%------------------------------------------------------------------------------
pr12765_update_stats_timer(_FromVsn) ->
    emqx_stats:update_interval(broker_stats, fun emqx_broker_helper:stats_fun/0).

pr12765_revert_stats_timer(_ToVsn) ->
    emqx_stats:update_interval(broker_stats, fun emqx_broker:stats_fun/0).

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------
