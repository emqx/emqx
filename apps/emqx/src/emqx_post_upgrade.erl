%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Example of a hot upgrade callback function.
%% PR#12765
% -export([
%     pr12765_update_stats_timer/1,
%     pr20000_ensure_sup_started/3
% ]).

%% Please ensure that every callback function is reentrant.
%% This way, users can attempt upgrade multiple times if an issue arises.
%%
% pr12765_update_stats_timer(_FromVsn) ->
%     emqx_stats:update_interval(broker_stats, fun emqx_broker_helper:stats_fun/0).
%
% pr20000_ensure_sup_started(_FromVsn, "5.6.1" ++ _, ChildSpec) ->
%     ChildId = maps:get(id, ChildSpec),
%     case supervisor:terminate_child(emqx_sup, ChildId) of
%         ok -> supervisor:delete_child(emqx_sup, ChildId);
%         Error -> Error
%     end,
%     supervisor:start_child(emqx_sup, ChildSpec);
% pr20000_ensure_sup_started(_FromVsn, _TargetVsn, _) ->
%     ok.
