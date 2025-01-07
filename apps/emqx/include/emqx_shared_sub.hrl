%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_SHARED_SUB_HRL).
-define(EMQX_SHARED_SUB_HRL, true).

%% Mnesia table for shared sub message routing
-define(SHARED_SUBSCRIPTION, emqx_shared_subscription).

%% ETS tables for Shared PubSub
-define(SHARED_SUBSCRIBER, emqx_shared_subscriber).
-define(SHARED_SUBS_ROUND_ROBIN_COUNTER, emqx_shared_subscriber_round_robin_counter).

-endif.
