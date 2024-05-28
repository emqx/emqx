%%--------------------------------------------------------------------
%% Copyright (c) 2022, 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This header contains definitions of durable session metadata
%% keys, that can be consumed by the external code.
-ifndef(EMQX_DURABLE_SESSION_META_HRL).
-define(EMQX_DURABLE_SESSION_META_HRL, true).

%% Session metadata keys:
-define(created_at, created_at).
-define(last_alive_at, last_alive_at).
-define(expiry_interval, expiry_interval).
%% Unique integer used to create unique identities:
-define(last_id, last_id).
%% Connection info (relevent for the dashboard):
-define(peername, peername).
-define(will_message, will_message).
-define(clientinfo, clientinfo).
-define(protocol, protocol).
-define(offline_info, offline_info).

-endif.
