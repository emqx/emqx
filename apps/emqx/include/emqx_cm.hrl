%%-------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_CM_HRL).
-define(EMQX_CM_HRL, true).

%% Tables for channel management.
-define(CHAN_TAB, emqx_channel).
-define(CHAN_CONN_TAB, emqx_channel_conn).
-define(CHAN_INFO_TAB, emqx_channel_info).
-define(CHAN_LIVE_TAB, emqx_channel_live).

%% Mria table for session registration.
-define(CHAN_REG_TAB, emqx_channel_registry).

-define(T_KICK, 5_000).
-define(T_GET_INFO, 5_000).
-define(T_TAKEOVER, 15_000).

-define(CM_POOL, emqx_cm_pool).

%% Registered sessions.
-record(channel, {
    chid :: emqx_types:clientid() | '_',
    %% pid field is extended in 5.6.0 to support recording unregistration timestamp.
    pid :: pid() | non_neg_integer() | '$1'
}).

%% Map from channel pid to connection module and client ID.
-record(chan_conn, {
    pid :: pid() | '_' | '$1',
    mod :: module() | '_',
    clientid :: emqx_types:clientid() | '_'
}).

-endif.
