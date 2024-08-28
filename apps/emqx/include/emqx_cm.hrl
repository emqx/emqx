%%-------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(IS_CLIENTID(CLIENTID),
    (is_binary(CLIENTID) orelse (is_atom(CLIENTID) andalso CLIENTID =/= undefined))
).

-define(IS_CID(CID),
    (is_tuple(CID) andalso tuple_size(CID) == 2 andalso ?IS_CLIENTID(element(2, CID)))
).

-define(NO_MTNS, undefined).

-define(WITH_EMPTY_MTNS(ClientId), {undefined, ClientId}).

%% Registered sessions.
-record(channel, {
    chid :: emqx_types:cid() | '_',
    %% pid field is extended in 5.6.0 to support recording unregistration timestamp.
    pid :: pid() | non_neg_integer() | '$1'
}).

-endif.
