%%--------------------------------------------------------------------
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

-ifndef(EMQX_SESSION_MEM_HRL).
-define(EMQX_SESSION_MEM_HRL, true).

-record(session, {
    %% Client's id
    clientid :: emqx_types:clientid(),
    id :: emqx_session:session_id(),
    %% Is this session a persistent session i.e. was it started with Session-Expiry > 0
    is_persistent :: boolean(),
    %% Client’s Subscriptions.
    subscriptions :: map(),
    %% Max subscriptions allowed
    max_subscriptions :: non_neg_integer() | infinity,
    %% Upgrade QoS?
    upgrade_qos = false :: boolean(),
    %% Client <- Broker: QoS1/2 messages sent to the client but
    %% have not been unacked.
    inflight :: emqx_inflight:inflight(),
    %% All QoS1/2 messages published to when client is disconnected,
    %% or QoS1/2 messages pending transmission to the Client.
    %%
    %% Optionally, QoS0 messages pending transmission to the Client.
    mqueue :: emqx_mqueue:mqueue(),
    %% Next packet id of the session
    next_pkt_id = 1 :: emqx_types:packet_id(),
    %% Retry interval for redelivering QoS1/2 messages (Unit: millisecond)
    retry_interval :: timeout(),
    %% Client -> Broker: QoS2 messages received from the client, but
    %% have not been completely acknowledged
    awaiting_rel :: map(),
    %% Maximum number of awaiting QoS2 messages allowed
    max_awaiting_rel :: non_neg_integer() | infinity,
    %% Awaiting PUBREL Timeout (Unit: millisecond)
    await_rel_timeout :: timeout(),
    %% Created at
    created_at :: pos_integer()
}).

-endif.
