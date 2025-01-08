%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(EMQX_SESSION_DS_INTERNALS_HRL).
-define(EMQX_SESSION_DS_INTERNALS_HRL, true).

-include("emqx_persistent_message.hrl").
-include("emqx_durable_session_metadata.hrl").

-define(DURABLE_SESSION_STATE, emqx_persistent_session).

-define(SESSION_TAB, emqx_ds_session).
-define(SESSION_SUBSCRIPTIONS_TAB, emqx_ds_session_subscriptions).
-define(SESSION_STREAM_TAB, emqx_ds_stream_tab).
-define(SESSION_PUBRANGE_TAB, emqx_ds_pubrange_tab).
-define(SESSION_COMMITTED_OFFSET_TAB, emqx_ds_committed_offset_tab).
-define(DS_MRIA_SHARD, emqx_ds_session_shard).

%%%%% Session sequence numbers:

%%
%%   -----|----------|-----|-----|------> seqno
%%        |          |     |     |
%%   committed      dup   rec   next
%%                       (Qos2)

%% Seqno becomes committed after receiving PUBACK for QoS1 or PUBCOMP
%% for QoS2.
-define(committed(QOS), QOS).
%% Seqno becomes dup after broker sends QoS1 or QoS2 message to the
%% client. Upon session reconnect, messages with seqno in the
%% committed..dup range are retransmitted with DUP flag.
%%
-define(dup(QOS), (10 + QOS)).
%% Rec flag is specific for the QoS2. It contains seqno of the last
%% PUBREC received from the client. When the session reconnects,
%% PUBREL packages for the dup..rec range are retransmitted.
-define(rec, 22).
%% Last seqno assigned to a message (it may not be sent yet).
-define(next(QOS), (30 + QOS)).

%%%%% Stream Replay State:
-record(srs, {
    rank_x :: emqx_ds:rank_x(),
    rank_y :: emqx_ds:rank_y(),
    %% Iterators at the beginning and the end of the last batch:
    it_begin :: emqx_ds:iterator() | undefined,
    it_end :: emqx_ds:iterator() | end_of_stream,
    %% Size of the last batch:
    batch_size = 0 :: non_neg_integer(),
    %% Session sequence numbers at the time when the batch was fetched:
    first_seqno_qos1 = 0 :: emqx_persistent_session_ds:seqno(),
    first_seqno_qos2 = 0 :: emqx_persistent_session_ds:seqno(),
    %% Sequence numbers that have to be committed for the batch:
    last_seqno_qos1 = 0 :: emqx_persistent_session_ds:seqno(),
    last_seqno_qos2 = 0 :: emqx_persistent_session_ds:seqno(),
    %% This stream belongs to an unsubscribed topic-filter, and is
    %% marked for deletion:
    unsubscribed = false :: boolean(),
    %% Reference to the subscription state:
    sub_state_id :: emqx_persistent_session_ds_subs:subscription_state_id()
}).

%% (Erlang) messages that session should forward to the
%% shared subscription handler.
-record(shared_sub_message, {
    subscription_id :: emqx_persistent_session_ds:subscription_id(),
    message :: term()
}).
-define(shared_sub_message(SUBSCRIPTION_ID, MSG), #shared_sub_message{
    subscription_id = SUBSCRIPTION_ID,
    message = MSG
}).
-define(shared_sub_message, #shared_sub_message{}).

-endif.
