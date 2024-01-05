%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(EMQX_PERSISTENT_SESSION_DS_HRL_HRL).
-define(EMQX_PERSISTENT_SESSION_DS_HRL_HRL, true).

-define(PERSISTENT_MESSAGE_DB, emqx_persistent_message).

-define(SESSION_TAB, emqx_ds_session).
-define(SESSION_SUBSCRIPTIONS_TAB, emqx_ds_session_subscriptions).
-define(SESSION_STREAM_TAB, emqx_ds_stream_tab).
-define(SESSION_PUBRANGE_TAB, emqx_ds_pubrange_tab).
-define(SESSION_COMMITTED_OFFSET_TAB, emqx_ds_committed_offset_tab).
-define(DS_MRIA_SHARD, emqx_ds_session_shard).

%%%%% Session sequence numbers:

%%
%%   -----|----------|----------|------> seqno
%%        |          |          |
%%   committed      dup       next

%% Seqno becomes committed after receiving PUBACK for QoS1 or PUBCOMP
%% for QoS2.
-define(committed(QOS), {0, QOS}).
%% Seqno becomes dup:
%%
%% 1. After broker sends QoS1 message to the client
%% 2. After it receives PUBREC from the client for the QoS2 message
-define(dup(QOS), {1, QOS}).
%% Last seqno assigned to some message (that may reside in the
%% mqueue):
-define(next(QOS), {0, QOS}).

%%%%% State of the stream:
-record(ifs, {
    rank_x :: emqx_ds:rank_x(),
    rank_y :: emqx_ds:rank_y(),
    %% Iterator at the end of the last batch:
    it_end :: emqx_ds:iterator() | end_of_stream,
    %% Key that points at the beginning of the batch:
    batch_begin_key :: binary() | undefined,
    batch_size = 0 :: non_neg_integer(),
    %% Session sequence number at the time when the batch was fetched:
    first_seqno_qos1 = 0 :: emqx_persistent_session_ds:seqno(),
    first_seqno_qos2 = 0 :: emqx_persistent_session_ds:seqno(),
    %% Number of messages collected in the last batch:
    last_seqno_qos1 = 0 :: emqx_persistent_session_ds:seqno(),
    last_seqno_qos2 = 0 :: emqx_persistent_session_ds:seqno()
}).

%% TODO: remove
-record(session, {
    %% same as clientid
    id :: emqx_persistent_session_ds:id(),
    %% creation time
    created_at :: _Millisecond :: non_neg_integer(),
    last_alive_at :: _Millisecond :: non_neg_integer(),
    conninfo :: emqx_types:conninfo(),
    %% for future usage
    props = #{} :: map()
}).

-endif.
