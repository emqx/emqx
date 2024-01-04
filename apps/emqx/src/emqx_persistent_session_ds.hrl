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

%% State of the stream:
-record(ifs, {
    rank_x :: emqx_ds:rank_x(),
    rank_y :: emqx_ds:rank_y(),
    %% Iterator at the end of the last batch:
    it_end :: emqx_ds:iterator() | undefined | end_of_stream,
    %% Size of the last batch:
    batch_size :: pos_integer() | undefined,
    %% Key that points at the beginning of the batch:
    batch_begin_key :: binary() | undefined,
    %% Number of messages collected in the last batch:
    batch_n_messages :: pos_integer() | undefined,
    %% Session sequence number at the time when the batch was fetched:
    first_seqno_qos1 :: emqx_persistent_session_ds:seqno() | undefined,
    first_seqno_qos2 :: emqx_persistent_session_ds:seqno() | undefined,
    %% Sequence numbers that the client must PUBACK or PUBREL
    %% before we can consider the batch to be fully replayed:
    last_seqno_qos1 :: emqx_persistent_session_ds:seqno() | undefined,
    last_seqno_qos2 :: emqx_persistent_session_ds:seqno() | undefined
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
