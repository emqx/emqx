%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(SESSION_MARKER_TAB, emqx_ds_marker_tab).
-define(DS_MRIA_SHARD, emqx_ds_session_shard).

%% Integer tags for `misc` maps keys.
-define(T_tracks, 1).

-record(ds_sub, {
    id :: emqx_persistent_session_ds:subscription_id(),
    start_time :: emqx_ds:time(),
    props = #{} :: map(),
    extra = #{} :: map()
}).
-type ds_sub() :: #ds_sub{}.

-record(ds_stream, {
    session :: emqx_persistent_session_ds:id(),
    ref :: _StreamRef,
    stream :: emqx_ds:stream(),
    rank :: emqx_ds:stream_rank(),
    beginning :: emqx_ds:iterator()
}).
-type ds_stream() :: #ds_stream{}.

-record(ds_pubrange, {
    id :: {
        %% What session this range belongs to.
        _Session :: emqx_persistent_session_ds:id(),
        %% Where this range starts.
        _First :: emqx_persistent_message_ds_replayer:seqno()
    },
    %% Where this range ends: the first seqno that is not included in the range.
    until :: emqx_persistent_message_ds_replayer:seqno(),
    %% Which stream this range is over.
    stream :: _StreamRef,
    %% Type of a range:
    %% * Inflight range is a range of yet unacked messages from this stream.
    %% * Checkpoint range was already acked, its purpose is to keep track of the
    %%   very last iterator for this stream.
    type :: inflight | checkpoint,
    %% Meaning of this depends on the type of the range:
    %% * For inflight range, this is the iterator pointing to the first message in
    %%   the range.
    %% * For checkpoint range, this is the iterator pointing right past the last
    %%   message in the range.
    iterator :: emqx_ds:iterator(),
    %% Reserved for future use.
    misc = #{} :: #{
        %% What commit tracks this range is part of.
        %% This is rarely stored: we only need to persist it when the range
        %% contains QoS 2 messages.
        ?T_tracks => non_neg_integer(),
        _ => _
    }
}).
-type ds_pubrange() :: #ds_pubrange{}.

-record(ds_marker, {
    id :: {
        %% What session this marker belongs to.
        _Session :: emqx_persistent_session_ds:id(),
        %% Marker name.
        _MarkerName
    },
    %% Where this marker is pointing to: the first seqno that is not marked.
    until :: emqx_persistent_message_ds_replayer:seqno()
}).

-record(session, {
    %% same as clientid
    id :: emqx_persistent_session_ds:id(),
    %% creation time
    created_at :: _Millisecond :: non_neg_integer(),
    expires_at = never :: _Millisecond :: non_neg_integer() | never,
    %% for future usage
    props = #{} :: map()
}).

-endif.
