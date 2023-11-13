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
-define(SESSION_ITER_TAB, emqx_ds_iter_tab).
-define(DS_MRIA_SHARD, emqx_ds_session_shard).

-record(ds_sub, {
    id :: emqx_persistent_session_ds:subscription_id(),
    start_time :: emqx_ds:time(),
    props = #{} :: map(),
    extra = #{} :: map()
}).
-type ds_sub() :: #ds_sub{}.

-record(ds_stream, {
    session :: emqx_persistent_session_ds:id(),
    topic_filter :: emqx_ds:topic_filter(),
    stream :: emqx_ds:stream(),
    rank :: emqx_ds:stream_rank()
}).
-type ds_stream() :: #ds_stream{}.

-record(ds_iter, {
    id :: {emqx_persistent_session_ds:id(), emqx_ds:stream()},
    iter :: emqx_ds:iterator()
}).

-record(session, {
    %% same as clientid
    id :: emqx_persistent_session_ds:id(),
    %% creation time
    created_at :: _Millisecond :: non_neg_integer(),
    expires_at = never :: _Millisecond :: non_neg_integer() | never,
    inflight :: emqx_persistent_message_ds_replayer:inflight(),
    %% for future usage
    props = #{} :: map()
}).

-endif.
