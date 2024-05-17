%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_ds_shared_subs).

-include("emqx_mqtt.hrl").

-record(state, {
    sub_states :: #{
        emqx_persistent_session_ds:subscription_id() => shared_sub_state()
    }
}).

-type state() :: #state{}.
-type shared_sub_state() ::
    {connecting, connecting_data()} | {replaying, replaying_data()} | {updating, updating_data()}.

-type connecting_data() :: #{}.
-type replaying_data() :: #{}.
-type updating_data() :: #{}.

-type shared_topic_filter() :: #share{}.
-type event() :: term().

-export([
    open/1,
    new/0,

    on_subscribe/3,
    on_unsubscribe/4,
    on_session_drop/2,
    on_timeout/3,

    find_new_streams/2,
    put_stream/3,

    to_map/2
]).

-export_type([
    state/0,
    shared_topic_filter/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> state().
new() -> #state{sub_states = #{}}.

-spec open(emqx_persistent_session_ds_state:t()) ->
    {ok, emqx_persistent_session_ds_state:t(), state()}.
open(SessionS) ->
    {ok, SessionS, #state{sub_states = #{}}}.

-spec on_subscribe(
    shared_topic_filter(), emqx_types:subopts(), emqx_persistent_session_ds:session()
) ->
    {ok, emqx_persistent_session_ds_state:t(), state()}.
on_subscribe(_TopicFilter, _SubOpts, #{s := S, shared_sub_s := SS} = _Session) ->
    {ok, S, SS}.

-spec on_unsubscribe(
    emqx_persistent_session_ds:id(),
    shared_topic_filter(),
    emqx_persistent_session_ds_state:t(),
    state()
) ->
    {ok, emqx_persistent_session_ds_state:t(), state(), emqx_persistent_session_ds:subscription()}
    | {error, ?RC_NO_SUBSCRIPTION_EXISTED}.
on_unsubscribe(_Id, _TopicFilter, _SessionS, _S) ->
    {error, ?RC_NO_SUBSCRIPTION_EXISTED}.

-spec on_timeout(event(), emqx_persistent_session_ds_state:t(), state()) -> ok.
on_timeout(_Event, _SessionS, _S) -> ok.

-spec on_session_drop(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) -> ok.
on_session_drop(_SessionId, _SessionS) -> ok.

-spec find_new_streams(emqx_persistent_session_ds_state:t(), state()) ->
    [{emqx_persistent_session_ds_state:stream_key(), emqx_persistent_session_ds:stream_state()}].
find_new_streams(_SessionS, _S) -> [].

-spec put_stream(
    emqx_persistent_session_ds_state:stream_key(),
    emqx_persistent_session_ds:stream_state(),
    state()
) -> state().
put_stream(_Key, _Stream, S) -> S.

-spec to_map(emqx_persistent_session_ds_state:t(), state()) -> map().
to_map(_SessionS, _S) -> #{}.
