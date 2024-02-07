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

%% @doc This module encapsulates the data related to the client's
%% subscriptions. It tries to reppresent the subscriptions as if they
%% were a simple key-value map.
%%
%% In reality, however, the session has to retain old the
%% subscriptions for longer to ensure the consistency of message
%% replay.
-module(emqx_persistent_session_ds_subs).

%% API:
-export([on_subscribe/3, on_unsubscribe/3, gc/1, lookup/2, to_map/1, fold/3, fold_all/3]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

%% @doc Process a new subscription
-spec on_subscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds:subscription(),
    emqx_persistent_session_ds_state:t()
) ->
    emqx_persistent_session_ds_state:t().
on_subscribe(TopicFilter, Subscription, S) ->
    emqx_persistent_session_ds_state:put_subscription(TopicFilter, [], Subscription, S).

%% @doc Process UNSUBSCRIBE
-spec on_unsubscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds:subscription(),
    emqx_persistent_session_ds_state:t()
) ->
    emqx_persistent_session_ds_state:t().
on_unsubscribe(TopicFilter, Subscription0, S0) ->
    %% Note: we cannot delete the subscription immediately, since its
    %% metadata can be used during replay (see `process_batch'). We
    %% instead mark it as deleted, and let `subscription_gc' function
    %% dispatch it later:
    Subscription = Subscription0#{deleted => true},
    emqx_persistent_session_ds_state:put_subscription(TopicFilter, [], Subscription, S0).

%% @doc Remove subscriptions that have been marked for deletion, and
%% that don't have any unacked messages:
-spec gc(emqx_persistent_session_ds_state:t()) -> emqx_persistent_session_ds_state:t().
gc(S0) ->
    fold_all(
        fun(TopicFilter, #{id := SubId, deleted := Deleted}, Acc) ->
            case Deleted andalso has_no_unacked_streams(SubId, S0) of
                true ->
                    emqx_persistent_session_ds_state:del_subscription(TopicFilter, [], Acc);
                false ->
                    Acc
            end
        end,
        S0,
        S0
    ).

%% @doc Fold over active subscriptions:
-spec lookup(emqx_persistent_session_ds:topic_filter(), emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds:subscription() | undefined.
lookup(TopicFilter, S) ->
    Subs = emqx_persistent_session_ds_state:get_subscriptions(S),
    case emqx_topic_gbt:lookup(TopicFilter, [], Subs, undefined) of
        #{deleted := true} ->
            undefined;
        Sub ->
            Sub
    end.

%% @doc Convert active subscriptions to a map, for information
%% purpose:
-spec to_map(emqx_persistent_session_ds_state:t()) -> map().
to_map(S) ->
    fold(
        fun(TopicFilter, #{props := Props}, Acc) -> Acc#{TopicFilter => Props} end,
        #{},
        S
    ).

%% @doc Fold over active subscriptions:
-spec fold(
    fun((emqx_types:topic(), emqx_persistent_session_ds:subscription(), Acc) -> Acc),
    Acc,
    emqx_persistent_session_ds_state:t()
) ->
    Acc.
fold(Fun, AccIn, S) ->
    fold_all(
        fun(TopicFilter, Sub = #{deleted := Deleted}, Acc) ->
            case Deleted of
                true -> Acc;
                false -> Fun(TopicFilter, Sub, Acc)
            end
        end,
        AccIn,
        S
    ).

%% @doc Fold over all subscriptions, including inactive ones:
-spec fold_all(
    fun((emqx_types:topic(), emqx_persistent_session_ds:subscription(), Acc) -> Acc),
    Acc,
    emqx_persistent_session_ds_state:t()
) ->
    Acc.
fold_all(Fun, AccIn, S) ->
    Subs = emqx_persistent_session_ds_state:get_subscriptions(S),
    emqx_topic_gbt:fold(
        fun(Key, Sub, Acc) -> Fun(emqx_topic_gbt:get_topic(Key), Sub, Acc) end,
        AccIn,
        Subs
    ).

%%================================================================================
%% Internal functions
%%================================================================================

-spec has_no_unacked_streams(
    emqx_persistent_session_ds:subscription_id(), emqx_persistent_session_ds_state:t()
) -> boolean().
has_no_unacked_streams(SubId, S) ->
    emqx_persistent_session_ds_state:fold_streams(
        fun
            ({SID, _Stream}, Srs, Acc) when SID =:= SubId ->
                emqx_persistent_session_ds_stream_scheduler:is_fully_acked(Srs, S) andalso Acc;
            (_StreamKey, _Srs, Acc) ->
                Acc
        end,
        true,
        S
    ).
