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

%% @doc This module encapsulates the data related to the client's
%% subscriptions. It tries to reppresent the subscriptions as if they
%% were a simple key-value map.
%%
%% In reality, however, the session has to retain old the
%% subscriptions for longer to ensure the consistency of message
%% replay.
-module(emqx_persistent_session_ds_subs).

%% API:
-export([
    open/1,
    fold_private_subscriptions/3,
    on_subscribe/3,
    on_unsubscribe/4,
    on_session_drop/2,
    gc/1,
    lookup/2,
    to_map/1
]).

%% Management API:
-export([
    cold_get_subscription/2
]).

-export_type([subscription_state_id/0, subscription/0, subscription_state/0, new_stream_subs/0]).

-include("session_internals.hrl").
-include("emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type subscription() :: #{
    %% Session-unique identifier of the subscription. Other objects
    %% can use it as a compact reference:
    id := emqx_persistent_session_ds:subscription_id(),
    %% Reference to the current subscription state:
    current_state := subscription_state_id(),
    %% Time when the subscription was added:
    start_time := emqx_ds:time()
}.

-type subscription_state_id() :: integer().

-type subscription_state() :: #{
    parent_subscription := emqx_persistent_session_ds:subscription_id(),
    upgrade_qos := boolean(),
    %% SubOpts:
    subopts := #{
        nl => _,
        qos => _,
        rap => _,
        subid => _,
        _ => _
    },
    %% Optional field that is added when subscription state becomes
    %% outdated:
    superseded_by => subscription_state_id()
}.

-type new_stream_subs() :: #{emqx_ds_new_streams:watch() => emqx_types:topic()}.

%%================================================================================
%% API functions
%%================================================================================

-spec open(emqx_persistent_session_ds_state:t()) -> new_stream_subs().
open(S) ->
    %% Receive notifications about new streams for the topic filter:
    fold_private_subscriptions(
        fun(TopicFilterBin, _Sub, Acc) ->
            Acc#{watch_streams(TopicFilterBin) => TopicFilterBin}
        end,
        #{},
        S
    ).

%% @doc Fold over non-shared subscriptions:
fold_private_subscriptions(Fun, Acc, S) ->
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (#share{}, _Sub, Acc0) -> Acc0;
            (TopicFilterBin, Sub, Acc0) -> Fun(TopicFilterBin, Sub, Acc0)
        end,
        Acc,
        S
    ).

%% @doc Process a new subscription
-spec on_subscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) ->
    {ok, emqx_persistent_session_ds_state:t(), new_stream_subs()}
    | {error, ?RC_QUOTA_EXCEEDED}.
on_subscribe(TopicFilter, SubOpts, #{
    id := SessionId, s := S0, props := Props, new_stream_subs := NewStreamSubs
}) ->
    #{upgrade_qos := UpgradeQoS, max_subscriptions := MaxSubscriptions} = Props,
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined ->
            %% This is a new subscription:
            case emqx_persistent_session_ds_state:n_subscriptions(S0) < MaxSubscriptions of
                true ->
                    ok = emqx_persistent_session_ds_router:do_add_route(TopicFilter, SessionId),
                    _ = emqx_external_broker:add_persistent_route(TopicFilter, SessionId),
                    {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
                    {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
                    SState = #{
                        parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts
                    },
                    S3 = emqx_persistent_session_ds_state:put_subscription_state(
                        SStateId, SState, S2
                    ),
                    Subscription = #{
                        id => SubId,
                        current_state => SStateId,
                        start_time => now_ms()
                    },
                    S = emqx_persistent_session_ds_state:put_subscription(
                        TopicFilter, Subscription, S3
                    ),
                    ?tp(persistent_session_ds_subscription_added, #{
                        topic_filter => TopicFilter, session => SessionId
                    }),
                    {ok, gc(S), NewStreamSubs#{watch_streams(TopicFilter) => TopicFilter}};
                false ->
                    {error, ?RC_QUOTA_EXCEEDED}
            end;
        Sub0 = #{current_state := SStateId0, id := SubId} ->
            SState = #{parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts},
            case emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0) of
                SState ->
                    %% Client resubscribed with the same parameters:
                    {ok, gc(S0), NewStreamSubs};
                OldSState ->
                    %% Subsription parameters changed:
                    {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
                    S2 = emqx_persistent_session_ds_state:put_subscription_state(
                        SStateId, SState, S1
                    ),
                    S3 = emqx_persistent_session_ds_state:put_subscription_state(
                        SStateId0, OldSState#{superseded_by => SStateId}, S2
                    ),
                    Sub = Sub0#{current_state := SStateId},
                    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Sub, S3),
                    {ok, gc(S), NewStreamSubs}
            end
    end.

%% @doc Process UNSUBSCRIBE
-spec on_unsubscribe(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_state:t(),
    new_stream_subs()
) ->
    {ok, emqx_persistent_session_ds_state:t(), new_stream_subs(),
        emqx_persistent_session_ds:subscription()}
    | {error, ?RC_NO_SUBSCRIPTION_EXISTED}.
on_unsubscribe(SessionId, TopicFilter, S0, NewStreamSubs) ->
    case lookup(TopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        Subscription ->
            ?tp(persistent_session_ds_subscription_delete, #{
                session_id => SessionId, topic_filter => TopicFilter
            }),
            ?tp_span(
                persistent_session_ds_subscription_route_delete,
                #{session_id => SessionId, topic_filter => TopicFilter},
                ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilter, SessionId)
            ),
            _ = emqx_external_broker:delete_persistent_route(TopicFilter, SessionId),
            S = emqx_persistent_session_ds_state:del_subscription(TopicFilter, S0),
            {ok, gc(S), unwatch_streams(TopicFilter, NewStreamSubs), Subscription}
    end.

-spec on_session_drop(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) -> ok.
on_session_drop(SessionId, S0) ->
    _ = fold_private_subscriptions(
        fun(TopicFilter, _Subscription, S) ->
            case on_unsubscribe(SessionId, TopicFilter, S, #{}) of
                {ok, S1, _, _} -> S1;
                _ -> S
            end
        end,
        S0,
        S0
    ),
    ok.

%% @doc Remove subscription states that don't have a parent, and that
%% don't have any unacked messages.
%% TODO
%% This function collects shared subs as well
%% Move to a separate module to keep symmetry?
-spec gc(emqx_persistent_session_ds_state:t()) -> emqx_persistent_session_ds_state:t().
gc(S0) ->
    %% Create a set of subscription states IDs referenced either by a
    %% subscription or a stream replay state:
    AliveSet0 = emqx_persistent_session_ds_state:fold_subscriptions(
        fun(_TopicFilter, #{current_state := SStateId}, Acc) ->
            Acc#{SStateId => true}
        end,
        #{},
        S0
    ),
    AliveSet = emqx_persistent_session_ds_state:fold_streams(
        fun(_StreamId, #srs{sub_state_id = SStateId}, Acc) ->
            Acc#{SStateId => true}
        end,
        AliveSet0,
        S0
    ),
    %% Delete subscription states that don't belong to the alive set:
    emqx_persistent_session_ds_state:fold_subscription_states(
        fun(SStateId, _, S) ->
            case maps:is_key(SStateId, AliveSet) of
                true ->
                    S;
                false ->
                    emqx_persistent_session_ds_state:del_subscription_state(SStateId, S)
            end
        end,
        S0,
        S0
    ).

%% @doc Lookup a subscription and merge it with its current state:
-spec lookup(emqx_persistent_session_ds:topic_filter(), emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds:subscription() | undefined.
lookup(TopicFilter, S) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S) of
        Sub = #{current_state := SStateId} ->
            case emqx_persistent_session_ds_state:get_subscription_state(SStateId, S) of
                #{subopts := SubOpts} ->
                    Sub#{subopts => SubOpts};
                undefined ->
                    undefined
            end;
        undefined ->
            undefined
    end.

%% @doc Convert active subscriptions to a map, for information
%% purpose:
-spec to_map(emqx_persistent_session_ds_state:t()) -> map().
to_map(S) ->
    fold_private_subscriptions(
        fun(TopicFilter, _, Acc) -> Acc#{TopicFilter => lookup(TopicFilter, S)} end,
        #{},
        S
    ).

-spec cold_get_subscription(emqx_persistent_session_ds:id(), emqx_types:topic()) ->
    emqx_persistent_session_ds:subscription() | undefined.
cold_get_subscription(SessionId, Topic) ->
    case emqx_persistent_session_ds_state:cold_get_subscription(SessionId, Topic) of
        [Sub = #{current_state := SStateId}] ->
            case
                emqx_persistent_session_ds_state:cold_get_subscription_state(SessionId, SStateId)
            of
                [#{subopts := Subopts}] ->
                    Sub#{subopts => Subopts};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

%%================================================================================
%% Internal functions
%%================================================================================

now_ms() ->
    erlang:system_time(millisecond).

watch_streams(TopicFilterBin) ->
    {ok, Ref} = emqx_ds_new_streams:watch(
        ?PERSISTENT_MESSAGE_DB, emqx_topic:words(TopicFilterBin)
    ),
    ?tp(debug, sessds_watch_streams, #{topic_filter => TopicFilterBin, ref => Ref}),
    Ref.

unwatch_streams(TopicFilter, NewStreamSubs) ->
    try
        maps:foreach(
            fun(Ref, TF) ->
                case TF of
                    TopicFilter -> throw({found, Ref});
                    _ -> ok
                end
            end,
            NewStreamSubs
        ),
        NewStreamSubs
    catch
        {found, Ref} ->
            ?tp(debug, sessds_unwatch_streams, #{topic_filter => TopicFilter, ref => Ref}),
            emqx_ds_new_streams:unwatch(?PERSISTENT_MESSAGE_DB, Ref),
            maps:remove(Ref, NewStreamSubs)
    end.
