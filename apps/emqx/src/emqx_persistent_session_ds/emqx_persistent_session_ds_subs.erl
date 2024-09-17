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
-export([
    on_subscribe/3,
    on_unsubscribe/2,
    on_session_replay/2,
    on_session_drop/2,
    gc/1,
    lookup/2,
    fold/3,
    fold/4,
    to_map/1
]).

%% Management API:
-export([
    cold_get_subscription/2
]).

-export_type([subscription_state_id/0, subscription/0, subscription_state/0]).

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
    %% Mode of the subscription:
    %% * `{persistent, any}`:
    %%    Messages are sourced from DS,
    %% * `{persistent, noqos0}`:
    %%    QoS 1/2 messages are sourced from DS, QoS 0 messages are expected to
    %%    arrive through the realtime `emqx_broker` channel.
    %% * `realtime`:
    %%    All messages are expected to arrive through the realtime channel.
    mode => subscription_mode(),
    %% Reference to the current subscription state:
    current_state := subscription_state_id(),
    %% Time when the subscription was added:
    start_time := emqx_ds:time()
}.

-type subscription_mode() :: {persistent, any | noqos0} | realtime.

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
    }
}.

%%================================================================================
%% API functions
%%================================================================================

%% @doc Process a new subscription
-spec on_subscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) ->
    {ok, emqx_persistent_session_ds_state:t()} | {error, ?RC_QUOTA_EXCEEDED}.
on_subscribe(TopicFilter, SubOpts, #{id := SessionId, s := S0, props := Props}) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    NSubscriptions = emqx_persistent_session_ds_state:n_subscriptions(S0),
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        %% This is a new subscription:
        undefined when NSubscriptions < MaxSubscriptions ->
            S = create_subscription(SessionId, TopicFilter, SubOpts, Props, S0),
            {ok, S};
        undefined when NSubscriptions >= MaxSubscriptions ->
            {error, ?RC_QUOTA_EXCEEDED};
        %% This is existing subscription:
        Sub = #{} ->
            Mode = get_subscription_mode(Sub),
            case subscription_mode(TopicFilter, SubOpts, Props) of
                Mode ->
                    %% ...And the mode hasn't changed, update it:
                    S = update_subscription(TopicFilter, SubOpts, Sub, Props, S0);
                _Different ->
                    %% ...But the mode is different, recreate it:
                    S1 = delete_subscription(SessionId, TopicFilter, Sub, S0),
                    S = create_subscription(SessionId, TopicFilter, SubOpts, Props, S1)
            end,
            {ok, S}
    end.

create_subscription(SessionId, TopicFilter, SubOpts, Props, S0) ->
    Mode = subscription_mode(TopicFilter, SubOpts, Props),
    {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
    {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
    %% TODO: Unnecessary for realtime mode subscriptions.
    SState = #{
        parent_subscription => SubId,
        upgrade_qos => maps:get(upgrade_qos, Props),
        subopts => SubOpts
    },
    Subscription = #{
        id => SubId,
        mode => Mode,
        current_state => SStateId,
        start_time => now_ms()
    },
    ok = add_route(Mode, SessionId, TopicFilter, SubOpts),
    S3 = emqx_persistent_session_ds_state:put_subscription_state(SStateId, SState, S2),
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Subscription, S3),
    %% TODO? S = emqx_persistent_session_ds_stream_scheduler:on_subscribe(SubId, S1),
    ?tp(persistent_session_ds_subscription_added, #{
        session => SessionId,
        topic_filter => TopicFilter
    }),
    S.

update_subscription(
    TopicFilter,
    SubOpts,
    Sub0 = #{id := SubId, current_state := SStateId0},
    _Props = #{upgrade_qos := UpgradeQoS},
    S0
) ->
    SState = #{parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts},
    case emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0) of
        SState ->
            %% Client resubscribed with the same parameters:
            S0;
        _ ->
            %% Subsription parameters changed:
            {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            S2 = emqx_persistent_session_ds_state:put_subscription_state(SStateId, SState, S1),
            Sub = Sub0#{current_state => SStateId},
            emqx_persistent_session_ds_state:put_subscription(TopicFilter, Sub, S2)
    end.

%% @doc Process UNSUBSCRIBE
-spec on_unsubscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds:session()
) ->
    {ok, emqx_persistent_session_ds_state:t(), emqx_persistent_session_ds:subscription()}
    | {error, ?RC_NO_SUBSCRIPTION_EXISTED}.
on_unsubscribe(TopicFilter, #{id := SessionId, s := S0}) ->
    case lookup(TopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        Subscription ->
            S = delete_subscription(SessionId, TopicFilter, Subscription, S0),
            {ok, S, Subscription}
    end.

delete_subscription(SessionId, TopicFilter, Subscription, S0) ->
    ?tp(persistent_session_ds_subscription_delete, #{
        session => SessionId,
        topic_filter => TopicFilter
    }),
    Mode = get_subscription_mode(Subscription),
    ok = delete_route(Mode, SessionId, TopicFilter),
    emqx_persistent_session_ds_state:del_subscription(TopicFilter, S0).

-spec on_session_replay(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) ->
    ok.
on_session_replay(SessionId, S) ->
    fold(
        fun(TopicFilter, Subscription, _) ->
            restore_subscription(SessionId, TopicFilter, Subscription, S)
        end,
        ok,
        S,
        [regular]
    ).

restore_subscription(SessionId, TopicFilter, #{mode := Mode, current_state := SStateId}, S) ->
    #{subopts := SubOpts} = emqx_persistent_session_ds_state:get_subscription_state(SStateId, S),
    restore_route(Mode, SessionId, TopicFilter, SubOpts).

-spec on_session_drop(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) -> ok.
on_session_drop(SessionId, S0) ->
    _ = fold(
        fun(TopicFilter, Subscription, S) ->
            delete_subscription(SessionId, TopicFilter, Subscription, S)
        end,
        S0,
        S0,
        [regular]
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
        fun(_StreamId, SRS = #srs{sub_state_id = SStateId}, Acc) ->
            case emqx_persistent_session_ds_stream_scheduler:is_fully_acked(SRS, S0) of
                false ->
                    Acc#{SStateId => true};
                true ->
                    Acc
            end
        end,
        AliveSet0,
        S0
    ),
    %% Delete dangling subscription states:
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
                    set_default_subscription_mode(Sub#{subopts => SubOpts});
                undefined ->
                    undefined
            end;
        undefined ->
            undefined
    end.

fold(Fun, Acc, S) ->
    fold(Fun, Acc, S, []).

fold(Fun, Acc, S, Opts) ->
    OptRegular = lists:member(regular, Opts),
    OptPersistent = lists:member(persistent, Opts),
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (#share{}, _Sub, Acc0) when OptRegular ->
                Acc0;
            (_TopicFilter, #{mode := realtime}, Acc0) when OptPersistent ->
                Acc0;
            (TopicFilter, Sub, Acc0) ->
                Fun(TopicFilter, Sub, Acc0)
        end,
        Acc,
        S
    ).

%% @doc Convert active subscriptions to a map, for information
%% purpose:
-spec to_map(emqx_persistent_session_ds_state:t()) -> map().
to_map(S) ->
    fold(
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

subscription_mode(_Topic, #{qos := _QoS}, #{upgrade_qos_any := true}) ->
    %% NOTE
    %% We can't use `noqos0` routing if _any_ durable session could potentially
    %% have `upgrade_qos` enabled, even not yet existing session. That's because
    %% any future session that starts with `upgrade_qos` enabled will have to
    %% update the master routing table, which then will cause QoS0 messages to end
    %% up in DS, which in turn will lead to message duplication in _existing_
    %% `noqos0` subscriptions.
    {persistent, any};
subscription_mode(_Topic, #{qos := ?QOS_0}, #{}) ->
    realtime;
subscription_mode(_Topic, #{qos := _QoS}, #{}) ->
    {persistent, noqos0}.

get_subscription_mode(Sub) ->
    maps:get(mode, Sub, {persistent, any}).

set_default_subscription_mode(Sub = #{mode := _}) ->
    Sub;
set_default_subscription_mode(Sub) ->
    Sub#{mode => {persistent, any}}.

add_route({persistent, any}, SessionId, Topic, _SubOpts) ->
    add_persistent_route(Topic, SessionId, root);
add_route({persistent, noqos0}, SessionId, Topic, SubOpts) ->
    ok = add_realtime_route(SessionId, Topic, SubOpts, qos0),
    add_persistent_route(Topic, SessionId, noqos0);
add_route(realtime, SessionId, Topic, SubOpts) ->
    add_realtime_route(SessionId, Topic, SubOpts, root).

restore_route({persistent, any}, _SessionId, _Topic, _SubOpts) ->
    ok;
restore_route({persistent, noqos0}, SessionId, Topic, SubOpts) ->
    add_realtime_route(SessionId, Topic, SubOpts, qos0);
restore_route(realtime, SessionId, Topic, SubOpts) ->
    add_realtime_route(SessionId, Topic, SubOpts, root).

delete_route({persistent, any}, SessionId, Topic) ->
    delete_persistent_route(Topic, SessionId, root);
delete_route({persistent, noqos0}, SessionId, Topic) ->
    ok = delete_realtime_route(SessionId, Topic),
    delete_persistent_route(Topic, SessionId, noqos0);
delete_route(realtime, SessionId, Topic) ->
    delete_realtime_route(SessionId, Topic).

add_persistent_route(Topic, SessionId, Scope) ->
    ok = emqx_persistent_session_ds_router:add_route(Topic, SessionId, Scope),
    _ = emqx_external_broker:add_persistent_route(Topic, SessionId),
    ok.

delete_persistent_route(Topic, SessionId, Scope) ->
    ok = emqx_persistent_session_ds_router:delete_route(Topic, SessionId, Scope),
    _ = emqx_external_broker:delete_persistent_route(Topic, SessionId),
    ok.

add_realtime_route(SessionId, Topic, SubOpts, Scope) ->
    emqx_broker:subscribe(Topic, SessionId, SubOpts, Scope).

delete_realtime_route(_SessionId, Topic) ->
    emqx_broker:unsubscribe(Topic).

now_ms() ->
    erlang:system_time(millisecond).
