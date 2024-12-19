%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module aggregates shared subscription handlers (ssubscribers)
%% for a session.

-module(emqx_ds_shared_sub_agent).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_config.hrl").

-export([
    new/1,
    open/2,
    pre_subscribe/3,
    has_subscription/2,
    has_subscriptions/1,

    on_subscribe/4,
    on_unsubscribe/2,
    on_stream_progress/2,
    on_info/3,
    on_disconnect/2
]).

-export([
    send_to_ssubscriber/2
]).

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

-type subscription() :: emqx_persistent_session_ds_shared_subs_agent:subscription().
-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type subscription_id() :: emqx_persistent_session_ds_shared_subs_agent:subscription_id().

-type options() :: #{
    session_id := emqx_persistent_session_ds:id()
}.

-record(ssubscriber_entry, {
    ssubscriber_id :: emqx_ds_shared_sub_proto:ssubscriber_id(),
    topic_filter :: share_topic_filter(),
    ssubscriber :: emqx_ds_shared_sub_subscriber:t()
}).

-type ssubscriber_entry() :: #ssubscriber_entry{}.

-type t() :: #{
    ssubscribers := #{
        subscription_id() => ssubscriber_entry()
    },
    session_id := emqx_persistent_session_ds:id()
}.

-record(message_to_ssubscriber, {
    ssubscriber_id :: emqx_ds_shared_sub_proto:ssubscriber_id(),
    message :: term()
}).

-export_type([
    t/0,
    options/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(options()) -> t().
new(Opts) ->
    init_state(Opts).

-spec open([{share_topic_filter(), subscription()}], options()) -> t().
open(TopicSubscriptions, Opts) ->
    State0 = init_state(Opts),
    State1 = lists:foldl(
        fun({ShareTopicFilter, #{id := SubscriptionId}}, State) ->
            ?tp(debug, ds_shared_sub_agent_open, #{
                subscription_id => SubscriptionId,
                topic_filter => ShareTopicFilter
            }),
            add_ssubscriber(State, SubscriptionId, ShareTopicFilter)
        end,
        State0,
        TopicSubscriptions
    ),
    State1.

-spec pre_subscribe(t(), share_topic_filter(), emqx_types:subopts()) ->
    ok | {error, emqx_types:reason_code()}.
pre_subscribe(_State, #share{group = Group, topic = Topic}, _SubOpts) ->
    case ?dq_config(enable) of
        true ->
            %% TODO: Weird to have side effects in function with this name.
            TS = emqx_message:timestamp_now(),
            case emqx_ds_shared_sub_queue:declare(Group, Topic, TS, _StartTime = TS) of
                {ok, _} ->
                    ok;
                exists ->
                    ok;
                {error, Class, Reason} ->
                    ?tp(debug, ds_shared_sub_agent_queue_declare_failed, #{
                        group => Group,
                        topic => Topic,
                        class => Class,
                        reason => Reason
                    }),
                    {error, ?RC_UNSPECIFIED_ERROR}
            end;
        false ->
            {error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}
    end.

-spec has_subscription(t(), subscription_id()) -> boolean().
has_subscription(#{ssubscribers := SSubscribers}, SubscriptionId) ->
    maps:is_key(SubscriptionId, SSubscribers).

-spec has_subscriptions(t()) -> boolean().
has_subscriptions(#{ssubscribers := SSubscribers}) ->
    maps:size(SSubscribers) > 0.

-spec on_subscribe(t(), subscription_id(), share_topic_filter(), emqx_types:subopts()) -> t().
on_subscribe(State0, SubscriptionId, ShareTopicFilter, _SubOpts) ->
    ?tp(debug, ds_shared_sub_agent_on_subscribe, #{
        share_topic_filter => ShareTopicFilter
    }),
    add_ssubscriber(State0, SubscriptionId, ShareTopicFilter).

-spec on_unsubscribe(t(), subscription_id()) -> t().
on_unsubscribe(State0, SubscriptionId) ->
    {[], State} = with_ssubscriber(State0, SubscriptionId, fun(_SSubscriberId, SSubscriber) ->
        emqx_ds_shared_sub_subscriber:on_unsubscribe(SSubscriber)
    end),
    State.

-spec on_stream_progress(t(), #{
    subscription_id() => [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]
}) -> t().
on_stream_progress(State, StreamProgresses) when map_size(StreamProgresses) == 0 ->
    State;
on_stream_progress(State, StreamProgresses) ->
    maps:fold(
        fun(SubscriptionId, Progresses, StateAcc0) ->
            {[], StateAcc1} = with_ssubscriber(StateAcc0, SubscriptionId, fun(
                _SSubscriberId, SSubscriber
            ) ->
                emqx_ds_shared_sub_subscriber:on_stream_progress(SSubscriber, Progresses)
            end),
            StateAcc1
        end,
        State,
        StreamProgresses
    ).

-spec on_disconnect(t(), #{
    subscription_id() => [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]
}) -> t().
on_disconnect(#{ssubscribers := SSubscribers} = State, StreamProgresses) ->
    ok = lists:foreach(
        fun(SubscriptionId) ->
            Progress = maps:get(SubscriptionId, StreamProgresses, []),
            disconnect_ssubscriber(State, SubscriptionId, Progress)
        end,
        maps:keys(SSubscribers)
    ),
    State#{ssubscribers => #{}}.

-spec on_info(t(), subscription_id(), term()) ->
    {[emqx_persistent_session_ds_shared_subs_agent:event()], t()}.
on_info(State, SubscriptionId, #message_to_ssubscriber{
    ssubscriber_id = SSubscriberId, message = Message
}) ->
    ?tp(debug, ds_shared_sub_message_to_ssubscriber, #{
        subscription_id => SubscriptionId,
        message => Message
    }),
    with_ssubscriber(State, SubscriptionId, fun(KnownSSubscriberId, SSubscriber) ->
        %% We may have recreated invalidated SSubscriber, resulting in a new SSubscriberId.
        %% Ignore the messages to the old SSubscriber.
        case KnownSSubscriberId of
            SSubscriberId ->
                emqx_ds_shared_sub_subscriber:on_info(SSubscriber, Message);
            _ ->
                {ok, [], SSubscriber}
        end
    end).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_state(Opts) ->
    SessionId = maps:get(session_id, Opts),
    #{
        session_id => SessionId,
        ssubscribers => #{}
    }.

disconnect_ssubscriber(State, SubscriptionId, Progress) ->
    case State of
        #{
            ssubscribers := #{
                SubscriptionId := #ssubscriber_entry{
                    ssubscriber = SSubscriber, ssubscriber_id = SSubscriberId
                }
            } = SSubscribers
        } ->
            ok = destroy_ssubscriber_id(SSubscriberId),
            %% The whole session is shutting down, no need to handle the result.
            _ = emqx_ds_shared_sub_subscriber:on_disconnect(SSubscriber, Progress),
            State#{ssubscribers => maps:remove(SubscriptionId, SSubscribers)};
        _ ->
            State
    end.

add_ssubscriber(
    #{session_id := SessionId, ssubscribers := SSubscribers0} = State0,
    SubscriptionId,
    ShareTopicFilter
) ->
    ?tp(debug, ds_shared_sub_agent_add_ssubscriber, #{
        share_topic_filter => ShareTopicFilter
    }),
    SSubscriberId = make_ssubscriber_id(SessionId, SubscriptionId),
    SSubscriber = emqx_ds_shared_sub_subscriber:new(#{
        session_id => SessionId,
        share_topic_filter => ShareTopicFilter,
        id => SSubscriberId,
        send_after => send_to_ssubscriber_after(SSubscriberId)
    }),
    SSubscriberEntry = #ssubscriber_entry{
        ssubscriber_id = SSubscriberId,
        topic_filter = ShareTopicFilter,
        ssubscriber = SSubscriber
    },
    SSubscribers1 = SSubscribers0#{
        SubscriptionId => SSubscriberEntry
    },
    State1 = State0#{ssubscribers => SSubscribers1},
    State1.

make_ssubscriber_id(Id, SubscriptionId) ->
    emqx_ds_shared_sub_proto:ssubscriber_id(Id, SubscriptionId, alias()).

destroy_ssubscriber_id(SSubscriberId) ->
    Alias = emqx_ds_shared_sub_proto:ssubscriber_pidref(SSubscriberId),
    _ = unalias(Alias),
    ok.

send_to_ssubscriber_after(SSubscriberId) ->
    SubscriptionId = emqx_ds_shared_sub_proto:ssubscriber_subscription_id(SSubscriberId),
    fun(Time, Msg) ->
        emqx_persistent_session_ds_shared_subs_agent:send_after(
            Time,
            SubscriptionId,
            self(),
            #message_to_ssubscriber{
                ssubscriber_id = SSubscriberId,
                message = Msg
            }
        )
    end.

send_to_ssubscriber(SSubscriberId, Msg) ->
    SubscriptionId = emqx_ds_shared_sub_proto:ssubscriber_subscription_id(SSubscriberId),
    emqx_persistent_session_ds_shared_subs_agent:send(
        emqx_ds_shared_sub_proto:ssubscriber_pidref(SSubscriberId),
        SubscriptionId,
        #message_to_ssubscriber{
            ssubscriber_id = SSubscriberId,
            message = Msg
        }
    ).

with_ssubscriber(State0, SubscriptionId, Fun) ->
    case State0 of
        #{
            ssubscribers := #{
                SubscriptionId := #ssubscriber_entry{
                    topic_filter = ShareTopicFilter,
                    ssubscriber = SSubscriber0,
                    ssubscriber_id = SSubscriberId
                } = Entry0
            } = SSubscribers
        } ->
            {Events0, State1} =
                case Fun(SSubscriberId, SSubscriber0) of
                    {ok, Events, SSubscriber1} ->
                        Entry1 = Entry0#ssubscriber_entry{
                            ssubscriber = SSubscriber1
                        },
                        {Events, State0#{ssubscribers => SSubscribers#{SubscriptionId => Entry1}}};
                    {stop, Events} ->
                        ok = destroy_ssubscriber_id(SSubscriberId),
                        {Events, State0#{ssubscribers => maps:remove(SubscriptionId, SSubscribers)}};
                    {reset, Events} ->
                        ok = destroy_ssubscriber_id(SSubscriberId),
                        {Events, add_ssubscriber(State0, SubscriptionId, ShareTopicFilter)}
                end,
            Events1 = enrich_events(Events0, SubscriptionId, ShareTopicFilter),
            {Events1, State1};
        #{session_id := SessionId} ->
            ?tp(warning, ds_shared_sub_agent_ssubscriber_not_found, #{
                session_id => SessionId,
                subscription_id => SubscriptionId
            }),
            {[], State0}
    end.

enrich_events(Events, SubscriptionId, ShareTopicFilter) ->
    [
        Event#{subscription_id => SubscriptionId, share_topic_filter => ShareTopicFilter}
     || Event <- Events
    ].
