%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub_registry).

-moduledoc """
The module is a small wrapper around the process dictionary to store subscriptions
of channels to the Message Queue consumers.
""".

-include("emqx_mq_internal.hrl").

-export([
    register/1,
    delete/1,
    find/1,
    update/2,
    all/0
]).

-define(SUB_PD_KEY(SUBSCRIBER_REF), {mq, SUBSCRIBER_REF}).
-define(TOPIC_PD_KEY(TOPIC), {mqt, TOPIC}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec register(emqx_mq_sub:t()) -> ok.
register(Sub) ->
    SubscriberRef = emqx_mq_sub:subscriber_ref(Sub),
    MQTopicFilter = emqx_mq_sub:mq_topic_filter(Sub),
    ?tp_debug(mq_sub_registry_register, #{
        subscriber_ref => SubscriberRef, mq_topic_filter => MQTopicFilter, sub => Sub
    }),
    _ = erlang:put(?SUB_PD_KEY(SubscriberRef), maps:without([subscriber_ref], Sub)),
    _ = erlang:put(?TOPIC_PD_KEY(MQTopicFilter), SubscriberRef),
    ok.

-spec delete(emqx_mq_types:subscriber_ref() | emqx_types:topic()) ->
    emqx_mq_sub:t() | undefined.
delete(SubscriberRef) when is_reference(SubscriberRef) ->
    case erlang:erase(?SUB_PD_KEY(SubscriberRef)) of
        undefined ->
            undefined;
        Sub ->
            MQTopicFilter = emqx_mq_sub:mq_topic_filter(Sub),
            _ = erlang:erase(?TOPIC_PD_KEY(MQTopicFilter)),
            Sub#{subscriber_ref => SubscriberRef}
    end;
delete(MQTopicFilter) when is_binary(MQTopicFilter) ->
    case erlang:get(?TOPIC_PD_KEY(MQTopicFilter)) of
        undefined ->
            undefined;
        SubscriberRef ->
            delete(SubscriberRef)
    end.

-spec find(emqx_mq_types:subscriber_ref() | emqx_mq_types:mq_topic()) ->
    emqx_mq_sub:t() | undefined.
find(SubscriberRef) when is_reference(SubscriberRef) ->
    case erlang:get(?SUB_PD_KEY(SubscriberRef)) of
        undefined ->
            undefined;
        Sub ->
            Sub#{subscriber_ref => SubscriberRef}
    end;
find(MQTopicFilter) when is_binary(MQTopicFilter) ->
    case erlang:get(?TOPIC_PD_KEY(MQTopicFilter)) of
        undefined ->
            undefined;
        SubscriberRef ->
            find(SubscriberRef)
    end.

-spec update(emqx_mq_types:subscriber_ref(), emqx_mq_sub:t()) -> ok.
update(SubscriberRef, Sub) ->
    _ = erlang:put(?SUB_PD_KEY(SubscriberRef), maps:without([subscriber_ref], Sub)),
    ok.

-spec all() -> [emqx_mq_sub:t()].
all() ->
    lists:filtermap(
        fun
            ({?SUB_PD_KEY(SubscriberRef), Sub}) ->
                {true, Sub#{subscriber_ref => SubscriberRef}};
            (_) ->
                false
        end,
        erlang:get()
    ).
