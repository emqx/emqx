%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-type name_topic() :: {emqx_mq_types:mq_name(), emqx_mq_types:mq_topic() | undefined}.

-define(SUB_PD_KEY(SUBSCRIBER_REF), {mq, SUBSCRIBER_REF}).
-define(NAME_TOPIC_PD_KEY(TOPIC), {mqt, TOPIC}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec register(emqx_mq_sub:t()) -> ok.
register(Sub) ->
    SubscriberRef = emqx_mq_sub:subscriber_ref(Sub),
    NameTopic = emqx_mq_sub:name_topic(Sub),
    case erlang:get(?NAME_TOPIC_PD_KEY(NameTopic)) of
        undefined ->
            ok;
        _OldSubscriberRef ->
            %% This should never happen.
            error({mq_sub_registry_topic_conflict, NameTopic})
    end,
    ?tp_debug(mq_sub_registry_register, #{
        subscriber_ref => SubscriberRef, name_topic => NameTopic, sub => Sub
    }),
    _ = erlang:put(?SUB_PD_KEY(SubscriberRef), maps:without([subscriber_ref], Sub)),
    _ = erlang:put(?NAME_TOPIC_PD_KEY(NameTopic), SubscriberRef),
    ok.

-spec delete(emqx_mq_types:subscriber_ref() | name_topic()) ->
    emqx_mq_sub:t() | undefined.
delete(SubscriberRef) when is_reference(SubscriberRef) ->
    case erlang:erase(?SUB_PD_KEY(SubscriberRef)) of
        undefined ->
            undefined;
        Sub ->
            NameTopic = emqx_mq_sub:name_topic(Sub),
            _ = erlang:erase(?NAME_TOPIC_PD_KEY(NameTopic)),
            Sub#{subscriber_ref => SubscriberRef}
    end;
delete({Name, Topic} = NameTopic) when is_binary(Name) andalso (is_binary(Topic) orelse Topic =:= undefined) ->
    case erlang:get(?NAME_TOPIC_PD_KEY(NameTopic)) of
        undefined ->
            undefined;
        SubscriberRef ->
            delete(SubscriberRef)
    end.

-spec find(emqx_mq_types:subscriber_ref() | name_topic()) ->
    emqx_mq_sub:t() | undefined.
find(SubscriberRef) when is_reference(SubscriberRef) ->
    case erlang:get(?SUB_PD_KEY(SubscriberRef)) of
        undefined ->
            undefined;
        Sub ->
            Sub#{subscriber_ref => SubscriberRef}
    end;
find({Name, Topic} = NameTopic) when is_binary(Name) andalso (is_binary(Topic) orelse Topic =:= undefined) ->
    case erlang:get(?NAME_TOPIC_PD_KEY(NameTopic)) of
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
