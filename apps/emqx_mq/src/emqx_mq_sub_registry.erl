%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub_registry).

-moduledoc """
The module is a small wrapper around the process dictionary to store subscriptions
of channels to the Message Queue consumers.
""".

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
register(#{subscriber_ref := SubscriberRef, topic := Topic} = Sub) ->
    ?tp(warning, mq_sub_registry_register, #{
        subscriber_ref => SubscriberRef, topic => Topic, sub => Sub
    }),
    _ = erlang:put(?SUB_PD_KEY(SubscriberRef), maps:without([subscriber_ref], Sub)),
    _ = erlang:put(?TOPIC_PD_KEY(Topic), SubscriberRef),
    ok.

-spec delete(emqx_mq_types:subscriber_ref() | emqx_types:topic()) ->
    emqx_mq_sub:sub() | undefined.
delete(SubscriberRef) when is_reference(SubscriberRef) ->
    case erlang:erase(?SUB_PD_KEY(SubscriberRef)) of
        undefined ->
            undefined;
        #{topic := Topic} = Sub ->
            _ = erlang:erase(?TOPIC_PD_KEY(Topic)),
            Sub#{subscriber_ref => SubscriberRef}
    end;
delete(Topic) when is_binary(Topic) ->
    case erlang:get(?TOPIC_PD_KEY(Topic)) of
        undefined ->
            undefined;
        SubscriberRef ->
            delete(SubscriberRef)
    end.

-spec find(emqx_mq_types:subscriber_ref()) -> emqx_mq_sub:t() | undefined.
find(SubscriberRef) ->
    case erlang:get(?SUB_PD_KEY(SubscriberRef)) of
        undefined ->
            undefined;
        Sub ->
            Sub#{subscriber_ref => SubscriberRef}
    end.

-spec update(emqx_mq_types:subscriber_ref(), emqx_mq_sub:t()) -> ok.
update(SubscriberRef, #{topic := Topic} = Sub) ->
    %%
    #{topic := Topic} = erlang:put(?SUB_PD_KEY(SubscriberRef), maps:without([subscriber_ref], Sub)),
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
