%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub_registry).

-moduledoc """
The module registers subscriptions from channels to the Message Queue consumers.
""".

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    create_tab/0,
    register_sub/1,
    delete_sub/1,
    get_sub/1,
    put_sub/2,
    cleanup_subs/1
]).

-define(TAB, ?MODULE).
-define(SUB_PD_KEY(SUBSCRIBER_REF), {mq, SUBSCRIBER_REF}).
-define(TOPIC_PD_KEY(TOPIC), {mqt, TOPIC}).

-record(subscription, {
    key :: {emqx_mq_types:channel_pid(), emqx_mq_types:subscriber_ref() | '_'},
    consumer_ref :: emqx_mq_types:consumer_ref() | '_'
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create_tab() -> ok.
create_tab() ->
    ok = emqx_utils_ets:new(?TAB, [
        ordered_set,
        public,
        {read_concurrency, true},
        {write_concurrency, true},
        {keypos, #subscription.key}
    ]).

-spec register_sub(emqx_mq_sub:t()) -> ok.
register_sub(
    #{subscriber_ref := SubscriberRef, consumer_ref := ConsumerRef, topic := Topic} = Sub0
) ->
    ChannelPid = self(),
    SubRec = #subscription{key = {ChannelPid, SubscriberRef}, consumer_ref = ConsumerRef},
    true = ets:insert(?TAB, SubRec),
    Sub = maps:without([subscriber_ref], Sub0),
    ?tp(warning, mq_sub_registry_register_sub, #{ets_key => {ChannelPid, SubscriberRef}, sub => Sub}),
    _ = erlang:put(?SUB_PD_KEY(SubscriberRef), Sub),
    _ = erlang:put(?TOPIC_PD_KEY(Topic), SubscriberRef),
    ok.

-spec delete_sub(emqx_mq_types:subscriber_ref() | emqx_types:topic()) ->
    emqx_mq_sub:sub() | undefined.
delete_sub(SubscriberRef) when is_reference(SubscriberRef) ->
    ChannelPid = self(),
    Key = {ChannelPid, SubscriberRef},
    case ets:lookup(?TAB, Key) of
        [] ->
            undefined;
        [#subscription{}] ->
            true = ets:delete(?TAB, Key),
            case erlang:erase(?SUB_PD_KEY(SubscriberRef)) of
                undefined ->
                    undefined;
                #{topic := Topic} = Sub ->
                    _ = erlang:erase(?TOPIC_PD_KEY(Topic)),
                    Sub
            end
    end;
delete_sub(Topic) when is_binary(Topic) ->
    case erlang:get(?TOPIC_PD_KEY(Topic)) of
        undefined ->
            undefined;
        SubscriberRef ->
            delete_sub(SubscriberRef)
    end.

-spec get_sub(emqx_mq_types:subscriber_ref()) -> emqx_mq_sub:t() | undefined.
get_sub(SubscriberRef) ->
    case erlang:get(?SUB_PD_KEY(SubscriberRef)) of
        undefined ->
            undefined;
        Sub ->
            Sub#{subscriber_ref => SubscriberRef}
    end.

-spec put_sub(emqx_mq_types:subscriber_ref(), emqx_mq_sub:t()) -> ok.
put_sub(SubscriberRef, Sub) ->
    _ = erlang:put(?SUB_PD_KEY(SubscriberRef), maps:without([subscriber_ref], Sub)),
    ok.

-spec cleanup_subs(emqx_mq_types:channel_pid()) ->
    [{emqx_mq_types:subscriber_ref(), emqx_mq_types:consumer_ref()}].
cleanup_subs(ChannelPid) ->
    SubscriptionRecs = ets:match(?TAB, #subscription{
        key = {ChannelPid, '$1'}, consumer_ref = '$2', _ = '_'
    }),
    Subs = lists:map(
        fun([SubscriberRef, ConsumerRef]) ->
            {SubscriberRef, ConsumerRef}
        end,
        SubscriptionRecs
    ),
    true = ets:match_delete(?TAB, #subscription{key = {ChannelPid, '_'}, _ = '_'}),
    Subs.
