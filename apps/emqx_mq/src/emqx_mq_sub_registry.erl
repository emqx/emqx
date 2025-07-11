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
-define(SUB_PD_KEY(SUBSCRIBER_ID), {mq, SUBSCRIBER_ID}).

-record(subscription, {
    key :: {emqx_mq_types:channel_pid(), emqx_mq_types:subscriber_id() | '_'},
    consumer_pid :: emqx_mq_types:consumer_pid() | '_'
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
register_sub(#{subscriber_id := SubscriberId, consumer_pid := ConsumerPid} = Sub0) ->
    ChannelPid = self(),
    SubRec = #subscription{key = {ChannelPid, SubscriberId}, consumer_pid = ConsumerPid},
    true = ets:insert(?TAB, SubRec),
    Sub = maps:without([subscriber_id], Sub0),
    ?tp(warning, mq_sub_registry_register_sub, #{ets_key => {ChannelPid, SubscriberId}, sub => Sub}),
    _ = erlang:put(?SUB_PD_KEY(SubscriberId), Sub),
    ok.

-spec delete_sub(emqx_mq_types:subscriber_id()) -> emqx_mq_sub:sub() | undefined.
delete_sub(SubscriberId) ->
    ChannelPid = self(),
    Key = {ChannelPid, SubscriberId},
    case ets:lookup(?TAB, Key) of
        [] ->
            undefined;
        [#subscription{}] ->
            true = ets:delete(?TAB, Key),
            case erlang:erase(?SUB_PD_KEY(SubscriberId)) of
                undefined ->
                    undefined;
                Sub ->
                    Sub#{consumer_pid => SubscriberId}
            end
    end.

-spec get_sub(emqx_mq_types:subscriber_id()) -> emqx_mq_sub:t() | undefined.
get_sub(SubscriberId) ->
    case erlang:get(?SUB_PD_KEY(SubscriberId)) of
        undefined ->
            undefined;
        Sub ->
            Sub#{subscriber_id => SubscriberId}
    end.

-spec put_sub(emqx_mq_types:subscriber_id(), emqx_mq_sub:t()) -> ok.
put_sub(SubscriberId, Sub) ->
    _ = erlang:put(?SUB_PD_KEY(SubscriberId), maps:without([subscriber_id], Sub)),
    ok.

-spec cleanup_subs(emqx_mq_types:channel_pid()) ->
    [{emqx_mq_types:subscriber_id(), emqx_mq_types:consumer_pid()}].
cleanup_subs(ChannelPid) ->
    SubscriptionRecs = ets:match(?TAB, #subscription{
        key = {ChannelPid, '$1'}, consumer_pid = '$2', _ = '_'
    }),
    Subs = lists:map(
        fun([SubscriberId, ConsumerPid]) ->
            {SubscriberId, ConsumerPid}
        end,
        SubscriptionRecs
    ),
    true = ets:match_delete(?TAB, #subscription{key = {ChannelPid, '_'}, _ = '_'}),
    Subs.
