%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_handler_registry).

-moduledoc """
Collection of handlers for the external message sources.
Primarily, allows to access the handlers both by the handler unique reference or by topic filter.
""".

-include("emqx_extsub_internal.hrl").

-export([
    new/0,
    register/4,
    delete/2,
    find/2,
    update/3,
    all/1,
    topic_filter/2,
    subscriber_ref/2
]).

-record(extsub, {
    topic_filter :: emqx_extsub_types:topic_filter(),
    handler :: emqx_extsub_handler:t()
}).

-record(registry, {
    handler_by_subref :: #{emqx_extsub_types:subscriber_ref() => #extsub{}},
    subref_by_topic :: #{emqx_extsub_types:topic_filter() => emqx_extsub_types:subscriber_ref()}
}).

-type t() :: #registry{}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> t().
new() ->
    #registry{handler_by_subref = #{}, subref_by_topic = #{}}.

-spec register(
    t(),
    emqx_extsub_types:topic_filter(),
    emqx_extsub_types:subscriber_ref(),
    emqx_extsub_handler:t()
) -> t().
register(
    #registry{handler_by_subref = BySubRef, subref_by_topic = ByTopic} = Registry,
    TopicFilter,
    SubscriberRef,
    Handler
) ->
    case ByTopic of
        #{TopicFilter := _} ->
            error({extsub_registry_topic_conflict, TopicFilter});
        _ ->
            Registry#registry{
                handler_by_subref = BySubRef#{
                    SubscriberRef => #extsub{
                        topic_filter = TopicFilter,
                        handler = Handler
                    }
                },
                subref_by_topic = ByTopic#{TopicFilter => SubscriberRef}
            }
    end.

-spec delete(t(), emqx_extsub_types:subscriber_ref() | emqx_types:topic()) -> t().
delete(
    #registry{handler_by_subref = BySubRef, subref_by_topic = ByTopic} = Registry, SubscriberRef
) when
    is_reference(SubscriberRef)
->
    case BySubRef of
        #{SubscriberRef := #extsub{topic_filter = TopicFilter}} ->
            Registry#registry{
                handler_by_subref = maps:remove(SubscriberRef, BySubRef),
                subref_by_topic = maps:remove(TopicFilter, ByTopic)
            };
        _ ->
            Registry
    end;
delete(#registry{subref_by_topic = ByTopic} = Registry, TopicFilter) when is_binary(TopicFilter) ->
    case ByTopic of
        #{TopicFilter := SubscriberRef} ->
            delete(Registry, SubscriberRef);
        _ ->
            Registry
    end.

-spec find(t(), emqx_extsub_types:subscriber_ref() | emqx_extsub_types:topic_filter()) ->
    emqx_extsub_handler:t() | undefined.
find(#registry{handler_by_subref = BySubRef}, SubscriberRef) when is_reference(SubscriberRef) ->
    case BySubRef of
        #{SubscriberRef := #extsub{handler = Handler}} ->
            Handler;
        _ ->
            undefined
    end;
find(#registry{subref_by_topic = ByTopic} = Registry, TopicFilter) when is_binary(TopicFilter) ->
    case ByTopic of
        #{TopicFilter := SubscriberRef} ->
            find(Registry, SubscriberRef);
        _ ->
            undefined
    end.

-spec update(t(), emqx_extsub_types:subscriber_ref(), emqx_extsub_handler:t()) -> t().
update(#registry{handler_by_subref = BySubRef} = Registry, SubscriberRef, Handler) ->
    case BySubRef of
        #{SubscriberRef := #extsub{topic_filter = TopicFilter}} ->
            Registry#registry{
                handler_by_subref = BySubRef#{
                    SubscriberRef => #extsub{
                        topic_filter = TopicFilter,
                        handler = Handler
                    }
                }
            };
        _ ->
            error({extsub_registry_subscriber_not_found, SubscriberRef})
    end.

-spec topic_filter(t(), emqx_extsub_types:subscriber_ref()) ->
    emqx_extsub_types:topic_filter() | undefined.
topic_filter(#registry{handler_by_subref = BySubRef}, SubscriberRef) when
    is_reference(SubscriberRef)
->
    case BySubRef of
        #{SubscriberRef := #extsub{topic_filter = TopicFilter}} ->
            TopicFilter;
        _ ->
            undefined
    end.

-spec subscriber_ref(t(), emqx_extsub_types:topic_filter()) ->
    emqx_extsub_types:subscriber_ref() | undefined.
subscriber_ref(#registry{subref_by_topic = ByTopic}, TopicFilter) when is_binary(TopicFilter) ->
    case ByTopic of
        #{TopicFilter := SubscriberRef} ->
            SubscriberRef;
        _ ->
            undefined
    end.

-spec all(t()) ->
    [
        {
            emqx_extsub_types:subscriber_ref(),
            emqx_extsub_types:topic_filter(),
            emqx_extsub_handler:t()
        }
    ].
all(#registry{handler_by_subref = BySubRef}) ->
    [
        {SubscriberRef, TopicFilter, Handler}
     || {SubscriberRef, #extsub{topic_filter = TopicFilter, handler = Handler}} <- maps:to_list(
            BySubRef
        )
    ].
