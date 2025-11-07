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
    handler_ref/2
]).

-record(extsub, {
    topic_filter :: emqx_extsub_types:topic_filter(),
    handler :: emqx_extsub_handler:t()
}).

-record(registry, {
    by_ref :: #{emqx_extsub_types:handler_ref() => #extsub{}},
    ref_by_topic :: #{emqx_extsub_types:topic_filter() => emqx_extsub_types:handler_ref()}
}).

-type t() :: #registry{}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> t().
new() ->
    #registry{by_ref = #{}, ref_by_topic = #{}}.

-spec register(
    t(),
    emqx_extsub_types:topic_filter(),
    emqx_extsub_types:handler_ref(),
    emqx_extsub_handler:t()
) -> t().
register(
    #registry{by_ref = ByRef, ref_by_topic = ByTopic} = Registry,
    TopicFilter,
    HandlerRef,
    Handler
) ->
    case ByTopic of
        #{TopicFilter := _} ->
            error({extsub_registry_topic_conflict, TopicFilter});
        _ ->
            Registry#registry{
                by_ref = ByRef#{
                    HandlerRef => #extsub{
                        topic_filter = TopicFilter,
                        handler = Handler
                    }
                },
                ref_by_topic = ByTopic#{TopicFilter => HandlerRef}
            }
    end.

-spec delete(t(), emqx_extsub_types:handler_ref() | emqx_types:topic()) -> t().
delete(
    #registry{by_ref = ByRef, ref_by_topic = ByTopic} = Registry, HandlerRef
) when
    is_reference(HandlerRef)
->
    case ByRef of
        #{HandlerRef := #extsub{topic_filter = TopicFilter}} ->
            Registry#registry{
                by_ref = maps:remove(HandlerRef, ByRef),
                ref_by_topic = maps:remove(TopicFilter, ByTopic)
            };
        _ ->
            Registry
    end;
delete(#registry{ref_by_topic = ByTopic} = Registry, TopicFilter) when is_binary(TopicFilter) ->
    case ByTopic of
        #{TopicFilter := HandlerRef} ->
            delete(Registry, HandlerRef);
        _ ->
            Registry
    end.

-spec find(t(), emqx_extsub_types:handler_ref() | emqx_extsub_types:topic_filter()) ->
    emqx_extsub_handler:t() | undefined.
find(#registry{by_ref = ByRef}, HandlerRef) when is_reference(HandlerRef) ->
    case ByRef of
        #{HandlerRef := #extsub{handler = Handler}} ->
            Handler;
        _ ->
            undefined
    end;
find(#registry{ref_by_topic = ByTopic} = Registry, TopicFilter) when is_binary(TopicFilter) ->
    case ByTopic of
        #{TopicFilter := HandlerRef} ->
            find(Registry, HandlerRef);
        _ ->
            undefined
    end.

-spec update(t(), emqx_extsub_types:handler_ref(), emqx_extsub_handler:t()) -> t().
update(#registry{by_ref = ByRef} = Registry, HandlerRef, Handler) ->
    case ByRef of
        #{HandlerRef := #extsub{topic_filter = TopicFilter}} ->
            Registry#registry{
                by_ref = ByRef#{
                    HandlerRef => #extsub{
                        topic_filter = TopicFilter,
                        handler = Handler
                    }
                }
            };
        _ ->
            error({extsub_registry_handler_not_found, HandlerRef})
    end.

-spec topic_filter(t(), emqx_extsub_types:handler_ref()) ->
    emqx_extsub_types:topic_filter() | undefined.
topic_filter(#registry{by_ref = ByRef}, HandlerRef) when
    is_reference(HandlerRef)
->
    case ByRef of
        #{HandlerRef := #extsub{topic_filter = TopicFilter}} ->
            TopicFilter;
        _ ->
            undefined
    end.

-spec handler_ref(t(), emqx_extsub_types:topic_filter()) ->
    emqx_extsub_types:handler_ref() | undefined.
handler_ref(#registry{ref_by_topic = ByTopic}, TopicFilter) when is_binary(TopicFilter) ->
    case ByTopic of
        #{TopicFilter := HandlerRef} ->
            HandlerRef;
        _ ->
            undefined
    end.

-spec all(t()) ->
    [
        {
            emqx_extsub_types:handler_ref(),
            emqx_extsub_types:topic_filter(),
            emqx_extsub_handler:t()
        }
    ].
all(#registry{by_ref = ByRef}) ->
    [
        {HandlerRef, TopicFilter, Handler}
     || {HandlerRef, #extsub{topic_filter = TopicFilter, handler = Handler}} <- maps:to_list(
            ByRef
        )
    ].
