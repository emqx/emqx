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
    init/0,
    register/2,
    unregister/1
]).

-export([
    new/0,
    subscribe/4,
    unsubscribe/3,
    find/2,
    update/3,
    recreate/3
]).

-record(extsub, {
    handler :: emqx_extsub_handler:t(),
    %% TODO
    %% Use set
    topic_filters :: [emqx_extsub_types:topic_filter()]
}).

-record(registry, {
    by_ref :: #{emqx_extsub_types:handler_ref() => #extsub{}},
    by_topic_cbm :: #{
        {module(), emqx_extsub_types:topic_filter()} => emqx_extsub_types:handler_ref()
    }
}).

-type t() :: #registry{}.

-export_type([t/0]).

-define(TAB, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Handler registration

-spec init() -> ok.
init() ->
    emqx_utils_ets:new(?TAB, [set, public, named_table, {read_concurrency, true}]).

-spec register(module(), emqx_extsub_types:handler_options()) -> ok.
register(CBM, Options) ->
    case ets:insert_new(?TAB, {CBM, Options}) of
        true -> ok;
        false -> error({extsub_handler_already_registered, CBM})
    end.

-spec unregister(module()) -> ok.
unregister(CBM) ->
    _ = ets:delete(?TAB, CBM),
    ok.

%% Instance methods

-spec new() -> t().
new() ->
    #registry{by_ref = #{}, by_topic_cbm = #{}}.

-spec subscribe(
    t(),
    emqx_extsub_handler:subscribe_type(),
    emqx_extsub_handler:subscribe_ctx(),
    [emqx_extsub_types:topic_filter()]
) -> t().
subscribe(Registry, SubscribeType, SubscribeCtx, TopicFilters) ->
    lists:foldl(
        fun({CBM, TopicFilter}, RegistryAcc) ->
            subscribe(RegistryAcc, SubscribeType, SubscribeCtx, CBM, TopicFilter)
        end,
        Registry,
        [{CBM, TopicFilter} || CBM <- cbms(), TopicFilter <- TopicFilters]
    ).

-spec unsubscribe(t(), emqx_extsub_handler:unsubscribe_type(), [emqx_extsub_types:topic_filter()]) ->
    t().
unsubscribe(
    #registry{by_topic_cbm = ByTopicCBM} = Registry, TerminateType, TopicFilters
) ->
    TopicFilterSet = sets:from_list(TopicFilters, [{version, 2}]),
    maps:fold(
        fun({Module, TopicFilter}, HandlerRef, RegistryAcc) ->
            case sets:is_element(TopicFilter, TopicFilterSet) of
                true ->
                    unsubscribe(RegistryAcc, TerminateType, Module, TopicFilter, HandlerRef);
                false ->
                    RegistryAcc
            end
        end,
        Registry,
        ByTopicCBM
    ).

-spec find(t(), emqx_extsub_types:handler_ref()) ->
    emqx_extsub_handler:t() | undefined.
find(#registry{by_ref = ByRef}, HandlerRef) when is_reference(HandlerRef) ->
    case ByRef of
        #{HandlerRef := #extsub{handler = Handler}} ->
            Handler;
        _ ->
            undefined
    end.

-spec update(t(), emqx_extsub_types:handler_ref(), emqx_extsub_handler:t()) -> t().
update(#registry{by_ref = ByRef} = Registry, HandlerRef, Handler) ->
    case ByRef of
        #{HandlerRef := #extsub{} = ExtSub} ->
            Registry#registry{
                by_ref = ByRef#{
                    HandlerRef => ExtSub#extsub{
                        handler = Handler
                    }
                }
            };
        _ ->
            error({extsub_registry_handler_not_found, HandlerRef})
    end.

recreate(
    #registry{by_ref = ByRef0, by_topic_cbm = ByTopicCBM0} = Registry0, SubscribeCtx, HandlerRef
) ->
    #extsub{topic_filters = TopicFilters, handler = Handler} = maps:get(HandlerRef, ByRef0),
    Module = emqx_extsub_handler:get_module(Handler),
    Options = emqx_extsub_handler:get_options(Handler),
    ByTopicCBM = maps:filter(
        fun({_Module, _TopicFilter}, HRef) ->
            HRef =/= HandlerRef
        end,
        ByTopicCBM0
    ),
    ByRef = maps:remove(HandlerRef, ByRef0),
    Registry = Registry0#registry{by_ref = ByRef, by_topic_cbm = ByTopicCBM},
    lists:foldl(
        fun(TopicFilter, RegistryAcc) ->
            subscribe(RegistryAcc, resume, SubscribeCtx, {Module, Options}, TopicFilter)
        end,
        Registry,
        TopicFilters
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

subscribe(
    #registry{by_ref = ByRef, by_topic_cbm = ByTopicCBM} = Registry,
    SubscribeType,
    SubscribeCtx0,
    {Module, #{multi_topic := true} = Options},
    TopicFilter
) ->
    ?tp_debug(extsub_handler_registry_subscribe_multi_topic, #{
        module => Module, options => Options, topic_filter => TopicFilter
    }),
    case ByTopicCBM of
        #{{Module, TopicFilter} := _HandlerRef} ->
            ?tp_debug(extsub_handler_registry_subscribe_multi_topic_already_subscribed, #{
                module => Module, options => Options, topic_filter => TopicFilter
            }),
            Registry;
        _ ->
            ModuleHandlerRefs = [
                HandlerRef
             || {{Mod, _TF}, HandlerRef} <- maps:to_list(ByTopicCBM), Mod =:= Module
            ],
            case ModuleHandlerRefs of
                [HandlerRef] ->
                    #extsub{handler = Handler0, topic_filters = TopicFilters} = maps:get(
                        HandlerRef, ByRef
                    ),
                    SubscribeCtx = create_subscribe_ctx(HandlerRef, SubscribeCtx0),
                    case
                        emqx_extsub_handler:subscribe(
                            SubscribeType, SubscribeCtx, Handler0, TopicFilter
                        )
                    of
                        {ok, Handler} ->
                            ?tp_debug(extsub_handler_registry_subscribe_multi_topic_subscribed, #{
                                module => Module, options => Options, topic_filter => TopicFilter
                            }),
                            Registry#registry{
                                by_ref = ByRef#{
                                    HandlerRef => #extsub{
                                        handler = Handler,
                                        topic_filters = [TopicFilter | TopicFilters]
                                    }
                                },
                                by_topic_cbm = ByTopicCBM#{{Module, TopicFilter} => HandlerRef}
                            };
                        ignore ->
                            ?tp_debug(extsub_handler_registry_subscribe_multi_topic_ignore, #{
                                module => Module, options => Options, topic_filter => TopicFilter
                            }),
                            Registry
                    end;
                [] ->
                    HandlerRef = make_ref(),
                    SubscribeCtx = create_subscribe_ctx(HandlerRef, SubscribeCtx0),
                    case
                        emqx_extsub_handler:subscribe_new(
                            SubscribeType, Module, Options, SubscribeCtx, TopicFilter
                        )
                    of
                        {ok, Handler} ->
                            ?tp_debug(
                                extsub_handler_registry_subscribe_multi_topic_subscribed_new, #{
                                    module => Module,
                                    options => Options,
                                    topic_filter => TopicFilter
                                }
                            ),
                            Registry#registry{
                                by_ref = ByRef#{
                                    HandlerRef => #extsub{
                                        handler = Handler, topic_filters = [TopicFilter]
                                    }
                                },
                                by_topic_cbm = ByTopicCBM#{{Module, TopicFilter} => HandlerRef}
                            };
                        ignore ->
                            ?tp_debug(extsub_handler_registry_subscribe_multi_topic_ignore_new, #{
                                module => Module, options => Options, topic_filter => TopicFilter
                            }),
                            Registry
                    end
            end
    end;
subscribe(
    #registry{by_ref = ByRef, by_topic_cbm = ByTopicCBM} = Registry,
    InitType,
    InitCtx0,
    {Module, #{multi_topic := false} = Options},
    TopicFilter
) ->
    ?tp_debug(extsub_handler_registry_subscribe_single_topic, #{
        module => Module, options => Options, topic_filter => TopicFilter
    }),
    case ByTopicCBM of
        #{{Module, TopicFilter} := _HandlerRef} ->
            Registry;
        _ ->
            HandlerRef = make_ref(),
            InitCtx = create_subscribe_ctx(HandlerRef, InitCtx0),
            case
                emqx_extsub_handler:subscribe_new(InitType, Module, Options, InitCtx, TopicFilter)
            of
                {ok, Handler} ->
                    Registry#registry{
                        by_ref = ByRef#{
                            HandlerRef => #extsub{handler = Handler, topic_filters = [TopicFilter]}
                        },
                        by_topic_cbm = ByTopicCBM#{{Module, TopicFilter} => HandlerRef}
                    };
                ignore ->
                    Registry
            end
    end.

unsubscribe(
    #registry{by_ref = ByRef0, by_topic_cbm = ByTopicCBM0} = Registry,
    UnsubscribeType,
    Module,
    TopicFilter,
    HandlerRef
) ->
    #extsub{handler = Handler0, topic_filters = HandlerTopicFilters0} = maps:get(
        HandlerRef, ByRef0
    ),
    Handler = emqx_extsub_handler:unsubscribe(UnsubscribeType, Handler0, TopicFilter),
    HandlerTopicFilters = lists:delete(TopicFilter, HandlerTopicFilters0),
    ByRef =
        case HandlerTopicFilters of
            [] ->
                ok = emqx_extsub_handler:terminate(Handler),
                maps:remove(HandlerRef, ByRef0);
            _ ->
                ByRef0#{
                    HandlerRef => #extsub{handler = Handler, topic_filters = HandlerTopicFilters}
                }
        end,
    ByTopicCBM = maps:remove({Module, TopicFilter}, ByTopicCBM0),
    Registry#registry{by_ref = ByRef, by_topic_cbm = ByTopicCBM}.

cbms() ->
    ets:tab2list(?TAB).

create_subscribe_ctx(Ref, Ctx) ->
    ?tp_debug(extsub_handler_registry_create_subscribe_ctx, #{ref => Ref, ctx => Ctx}),
    Pid = self(),
    SendAfter = fun(Interval, Info) ->
        ?tp_debug(extsub_handler_registry_create_subscribe_ctx_send_after, #{
            to => Pid, interval => Interval, info => Info
        }),
        _ = erlang:send_after(Interval, Pid, #info_to_extsub{
            handler_ref = Ref, info = Info
        }),
        ok
    end,
    Send = fun(Info) ->
        ?tp_debug(extsub_handler_registry_create_subscribe_ctx_send, #{to => Pid, info => Info}),
        _ = erlang:send(Pid, #info_to_extsub{
            handler_ref = Ref, info = Info
        }),
        ok
    end,
    Ctx#{
        send_after => SendAfter,
        send => Send
    }.
