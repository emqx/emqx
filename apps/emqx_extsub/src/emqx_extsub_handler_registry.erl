%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_handler_registry).

-moduledoc """
Collection of handlers for the external message sources.
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
    save_subopts/3,
    delivered/6,
    info/4
]).

-export([
    generic_message_handlers/1,
    message_counts/2,
    inspect/1,
    buffer_size/1,
    buffer_add/3,
    buffer_take/2,
    buffer_add_back/4
]).

-record(extsub, {
    handler :: emqx_extsub_handler:t(),
    topic_filters :: #{emqx_extsub_types:topic_filter() => emqx_types:subopts()}
}).

-record(registry, {
    buffer :: emqx_extsub_buffer:t(),
    by_ref :: #{emqx_extsub_types:handler_ref() => #extsub{}},
    by_topic_cbm :: #{
        {module(), emqx_extsub_types:topic_filter()} => emqx_extsub_types:handler_ref()
    },
    generic_message_handlers :: [emqx_extsub_types:handler_ref()]
}).

-type t() :: #registry{}.

-type subscribe_init_ctx() :: #{
    clientinfo := emqx_types:clientinfo(),
    can_receive_acks := boolean()
}.

-type info_init_ctx() :: #{
    desired_message_count := non_neg_integer(),
    delivering_count := non_neg_integer(),
    clientinfo := emqx_types:clientinfo(),
    can_receive_acks := boolean()
}.

-export_type([t/0]).

-define(TAB, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Handler registration

-spec init() -> ok.
init() ->
    emqx_utils_ets:new(?TAB, [ordered_set, public, named_table, {read_concurrency, true}]).

-spec register(module(), emqx_extsub_types:handler_options()) ->
    ok | {error, extsub_handler_already_registered}.
register(CBM, Options) ->
    case ets:insert_new(?TAB, {CBM, Options}) of
        true -> ok;
        false -> {error, extsub_handler_already_registered}
    end.

-spec unregister(module()) -> ok.
unregister(CBM) ->
    _ = ets:delete(?TAB, CBM),
    ok.

%% Instance methods

-spec new() -> t().
new() ->
    #registry{
        buffer = emqx_extsub_buffer:new(),
        by_ref = #{},
        by_topic_cbm = #{},
        generic_message_handlers = []
    }.

-spec subscribe(
    t(),
    emqx_extsub_handler:subscribe_type(),
    subscribe_init_ctx(),
    #{emqx_extsub_types:topic_filter() => emqx_types:subopts()}
) -> t().
subscribe(Registry, SubscribeType, SubscribeCtx, TopicFiltersToSubOpts) ->
    lists:foldl(
        fun({CBM, TopicFilter, SubOpts}, RegistryAcc) ->
            subscribe(RegistryAcc, SubscribeType, SubscribeCtx, CBM, TopicFilter, SubOpts)
        end,
        Registry,
        [
            {CBM, TopicFilter, SubOpts}
         || CBM <- cbms(),
            {TopicFilter, SubOpts} <- maps:to_list(TopicFiltersToSubOpts)
        ]
    ).

-spec unsubscribe(t(), emqx_extsub_handler:unsubscribe_type(), #{
    emqx_extsub_types:topic_filter() => emqx_types:subopts()
}) ->
    t().
unsubscribe(
    #registry{by_topic_cbm = ByTopicCBM} = Registry, TerminateType, Subs
) ->
    maps:fold(
        fun({Module, TopicFilter}, HandlerRef, RegistryAcc) ->
            case Subs of
                #{TopicFilter := SubOpts} ->
                    unsubscribe(
                        RegistryAcc, TerminateType, SubOpts, Module, TopicFilter, HandlerRef
                    );
                _ ->
                    RegistryAcc
            end
        end,
        Registry,
        ByTopicCBM
    ).

save_subopts(#registry{by_topic_cbm = ByTopicCBM} = Registry0, Context, SubOpts) ->
    #{topic_filter := TopicFilter} = Context,
    maps:fold(
        fun
            ({Module, HandlerTopicFilter}, HandlerRef, {OutAcc0, RegistryAcc0}) when
                HandlerTopicFilter == TopicFilter
            ->
                #registry{by_ref = ByRef0} = RegistryAcc0,
                #{HandlerRef := #extsub{handler = Handler0} = ExtSub0} = ByRef0,
                case emqx_extsub_handler:save_subopts(Handler0, Context, SubOpts) of
                    {ok, Handler} ->
                        ExtSub = ExtSub0#extsub{handler = Handler},
                        ByRef = ByRef0#{HandlerRef := ExtSub},
                        RegistryAcc = RegistryAcc0#registry{by_ref = ByRef},
                        {OutAcc0, RegistryAcc};
                    {ok, Handler, Res} ->
                        ExtSub = ExtSub0#extsub{handler = Handler},
                        ByRef = ByRef0#{HandlerRef := ExtSub},
                        RegistryAcc = RegistryAcc0#registry{by_ref = ByRef},
                        OutAcc = OutAcc0#{Module => Res},
                        {OutAcc, RegistryAcc}
                end;
            (_ModTF, _HandlerRef, Acc) ->
                Acc
        end,
        {#{}, Registry0},
        ByTopicCBM
    ).

-spec delivered(
    t(),
    emqx_extsub_types:handler_ref(),
    emqx_extsub_handler:ack_ctx(),
    emqx_extsub_buffer:seq_id(),
    emqx_types:message(),
    emqx_types:reason_code()
) -> t().
delivered(
    #registry{buffer = Buffer0, by_ref = ByRef} = Registry0,
    HandlerRef,
    AckCtx,
    SeqId,
    Msg,
    ReasonCode
) ->
    case ByRef of
        #{HandlerRef := #extsub{handler = Handler0} = ExtSub} ->
            Buffer = emqx_extsub_buffer:set_delivered(Buffer0, HandlerRef, SeqId),
            case emqx_extsub_handler:delivered(Handler0, AckCtx, Msg, ReasonCode) of
                {ok, Handler} ->
                    Registry0#registry{
                        buffer = Buffer,
                        by_ref = ByRef#{HandlerRef := ExtSub#extsub{handler = Handler}}
                    };
                {destroy, TopicFilters} ->
                    destroy(Registry0, HandlerRef, TopicFilters);
                destroy ->
                    destroy_all(Registry0, HandlerRef)
            end;
        _ ->
            Registry0
    end.

-spec info(t(), emqx_extsub_types:handler_ref(), info_init_ctx(), term()) ->
    {ok, t()}
    | {ok, t(), [emqx_types:message()]}
    | {destroy, [emqx_extsub_types:topic_filter()]}
    | destroy
    | recreate.
info(
    #registry{by_ref = ByRef} = Registry0, HandlerRef, InfoCtx, Info
) ->
    case ByRef of
        #{HandlerRef := #extsub{handler = Handler0} = ExtSub} ->
            case emqx_extsub_handler:info(Handler0, InfoCtx, Info) of
                {ok, Handler} ->
                    {ok, Registry0#registry{
                        by_ref = ByRef#{HandlerRef := ExtSub#extsub{handler = Handler}}
                    }};
                {ok, Handler, Messages} ->
                    {ok,
                        Registry0#registry{
                            by_ref = ByRef#{HandlerRef := ExtSub#extsub{handler = Handler}}
                        },
                        Messages};
                {destroy, TopicFilters} ->
                    {ok, destroy(Registry0, HandlerRef, TopicFilters)};
                destroy ->
                    {ok, destroy_all(Registry0, HandlerRef)};
                recreate ->
                    {ok, recreate(Registry0, to_subscribe_init_ctx(InfoCtx), HandlerRef)}
            end;
        _ ->
            {ok, Registry0}
    end.

-spec generic_message_handlers(t()) -> [emqx_extsub_types:handler_ref()].
generic_message_handlers(#registry{generic_message_handlers = GenericMessageHandlers}) ->
    GenericMessageHandlers.

-spec inspect(t()) -> map().
inspect(#registry{
    buffer = Buffer,
    by_ref = ByRef,
    by_topic_cbm = ByTopicCBM,
    generic_message_handlers = GenericMessageHandlers
}) ->
    #{
        buffer => emqx_extsub_buffer:inspect(Buffer),
        by_ref => maps:keys(ByRef),
        by_topic_cbm => ByTopicCBM,
        generic_message_handlers => GenericMessageHandlers
    }.

-spec message_counts(t(), emqx_extsub_types:handler_ref()) ->
    {non_neg_integer(), non_neg_integer()}.
message_counts(#registry{by_ref = ByRef, buffer = Buffer}, HandlerRef) ->
    case ByRef of
        #{HandlerRef := #extsub{handler = Handler}} ->
            BufferSize = emqx_extsub_handler:get_option(buffer_size, Handler, ?EXTSUB_BUFFER_SIZE),
            DeliveringCnt = emqx_extsub_buffer:delivering_count(Buffer, HandlerRef),
            case DeliveringCnt of
                N when N < BufferSize andalso is_integer(BufferSize) ->
                    {DeliveringCnt, BufferSize};
                _ ->
                    {DeliveringCnt, 0}
            end;
        _ ->
            {0, 0}
    end.

-spec buffer_size(t()) -> non_neg_integer().
buffer_size(#registry{buffer = Buffer}) ->
    emqx_extsub_buffer:size(Buffer).

-spec buffer_add(t(), emqx_extsub_types:handler_ref(), [emqx_types:message()]) -> t().
buffer_add(#registry{buffer = Buffer0} = Registry, HandlerRef, Messages) ->
    Buffer = emqx_extsub_buffer:add_new(Buffer0, HandlerRef, Messages),
    Registry#registry{buffer = Buffer}.

-spec buffer_take(t(), non_neg_integer()) ->
    {
        [{emqx_extsub_types:handler_ref(), emqx_extsub_buffer:seq_id(), emqx_types:message()}],
        t()
    }.
buffer_take(#registry{buffer = Buffer0} = Registry, N) ->
    {Messages, Buffer} = emqx_extsub_buffer:take(Buffer0, N),
    {Messages, Registry#registry{buffer = Buffer}}.

-spec buffer_add_back(
    t(), emqx_extsub_types:handler_ref(), emqx_extsub_buffer:seq_id(), emqx_types:message()
) -> t().
buffer_add_back(#registry{buffer = Buffer0} = Registry, HandlerRef, SeqId, Msg) ->
    Buffer = emqx_extsub_buffer:add_back(Buffer0, HandlerRef, SeqId, Msg),
    Registry#registry{buffer = Buffer}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

subscribe(
    #registry{
        by_ref = ByRef, by_topic_cbm = ByTopicCBM, generic_message_handlers = GenericMessageHandlers
    } = Registry,
    SubscribeType,
    SubscribeCtx0,
    {Module, #{multi_topic := true} = Options} = HandlerSpec,
    TopicFilter,
    SubOpts
) ->
    IgnoreResubscribe = maps:get(ignore_resubscribe, Options, true),
    case ByTopicCBM of
        #{{Module, TopicFilter} := _HandlerRef} when IgnoreResubscribe ->
            Registry;
        #{{Module, TopicFilter} := HandlerRef} ->
            %% Should be parameterizable?
            TerminateType = unsubscribe,
            NewRegistry = unsubscribe(
                Registry, TerminateType, SubOpts, Module, TopicFilter, HandlerRef
            ),
            subscribe(NewRegistry, SubscribeType, SubscribeCtx0, HandlerSpec, TopicFilter, SubOpts);
        _ ->
            case find_module_handler_ref(Registry, Module) of
                {ok, HandlerRef} ->
                    #extsub{handler = Handler0, topic_filters = TopicFiltersToSubOpts} = maps:get(
                        HandlerRef, ByRef
                    ),
                    SubscribeCtx = create_subscribe_ctx(HandlerRef, Module, SubOpts, SubscribeCtx0),
                    case
                        emqx_extsub_handler:subscribe(
                            SubscribeType, SubscribeCtx, Handler0, TopicFilter
                        )
                    of
                        {ok, Handler} ->
                            Registry#registry{
                                by_ref = ByRef#{
                                    HandlerRef => #extsub{
                                        handler = Handler,
                                        topic_filters = TopicFiltersToSubOpts#{
                                            TopicFilter => SubOpts
                                        }
                                    }
                                },
                                by_topic_cbm = ByTopicCBM#{{Module, TopicFilter} => HandlerRef}
                            };
                        ignore ->
                            Registry
                    end;
                not_found ->
                    HandlerRef = make_ref(),
                    SubscribeCtx = create_subscribe_ctx(HandlerRef, Module, SubOpts, SubscribeCtx0),
                    case
                        emqx_extsub_handler:subscribe_new(
                            SubscribeType, Module, Options, SubscribeCtx, TopicFilter
                        )
                    of
                        {ok, Handler} ->
                            Registry#registry{
                                by_ref = ByRef#{
                                    HandlerRef => #extsub{
                                        handler = Handler,
                                        topic_filters = #{TopicFilter => SubOpts}
                                    }
                                },
                                by_topic_cbm = ByTopicCBM#{{Module, TopicFilter} => HandlerRef},
                                generic_message_handlers = add_to_generic_message_handlers(
                                    GenericMessageHandlers, HandlerRef, Options
                                )
                            };
                        ignore ->
                            Registry
                    end
            end
    end;
subscribe(
    #registry{
        by_ref = ByRef, by_topic_cbm = ByTopicCBM, generic_message_handlers = GenericMessageHandlers
    } = Registry,
    InitType,
    InitCtx0,
    {Module, #{multi_topic := false} = Options} = HandlerSpec,
    TopicFilter,
    SubOpts
) ->
    IgnoreResubscribe = maps:get(ignore_resubscribe, Options, true),
    case ByTopicCBM of
        #{{Module, TopicFilter} := _HandlerRef} when IgnoreResubscribe ->
            Registry;
        #{{Module, TopicFilter} := HandlerRef} ->
            %% Should be parameterizable?
            TerminateType = unsubscribe,
            NewRegistry = unsubscribe(
                Registry, TerminateType, SubOpts, Module, TopicFilter, HandlerRef
            ),
            subscribe(NewRegistry, InitType, InitCtx0, HandlerSpec, TopicFilter, SubOpts);
        _ ->
            HandlerRef = make_ref(),
            InitCtx = create_subscribe_ctx(HandlerRef, Module, SubOpts, InitCtx0),
            case
                emqx_extsub_handler:subscribe_new(InitType, Module, Options, InitCtx, TopicFilter)
            of
                {ok, Handler} ->
                    Registry#registry{
                        by_ref = ByRef#{
                            HandlerRef => #extsub{
                                handler = Handler,
                                topic_filters = #{TopicFilter => SubOpts}
                            }
                        },
                        by_topic_cbm = ByTopicCBM#{{Module, TopicFilter} => HandlerRef},
                        generic_message_handlers = add_to_generic_message_handlers(
                            GenericMessageHandlers, HandlerRef, Options
                        )
                    };
                ignore ->
                    Registry
            end
    end.

unsubscribe(
    #registry{
        buffer = Buffer0,
        by_ref = ByRef0,
        by_topic_cbm = ByTopicCBM0,
        generic_message_handlers = GenericMessageHandlers
    } = Registry,
    UnsubscribeType,
    SubOpts,
    Module,
    TopicFilter,
    HandlerRef
) ->
    #extsub{handler = Handler0, topic_filters = HandlerTopicFilters0} = maps:get(
        HandlerRef, ByRef0
    ),
    UnsubscribeCtx = create_unsubscribe_ctx(SubOpts),
    Handler = emqx_extsub_handler:unsubscribe(
        UnsubscribeType, UnsubscribeCtx, Handler0, TopicFilter
    ),
    HandlerTopicFilters = maps:remove(TopicFilter, HandlerTopicFilters0),
    case map_size(HandlerTopicFilters) of
        0 ->
            ok = emqx_extsub_handler:terminate(Handler),
            ByRef = maps:remove(HandlerRef, ByRef0),
            Buffer = emqx_extsub_buffer:drop_handler(Buffer0, HandlerRef);
        _ ->
            ByRef = ByRef0#{
                HandlerRef => #extsub{handler = Handler, topic_filters = HandlerTopicFilters}
            },
            Buffer = Buffer0
    end,
    ByTopicCBM = maps:remove({Module, TopicFilter}, ByTopicCBM0),
    Registry#registry{
        buffer = Buffer,
        by_ref = ByRef,
        by_topic_cbm = ByTopicCBM,
        generic_message_handlers = remove_from_generic_message_handlers(
            GenericMessageHandlers, HandlerRef
        )
    }.

destroy(#registry{} = Registry0, HandlerRef, TopicFilters) ->
    #registry{by_ref = ByRef0} = Registry0,
    #extsub{topic_filters = TopicFiltersToSubOpts, handler = Handler} = maps:get(
        HandlerRef, ByRef0
    ),
    Module = emqx_extsub_handler:get_module(Handler),
    UnsubscribeType = disconnect,
    maps:fold(
        fun(TopicFilter, SubOpts, RegistryAcc) ->
            unsubscribe(
                RegistryAcc, UnsubscribeType, SubOpts, Module, TopicFilter, HandlerRef
            )
        end,
        Registry0,
        maps:with(TopicFilters, TopicFiltersToSubOpts)
    ).

destroy_all(#registry{} = Registry0, HandlerRef) ->
    #registry{by_ref = ByRef0} = Registry0,
    #extsub{topic_filters = TopicFiltersToSubOpts} = maps:get(HandlerRef, ByRef0),
    TopicFilters = maps:keys(TopicFiltersToSubOpts),
    destroy(Registry0, HandlerRef, TopicFilters).

recreate(
    #registry{by_ref = ByRef0, by_topic_cbm = ByTopicCBM0} = Registry0, SubscribeInitCtx, HandlerRef
) ->
    #extsub{topic_filters = TopicFiltersToSubOpts, handler = Handler} = maps:get(
        HandlerRef, ByRef0
    ),
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
    maps:fold(
        fun(TopicFilter, SubOpts, RegistryAcc) ->
            subscribe(
                RegistryAcc, resume, SubscribeInitCtx, {Module, Options}, TopicFilter, SubOpts
            )
        end,
        Registry,
        TopicFiltersToSubOpts
    ).

find_module_handler_ref(#registry{by_topic_cbm = ByTopicCBM}, Module) ->
    do_find_module_handler_ref(maps:to_list(ByTopicCBM), Module).

do_find_module_handler_ref([], _Module) ->
    not_found;
do_find_module_handler_ref([{{Module, _TopicFilter}, HandlerRef} | _Rest], Module) ->
    {ok, HandlerRef};
do_find_module_handler_ref([_ | Rest], Module) ->
    do_find_module_handler_ref(Rest, Module).

cbms() ->
    ets:tab2list(?TAB).

add_to_generic_message_handlers(
    GenericMessageHandlers, HandlerRef, #{handle_generic_messages := true} = _Options
) ->
    case lists:member(HandlerRef, GenericMessageHandlers) of
        true ->
            GenericMessageHandlers;
        false ->
            [HandlerRef | GenericMessageHandlers]
    end;
add_to_generic_message_handlers(GenericMessageHandlers, _HandlerRef, _Options) ->
    GenericMessageHandlers.

remove_from_generic_message_handlers(GenericMessageHandlers, HandlerRef) ->
    lists:delete(HandlerRef, GenericMessageHandlers).

create_unsubscribe_ctx(SubOpts) ->
    #{
        subopts => SubOpts
    }.

create_subscribe_ctx(Ref, Module, SubOpts0, Ctx) ->
    Pid = self(),
    SendAfter = fun(Interval, Info) ->
        erlang:send_after(Interval, Pid, #info_to_extsub{
            handler_ref = Ref, info = Info
        })
    end,
    Send = fun(Info) ->
        _ = erlang:send(Pid, #info_to_extsub{
            handler_ref = Ref, info = Info
        }),
        ok
    end,
    SubOpts = emqx_extsub:filter_saved_subopts(Module, SubOpts0),
    Ctx#{
        subopts => SubOpts,
        send_after => SendAfter,
        send => Send
    }.

to_subscribe_init_ctx(Ctx) ->
    maps:with([clientinfo, can_receive_acks], Ctx).
