%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_handler).

-include("emqx_extsub_internal.hrl").

-export([
    subscribe/4,
    subscribe_new/5,
    unsubscribe/3,
    terminate/1,
    delivered/4,
    info/3,
    get_options/1,
    get_option/2,
    get_option/3,
    get_module/1
]).

-type state() :: term().

-type subscribe_type() :: subscribe | resume.
-type unsubscribe_type() :: unsubscribe | disconnect.

-type subscribe_ctx() :: #{
    clientinfo := emqx_types:clientinfo(),
    send_after := fun((emqx_extsub_types:interval_ms(), term()) -> reference()),
    send := fun((term()) -> ok),
    can_receive_acks := boolean()
}.

-type info_ctx() :: #{
    desired_message_count := non_neg_integer()
}.

-type ack_ctx() :: #{
    desired_message_count := non_neg_integer(),
    qos := emqx_types:qos()
}.

-record(handler, {
    st :: state(),
    cbm :: module(),
    options :: emqx_extsub_types:handler_options()
}).

-type t() :: #handler{}.

-export_type([
    t/0,
    subscribe_type/0,
    unsubscribe_type/0,
    subscribe_ctx/0,
    info_ctx/0,
    ack_ctx/0,
    state/0
]).

%% Handler callbacks

-callback handle_subscribe(
    subscribe_type(), subscribe_ctx(), state() | undefined, emqx_extsub_types:topic_filter()
) ->
    {ok, state()} | ignore.
-callback handle_unsubscribe(unsubscribe_type(), state(), emqx_extsub_types:topic_filter()) ->
    state().
-callback handle_terminate(state()) -> ok.
-callback handle_delivered(
    state(),
    ack_ctx(),
    emqx_types:message(),
    emqx_extsub_types:ack()
) -> state().
-callback handle_info(state(), info_ctx(), term()) ->
    {ok, state()}
    | {ok, state(), [emqx_types:message()]}
    | recreate.

-optional_callbacks([
    handle_unsubscribe/3,
    handle_terminate/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Handler options

-spec get_options(t()) -> emqx_extsub_types:handler_options().
get_options(#handler{options = Options}) ->
    Options.

-spec get_option(atom(), t()) -> term().
get_option(Name, Handler) ->
    get_option(Name, Handler, undefined).

-spec get_option(atom(), t(), term()) -> term().
get_option(Name, #handler{options = Options} = _Handler, Default) ->
    maps:get(Name, Options, Default).

-spec get_module(t()) -> module().
get_module(#handler{cbm = CBM}) ->
    CBM.

%% Working with ExtSub Handler implementations

-spec subscribe_new(
    subscribe_type(),
    module(),
    emqx_extsub_types:handler_options(),
    subscribe_ctx(),
    emqx_extsub_types:topic_filter()
) ->
    {ok, t()} | ignore.
subscribe_new(SubscribeType, CBM, Options, InitCtx, TopicFilter) ->
    try CBM:handle_subscribe(SubscribeType, InitCtx, undefined, TopicFilter) of
        ignore ->
            ignore;
        {ok, State} ->
            {ok, #handler{
                cbm = CBM,
                st = State,
                options = Options
            }}
    catch
        Class:Reason:StackTrace ->
            ?tp(error, extsub_handle_init_error, #{
                class => Class,
                reason => Reason,
                topic_filter => TopicFilter,
                stacktrace => StackTrace
            }),
            ignore
    end.

-spec subscribe(subscribe_type(), subscribe_ctx(), t(), emqx_extsub_types:topic_filter()) ->
    {ok, t()} | ignore.
subscribe(SubscribeType, SubscribeCtx, #handler{cbm = CBM, st = State0} = Handler, TopicFilter) ->
    case CBM:handle_subscribe(SubscribeType, SubscribeCtx, State0, TopicFilter) of
        ignore ->
            ignore;
        {ok, State} ->
            {ok, Handler#handler{st = State}}
    end.

-spec unsubscribe(unsubscribe_type(), t(), emqx_extsub_types:topic_filter()) -> t().
unsubscribe(UnsubscribeType, #handler{cbm = CBM, st = State0} = Handler, TopicFilter) ->
    case erlang:function_exported(CBM, handle_unsubscribe, 3) of
        true ->
            State = CBM:handle_unsubscribe(UnsubscribeType, State0, TopicFilter),
            Handler#handler{st = State};
        false ->
            Handler
    end.

-spec terminate(t()) -> ok.
terminate(#handler{cbm = CBM, st = State}) ->
    case erlang:function_exported(CBM, handle_terminate, 1) of
        true ->
            _ = CBM:handle_terminate(State),
            ok;
        false ->
            ok
    end.

-spec delivered(t(), ack_ctx(), emqx_types:message(), emqx_extsub_types:ack()) -> t().
delivered(#handler{cbm = CBM, st = State} = Handler, AckCtx, Msg, Ack) ->
    Handler#handler{st = CBM:handle_delivered(State, AckCtx, Msg, Ack)}.

-spec info(t(), info_ctx(), term()) ->
    {ok, t()} | {ok, t(), [emqx_types:message()]} | recreate.
info(#handler{cbm = CBM, st = State0} = Handler, InfoCtx, Info) ->
    {TimeUs, Result} = timer:tc(CBM, handle_info, [State0, InfoCtx, Info]),
    TimeMs = erlang:convert_time_unit(TimeUs, microsecond, millisecond),
    emqx_extsub_metrics:observe_hist(handle_info_latency_ms, TimeMs),
    case Result of
        {ok, State} ->
            {ok, Handler#handler{st = State}};
        {ok, State, Messages} ->
            {ok, Handler#handler{st = State}, Messages};
        recreate ->
            recreate
    end.
