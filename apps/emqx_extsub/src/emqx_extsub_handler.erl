%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_handler).

-include("emqx_extsub_internal.hrl").

-export([
    subscribe/4,
    subscribe_new/5,
    unsubscribe/4,
    terminate/1,
    delivered/4,
    info/3,
    save_subopts/3,
    get_options/1,
    get_option/2,
    get_option/3,
    get_module/1
]).

-type state() :: term().

-type subscribe_type() :: subscribe | resume.
-type unsubscribe_type() :: unsubscribe | disconnect.

-type subscribe_ctx() :: #{
    %% Different info about the enclosing channel
    clientinfo := emqx_types:clientinfo(),
    conninfo_fn := fun((atom()) -> term()),
    subopts := emqx_types:subopts(),
    can_receive_acks := boolean(),

    %% Functions to send messages to the handler itself
    send_after := fun((emqx_extsub_types:interval_ms(), term()) -> reference()),
    send := fun((term()) -> ok),
    _ => _
}.

-type unsubscribe_ctx() :: #{
    subopts := emqx_types:subopts()
}.

-type info_ctx() :: #{
    desired_message_count := non_neg_integer(),
    delivering_count := non_neg_integer(),
    _ => _
}.

-type ack_ctx() :: #{
    unacked_count := non_neg_integer(),
    delivering_count => non_neg_integer(),
    desired_message_count := non_neg_integer(),
    qos := emqx_types:qos(),
    _ => _
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
    subscribe_type(),
    subscribe_ctx(),
    state() | undefined,
    emqx_extsub_types:topic_filter()
) ->
    {ok, state()} | ignore.
-callback handle_unsubscribe(
    unsubscribe_type(), unsubscribe_ctx(), state(), emqx_extsub_types:topic_filter()
) ->
    state().
-callback handle_terminate(state()) -> ok.
-callback handle_delivered(
    state(),
    ack_ctx(),
    emqx_types:message(),
    emqx_extsub_types:ack()
) ->
    {ok, state()}
    | {destroy, [emqx_extsub_types:topic_filter()]}
    | destroy.
-callback handle_info(state(), info_ctx(), term()) ->
    {ok, state()}
    | {ok, state(), [emqx_types:message()]}
    | recreate
    | {destroy, [emqx_extsub_types:topic_filter()]}
    | destroy.

-optional_callbacks([
    handle_unsubscribe/4,
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

-spec unsubscribe(unsubscribe_type(), unsubscribe_ctx(), t(), emqx_extsub_types:topic_filter()) ->
    t().
unsubscribe(
    UnsubscribeType, UnsubscribeCtx, #handler{cbm = CBM, st = State0} = Handler, TopicFilter
) ->
    case erlang:function_exported(CBM, handle_unsubscribe, 4) of
        true ->
            State = CBM:handle_unsubscribe(UnsubscribeType, UnsubscribeCtx, State0, TopicFilter),
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

-spec delivered(t(), ack_ctx(), emqx_types:message(), emqx_extsub_types:ack()) ->
    {ok, t()}
    | {destroy, [emqx_extsub_types:topic_filter()]}
    | destroy.
delivered(#handler{cbm = CBM, st = State0} = Handler, AckCtx, Msg, Ack) ->
    case CBM:handle_delivered(State0, AckCtx, Msg, Ack) of
        {ok, State} ->
            {ok, Handler#handler{st = State}};
        {destroy, TopicFilters} ->
            {destroy, TopicFilters};
        destroy ->
            destroy
    end.

-spec info(t(), info_ctx(), term()) ->
    {ok, t()}
    | {ok, t(), [emqx_types:message()]}
    | {destroy, [emqx_extsub_types:topic_filter()]}
    | destroy
    | recreate.
info(#handler{cbm = CBM, st = State0} = Handler, InfoCtx, Info) ->
    {TimeUs, Result} = timer:tc(CBM, handle_info, [State0, InfoCtx, Info]),
    TimeMs = erlang:convert_time_unit(TimeUs, microsecond, millisecond),
    emqx_extsub_metrics:observe_hist(handle_info_latency_ms, TimeMs),
    case Result of
        {ok, State} ->
            {ok, Handler#handler{st = State}};
        {ok, State, Messages} ->
            {ok, Handler#handler{st = State}, Messages};
        {destroy, TopicFilters} ->
            {destroy, TopicFilters};
        destroy ->
            destroy;
        recreate ->
            recreate
    end.

save_subopts(#handler{cbm = CBM, st = State0} = Handler0, Context, SubOpts) ->
    case erlang:function_exported(CBM, handle_save_subopts, 3) of
        true ->
            case CBM:handle_save_subopts(State0, Context, SubOpts) of
                {ok, State, Res} ->
                    {ok, Handler0#handler{st = State}, Res};
                {ok, State} ->
                    {ok, Handler0#handler{st = State}}
            end;
        false ->
            {ok, Handler0}
    end.
