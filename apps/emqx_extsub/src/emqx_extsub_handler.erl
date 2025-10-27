%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_handler).

-include("emqx_extsub_internal.hrl").

-export([
    init/0,
    register/1,
    unregister/1
]).

-export([
    handle_init/3,
    handle_terminate/2,
    handle_ack/4,
    handle_info/3
]).

-type state() :: term().

-type init_type() :: subscribe | resume.
-type terminate_type() :: unsubscribe | disconnect.

-type init_ctx() :: #{
    clientinfo := emqx_types:clientinfo(),
    send_after := fun((emqx_extsub_types:interval_ms(), term()) -> reference()),
    send := fun((term()) -> ok)
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
    ctx :: init_ctx(),
    subscriber_ref :: emqx_extsub_types:subscriber_ref()
}).

-type t() :: #handler{}.

-export_type([t/0, init_type/0, terminate_type/0]).

-define(TAB, ?MODULE).
-define(EXTSUB_INTERNAL_MESSAGE_ID, extsub_int_msg_id).

%% ExtSub Handler behaviour

-callback handle_init(init_type(), init_ctx(), emqx_extsub_types:topic_filter()) ->
    {ok, state()} | ignore.
-callback handle_terminate(terminate_type(), state()) -> ok.
-callback handle_ack(
    state(),
    ack_ctx(),
    emqx_extsub_types:message_id(),
    emqx_types:message(),
    emqx_extsub_types:ack()
) -> state().
-callback handle_info(state(), info_ctx(), term()) ->
    {ok, state()}
    | {ok, state(), [{emqx_extsub_types:message_id(), emqx_types:message()}]}
    | recreate.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Register and unregister the ExtSub Handlers

-spec init() -> ok.
init() ->
    emqx_utils_ets:new(?TAB, [set, public, named_table, {read_concurrency, true}]).

-spec register(module()) -> ok.
register(CBM) ->
    case ets:insert_new(?TAB, {CBM, true}) of
        true -> ok;
        false -> error({extsub_handler_already_registered, CBM})
    end.

-spec unregister(module()) -> ok.
unregister(CBM) ->
    _ = ets:delete(?TAB, CBM),
    ok.

%% Working with ExtSub Handler implementations

-spec handle_init(init_type(), emqx_types:clientinfo(), emqx_extsub_types:topic_filter()) ->
    {ok, emqx_extsub_types:subscriber_ref(), t()} | ignore.
handle_init(InitType, ClientInfo, TopicFilter) ->
    SubscriberRef = make_ref(),
    Ctx = create_ctx(SubscriberRef, ClientInfo),
    try handle_init(InitType, Ctx, TopicFilter, cbms()) of
        ignore ->
            ignore;
        {CBM, State} ->
            {ok, SubscriberRef, #handler{
                cbm = CBM,
                st = State,
                ctx = Ctx,
                subscriber_ref = SubscriberRef
            }}
    catch
        Class:Reason:StackTrace ->
            ?tp(error, handle_subscribe_error, #{
                class => Class,
                reason => Reason,
                topic_filter => TopicFilter,
                stacktrace => StackTrace
            }),
            ignore
    end.

-spec handle_terminate(terminate_type(), t()) -> ok.
handle_terminate(TerminateType, #handler{cbm = CBM, st = State}) ->
    _ = CBM:handle_terminate(TerminateType, State),
    ok.

-spec handle_ack(t(), ack_ctx(), emqx_types:message(), emqx_extsub_types:ack()) -> t().
handle_ack(#handler{cbm = CBM, st = State} = Handler, AckCtx, Msg, Ack) ->
    MessageId = emqx_message:get_header(?EXTSUB_INTERNAL_MESSAGE_ID, Msg),
    Handler#handler{st = CBM:handle_ack(State, AckCtx, MessageId, Msg, Ack)}.

-spec handle_info(t(), info_ctx(), term()) ->
    {ok, t()} | {ok, t(), [emqx_types:message()]} | recreate.
handle_info(#handler{cbm = CBM, st = State0} = Handler, InfoCtx, Info) ->
    {TimeUs, Result} = timer:tc(CBM, handle_info, [State0, InfoCtx, Info]),
    TimeMs = erlang:convert_time_unit(TimeUs, microsecond, millisecond),
    emqx_extsub_metrics:observe_hist(handle_info_latency_ms, TimeMs),
    case Result of
        {ok, State} ->
            {ok, Handler#handler{st = State}};
        {ok, State, MessagesWithInternalIds} ->
            {ok, Handler#handler{st = State}, to_messages(MessagesWithInternalIds)};
        recreate ->
            recreate
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_init(_InitType, _Ctx, _TopicFilter, []) ->
    ignore;
handle_init(InitType, Ctx, TopicFilter, [CBM | CBMs]) ->
    case CBM:handle_init(InitType, Ctx, TopicFilter) of
        {ok, State} ->
            {CBM, State};
        ignore ->
            handle_init(InitType, Ctx, TopicFilter, CBMs)
    end.

create_ctx(SubscriberRef, ClientInfo) ->
    Pid = self(),
    SendAfter = fun(Interval, Info) ->
        _ = erlang:send_after(Interval, Pid, #info_to_extsub{
            subscriber_ref = SubscriberRef, info = Info
        }),
        ok
    end,
    Send = fun(Info) ->
        _ = erlang:send(Pid, #info_to_extsub{
            subscriber_ref = SubscriberRef, info = Info
        }),
        ok
    end,
    #{
        clientinfo => ClientInfo,
        send_after => SendAfter,
        send => Send
    }.

cbms() ->
    {CBMs, _} = lists:unzip(ets:tab2list(?TAB)),
    CBMs.

to_messages(MessagesWithIds) ->
    lists:map(
        fun({MessageId, Message}) ->
            emqx_message:set_header(?EXTSUB_INTERNAL_MESSAGE_ID, MessageId, Message)
        end,
        MessagesWithIds
    ).
