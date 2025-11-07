%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_handler).

-include("emqx_extsub_internal.hrl").

-export([
    init/0,
    register/2,
    unregister/1
]).

-export([
    handle_init/3,
    handle_terminate/2,
    handle_delivered/4,
    handle_info/3,
    get_options/2,
    get_options/3
]).

-type state() :: term().

-type init_type() :: subscribe | resume.
-type terminate_type() :: unsubscribe | disconnect.

-type init_ctx() :: #{
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

-type init_ctx_int() :: #{
    clientinfo := emqx_types:clientinfo(),
    can_receive_acks := boolean()
}.

-record(handler, {
    st :: state(),
    cbm :: module(),
    ctx :: init_ctx(),
    handler_ref :: emqx_extsub_types:handler_ref(),
    options :: emqx_extsub_types:handler_options()
}).

-type t() :: #handler{}.

-export_type([
    t/0,
    init_type/0,
    terminate_type/0,
    init_ctx/0,
    info_ctx/0,
    ack_ctx/0
]).

-define(TAB, ?MODULE).

%% ExtSub Handler behaviour

-callback handle_init(init_type(), init_ctx(), emqx_extsub_types:topic_filter()) ->
    {ok, state()} | ignore.
-callback handle_terminate(terminate_type(), state()) -> ok.
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

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% Register and unregister the ExtSub Handlers

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

%% Handler options

-spec get_options(atom(), t()) -> term().
get_options(Name, Handler) ->
    get_options(Name, Handler, undefined).

-spec get_options(atom(), t(), term()) -> term().
get_options(Name, #handler{options = Options} = _Handler, Default) ->
    maps:get(Name, Options, Default).

%% Working with ExtSub Handler implementations

-spec handle_init(init_type(), init_ctx_int(), emqx_extsub_types:topic_filter()) ->
    {ok, emqx_extsub_types:handler_ref(), t()} | ignore.
handle_init(InitType, Ctx0, TopicFilter) ->
    Ref = make_ref(),
    Ctx = create_ctx(Ref, Ctx0),
    try handle_init(InitType, Ctx, TopicFilter, cbms()) of
        ignore ->
            ignore;
        {CBM, Options, State} ->
            {ok, Ref, #handler{
                cbm = CBM,
                st = State,
                ctx = Ctx,
                handler_ref = Ref,
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

-spec handle_terminate(terminate_type(), t()) -> ok.
handle_terminate(TerminateType, #handler{cbm = CBM, st = State}) ->
    _ = CBM:handle_terminate(TerminateType, State),
    ok.

-spec handle_delivered(t(), ack_ctx(), emqx_types:message(), emqx_extsub_types:ack()) -> t().
handle_delivered(#handler{cbm = CBM, st = State} = Handler, AckCtx, Msg, Ack) ->
    Handler#handler{st = CBM:handle_delivered(State, AckCtx, Msg, Ack)}.

-spec handle_info(t(), info_ctx(), term()) ->
    {ok, t()} | {ok, t(), [emqx_types:message()]} | recreate.
handle_info(#handler{cbm = CBM, st = State0} = Handler, InfoCtx, Info) ->
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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_init(_InitType, _Ctx, _TopicFilter, []) ->
    ignore;
handle_init(InitType, Ctx, TopicFilter, [{CBM, Options} | CBMs]) ->
    case CBM:handle_init(InitType, Ctx, TopicFilter) of
        {ok, State} ->
            {CBM, Options, State};
        ignore ->
            handle_init(InitType, Ctx, TopicFilter, CBMs)
    end.

create_ctx(Ref, Ctx) ->
    Pid = self(),
    SendAfter = fun(Interval, Info) ->
        _ = erlang:send_after(Interval, Pid, #info_to_extsub{
            handler_ref = Ref, info = Info
        }),
        ok
    end,
    Send = fun(Info) ->
        _ = erlang:send(Pid, #info_to_extsub{
            handler_ref = Ref, info = Info
        }),
        ok
    end,
    Ctx#{
        send_after => SendAfter,
        send => Send
    }.

cbms() ->
    ets:tab2list(?TAB).
