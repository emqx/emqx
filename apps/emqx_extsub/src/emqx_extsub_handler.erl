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
    get_module/1,
    message_target/1,
    is_target_alive/1,
    send/2,
    send_after/3,
    monitor/2,
    demonitor/2
]).

-compile({no_auto_import, [monitor/2, demonitor/2]}).

-type state() :: term().

-type subscribe_type() :: subscribe | resume.
-type unsubscribe_type() :: unsubscribe | disconnect.

-opaque message_target() :: {pid(), emqx_extsub_types:handler_ref()}.

-type subscribe_ctx() :: #{
    %% Different info about the enclosing channel
    clientinfo := emqx_types:clientinfo(),
    proto_ver := emqx_types:proto_ver() | undefined,
    subopts := emqx_types:subopts(),
    can_receive_acks := boolean(),

    %% Use send/2 and send_after/3 to send messages to the handler itself.
    %% Use monitor/2 to receive 'DOWN' messages of other processes.
    message_target := message_target(),
    _ => _
}.

-type unsubscribe_ctx() :: #{
    subopts := emqx_types:subopts()
}.

-type info_ctx() :: #{
    desired_message_count := non_neg_integer(),
    delivering_count := non_neg_integer(),
    clientinfo := emqx_types:clientinfo(),
    can_receive_acks := boolean(),
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
    state/0,
    message_target/0
]).

%% Handler callbacks

-callback handle_subscribe(
    subscribe_type(),
    subscribe_ctx(),
    state() | undefined,
    emqx_extsub_types:topic_filter()
) ->
    {ok, state()} | ignore | destroy.
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
    | {destroy, state(), [emqx_extsub_types:topic_filter()]}
    | {destroy, state()}.
-callback handle_info(state(), info_ctx(), term()) ->
    {ok, state()}
    | {ok, state(), [emqx_types:message()]}
    | {recreate, state()}
    | {destroy, state(), [emqx_extsub_types:topic_filter()]}
    | {destroy, state()}.

-optional_callbacks([
    handle_unsubscribe/4,
    handle_terminate/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc "Create a message target for a handler owned by the calling channel.".
-spec message_target(emqx_extsub_types:handler_ref()) -> message_target().
message_target(Ref) ->
    {self(), Ref}.

-doc "Check whether the channel that owns the message target is alive.".
-spec is_target_alive(message_target()) -> boolean().
is_target_alive({Pid, _Ref}) ->
    is_process_alive(Pid).

-doc "Send a message to the channel that owns the handler.".
-spec send(message_target(), term()) -> ok.
send({Pid, Ref}, Info) ->
    _ = erlang:send(Pid, #info_to_extsub{handler_ref = Ref, info = Info}),
    ok.

-doc "Schedule a message for the channel that owns the handler.".
-spec send_after(non_neg_integer(), message_target(), term()) -> reference().
send_after(Interval, {Pid, Ref}, Info) ->
    erlang:send_after(Interval, Pid, #info_to_extsub{handler_ref = Ref, info = Info}).

-doc """
Monitor a process on behalf of the handler.

When the process exits, the handler receives `{'DOWN', MRef, process, Pid, Reason}`
in `handle_info/3`. Only the channel that owns the handler can call this function.
The handler must call `demonitor/2` before it is destroyed, for example in
`handle_terminate/1`. Otherwise the monitor stays until the channel exits.
""".
-spec monitor(message_target(), pid()) -> reference().
monitor({Owner, Ref}, Pid) when Owner =:= self() ->
    erlang:monitor(process, Pid, [{tag, #extsub_down{handler_ref = Ref}}]).

-doc "Remove a monitor created with `monitor/2` and drop its down message if it is already received.".
-spec demonitor(message_target(), reference()) -> ok.
demonitor({Owner, _Ref}, MRef) when Owner =:= self() ->
    true = erlang:demonitor(MRef, [flush]),
    ok.

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
        %% No handler exists yet, so there is nothing to destroy.
        destroy ->
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
    {ok, t()} | ignore | destroy.
subscribe(SubscribeType, SubscribeCtx, #handler{cbm = CBM, st = State0} = Handler, TopicFilter) ->
    case CBM:handle_subscribe(SubscribeType, SubscribeCtx, State0, TopicFilter) of
        ignore ->
            ignore;
        destroy ->
            destroy;
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
    | {destroy, t(), [emqx_extsub_types:topic_filter()]}
    | {destroy, t()}.
delivered(#handler{cbm = CBM, st = State0} = Handler, AckCtx, Msg, Ack) ->
    case CBM:handle_delivered(State0, AckCtx, Msg, Ack) of
        {ok, State} ->
            {ok, Handler#handler{st = State}};
        {destroy, State, TopicFilters} ->
            {destroy, Handler#handler{st = State}, TopicFilters};
        {destroy, State} ->
            {destroy, Handler#handler{st = State}}
    end.

-spec info(t(), info_ctx(), term()) ->
    {ok, t()}
    | {ok, t(), [emqx_types:message()]}
    | {destroy, t(), [emqx_extsub_types:topic_filter()]}
    | {destroy, t()}
    | {recreate, t()}.
info(#handler{cbm = CBM, st = State0} = Handler, InfoCtx, Info) ->
    Result =
        case emqx_extsub:need_observe_latencies() of
            true -> observed_handle_info(CBM, State0, InfoCtx, Info);
            false -> CBM:handle_info(State0, InfoCtx, Info)
        end,
    case Result of
        {ok, State} ->
            {ok, Handler#handler{st = State}};
        {ok, State, Messages} ->
            {ok, Handler#handler{st = State}, Messages};
        {destroy, State, TopicFilters} ->
            {destroy, Handler#handler{st = State}, TopicFilters};
        {destroy, State} ->
            {destroy, Handler#handler{st = State}};
        {recreate, State} ->
            {recreate, Handler#handler{st = State}}
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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

observed_handle_info(CBM, State, InfoCtx, Info) ->
    {TimeMs, Result} = timer:tc(CBM, handle_info, [State, InfoCtx, Info], millisecond),
    emqx_extsub_metrics:observe_hist(handle_info_latency_ms, TimeMs),
    Result.
