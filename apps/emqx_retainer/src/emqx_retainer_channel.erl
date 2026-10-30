%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_channel).

-moduledoc """
Retained-message delivery in the channel process.

On SUBSCRIBE, the module starts a retained read for the topic filter:
* An exact topic without the read limiter is read in the channel. The channel sends the
  result to itself.
* Other topic filters request one batch from a dispatcher worker.

The channel receives each batch as a `#retained_batch{}` message. The `client.handle_info`
hook returns the messages to the session. The channel keeps state for a read only until its
batch arrives.

The module passes the rest of the delivery to an ExtSub handler
(`emqx_retainer_extsub_handler`) with `emqx_extsub:handover/6` only when:
* more batches follow;
* the batch has more messages than the session can send now;
* the session refuses a message (the `message.nack` hook);
* no dispatcher worker serves the request.
""".

-include("emqx_retainer.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([register_hooks/0, unregister_hooks/0]).

%% Hooks
-export([
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_session_disconnected/2,
    on_session_save_subopts/2,
    on_client_handle_info/3,
    on_message_nack/2
]).

%% Internal exports (for `emqx_retainer_extsub_handler')
-export([need_retained/1, filter_delivery/1]).

-define(READS_PD_KEY, emqx_retainer_reads).
-define(MONITORS_PD_KEY, emqx_retainer_monitors).
-define(HANDLER, emqx_retainer_extsub_handler).
-define(HEADER_SUB_TOPIC, sub_topic).
%% Marks a message that a read delivered without ExtSub
-define(HEADER_READ, retainer_read).
%% Tag of the down message of a dispatcher worker
-define(DOWN_TAG, emqx_retainer_dispatcher_down).

%% A read that waits for its batch
-record(read, {
    request_ref :: reference(),
    %% The dispatcher worker that serves the request, `undefined' for a read in the channel
    worker :: pid() | undefined
}).

%% Hand over a read that no dispatcher worker serves
-record(retry_read, {
    topic_filter :: emqx_types:topic(),
    request_ref :: reference()
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:put('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_RETAINER),
    ok = emqx_hooks:put(
        'session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_RETAINER
    ),
    ok = emqx_hooks:put(
        'session.disconnected', {?MODULE, on_session_disconnected, []}, ?HP_RETAINER
    ),
    ok = emqx_hooks:put(
        'session.save_subopts', {?MODULE, on_session_save_subopts, []}, ?HP_RETAINER
    ),
    ok = emqx_hooks:put('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_RETAINER),
    ok = emqx_hooks:put('message.nack', {?MODULE, on_message_nack, []}, ?HP_RETAINER).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    ok = emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    ok = emqx_hooks:del('session.disconnected', {?MODULE, on_session_disconnected}),
    ok = emqx_hooks:del('session.save_subopts', {?MODULE, on_session_save_subopts}),
    ok = emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}),
    ok = emqx_hooks:del('message.nack', {?MODULE, on_message_nack}).

-doc "Retain Handling 0 sends retained messages on each SUBSCRIBE, 1 only for a new subscription.".
-spec need_retained(emqx_types:subopts()) -> boolean().
need_retained(#{rh := RH} = SubOpts) ->
    RH == 0 orelse (RH == 1 andalso maps:get(is_new, SubOpts, true)).

-doc "Drop the messages of banned publishers.".
-spec filter_delivery([emqx_types:message()]) -> [emqx_types:message()].
filter_delivery(Messages) ->
    lists:filter(fun check_clientid_banned/1, Messages).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

on_session_subscribed(#{protocol := mqtt}, TopicFilter, SubOpts) when is_binary(TopicFilter) ->
    case need_retained(SubOpts) of
        true -> start_read(TopicFilter);
        false -> ok
    end;
on_session_subscribed(_ClientInfo, _TopicFilter, _SubOpts) ->
    ok.

on_session_unsubscribed(_ClientInfo, TopicFilter, _SubOpts) ->
    stop_read(TopicFilter).

on_session_disconnected(_ClientInfo, _SessionInfo) ->
    maps:foreach(fun(_Worker, {MRef, _N}) -> erlang:demonitor(MRef, [flush]) end, get_monitors()),
    _ = erase(?MONITORS_PD_KEY),
    _ = erase(?READS_PD_KEY),
    ok.

%% A takeover or disconnect saves the subscriptions. The reads of the channel stop there.
on_session_save_subopts(#{topic_filter := TopicFilter}, _SubOpts) ->
    stop_read(TopicFilter).

on_client_handle_info(ClientInfo, #retained_batch{} = Batch, Acc0) ->
    #retained_batch{topic_filter = TopicFilter, request_ref = Ref} = Batch,
    case get_reads() of
        #{TopicFilter := #read{request_ref = Ref}} ->
            ok = stop_read(TopicFilter),
            {stop, handle_batch(ClientInfo, Batch, Acc0)};
        _ ->
            {stop, Acc0}
    end;
on_client_handle_info(ClientInfo, {?DOWN_TAG, _MRef, process, Worker, _Reason}, Acc) ->
    ok = handle_worker_down(ClientInfo, Worker),
    {stop, Acc};
on_client_handle_info(ClientInfo, #retry_read{} = Retry, Acc) ->
    #retry_read{topic_filter = TopicFilter, request_ref = Ref} = Retry,
    case get_reads() of
        #{TopicFilter := #read{request_ref = Ref}} ->
            ok = stop_read(TopicFilter),
            ok = handover(ClientInfo, session_info_fn(), TopicFilter, {cursor, undefined}, []);
        _ ->
            ok
    end,
    {stop, Acc};
on_client_handle_info(_ClientInfo, _Info, _Acc) ->
    ok.

%% The session refuses a message of a batch that the `client.handle_info' hook returned in the
%% same channel turn: pass it to ExtSub, which delivers it later.
%% A channel in takeover or disconnected state receives no batch: its reads are stopped.
on_message_nack(#message{headers = #{?HEADER_READ := true} = Headers} = Msg0, false) ->
    #{?HEADER_SUB_TOPIC := TopicFilter} = Headers,
    Msg = emqx_message:remove_header(?HEADER_READ, Msg0),
    #{clientinfo := ClientInfo, session_info_fn := SessionInfoFn} =
        emqx_hooks:context('message.nack'),
    ok = handover(ClientInfo, SessionInfoFn, TopicFilter, done, [Msg]),
    {stop, true};
on_message_nack(_Msg, _Nacked) ->
    ok.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

start_read(TopicFilter) ->
    ok = stop_read(TopicFilter),
    Read = #read{request_ref = make_ref()},
    Words = emqx_topic:words(TopicFilter),
    case need_dispatcher(Words) of
        true -> request(TopicFilter, Words, Read);
        false -> read_exact(TopicFilter, Read)
    end.

need_dispatcher(Words) ->
    emqx_topic:wildcard(Words) orelse
        emqx_config:get([retainer, flow_control, batch_deliver_limiter]) =/= infinity.

%% Read an exact topic in the channel and send the result to the channel itself.
read_exact(TopicFilter, #read{request_ref = Ref} = Read) ->
    case emqx_retainer:read_message(TopicFilter) of
        {ok, []} ->
            ok;
        {ok, Messages} ->
            _ = erlang:send(self(), #retained_batch{
                topic_filter = TopicFilter, request_ref = Ref, messages = Messages, cursor = done
            }),
            put_read(TopicFilter, Read)
    end.

request(TopicFilter, Words, #read{request_ref = Ref} = Read) ->
    case worker() of
        undefined ->
            %% ExtSub runs the subscribe hooks after this one, so hand over in the next turn.
            _ = erlang:send(self(), #retry_read{topic_filter = TopicFilter, request_ref = Ref}),
            put_read(TopicFilter, Read);
        Worker ->
            ok = monitor_worker(Worker),
            ok = emqx_retainer_dispatcher:fetch(Worker, self(), Ref, TopicFilter, Words, undefined),
            put_read(TopicFilter, Read#read{worker = Worker})
    end.

worker() ->
    try emqx_retainer_dispatcher:worker() of
        Worker when is_pid(Worker) -> Worker;
        false -> undefined
    catch
        error:badarg -> undefined
    end.

stop_read(TopicFilter) ->
    case get_reads() of
        #{TopicFilter := #read{worker = Worker}} = Reads ->
            ok = release_worker(Worker),
            put_reads(maps:remove(TopicFilter, Reads));
        _ ->
            ok
    end.

%% Deliver what the session can send now and hand over the rest.
handle_batch(ClientInfo, Batch, #{deliver := Delivers} = Acc) ->
    #retained_batch{topic_filter = TopicFilter, messages = Messages0, cursor = Cursor} = Batch,
    Messages = filter_delivery(Messages0),
    SessionInfoFn = session_info_fn(),
    {Now, Rest} = split(room(SessionInfoFn), Messages),
    case {Rest, Cursor} of
        {[], done} ->
            ok;
        {_, done} ->
            ok = handover(ClientInfo, SessionInfoFn, TopicFilter, done, enrich(TopicFilter, Rest));
        _ ->
            ok = handover(
                ClientInfo, SessionInfoFn, TopicFilter, {cursor, Cursor}, enrich(TopicFilter, Rest)
            )
    end,
    Acc#{deliver := delivers(TopicFilter, Now) ++ Delivers}.

session_info_fn() ->
    #{session_info_fn := SessionInfoFn} = emqx_hooks:context('client.handle_info'),
    SessionInfoFn.

%% Send at most as many messages as ExtSub would send to the session at once.
room(SessionInfoFn) ->
    MaxUnacked = emqx_extsub:max_unacked(),
    case SessionInfoFn(inflight_max) of
        0 -> MaxUnacked;
        InflightMax -> max(MaxUnacked, InflightMax - SessionInfoFn(inflight_cnt))
    end.

split(Room, Messages) when length(Messages) =< Room ->
    {Messages, []};
split(Room, Messages) ->
    lists:split(Room, Messages).

%% The subscription options come from the session. Without the subscription the messages
%% are dropped.
handover(ClientInfo, SessionInfoFn, TopicFilter, Cursor, Messages) ->
    ?tp(retainer_handover, #{
        topic_filter => TopicFilter, cursor => Cursor, count => length(Messages)
    }),
    Result =
        case SessionInfoFn(subscriptions) of
            #{TopicFilter := SubOpts} ->
                emqx_extsub:handover(ClientInfo, ?HANDLER, TopicFilter, SubOpts, Cursor, Messages);
            #{} ->
                ignore
        end,
    case Result of
        ok ->
            ok;
        ignore ->
            ?tp(retainer_handover_ignored, #{topic_filter => TopicFilter}),
            ok
    end.

delivers(TopicFilter, Messages) ->
    Headers = #{?HEADER_SUB_TOPIC => TopicFilter, ?HEADER_READ => true},
    [
        {deliver, emqx_message:topic(Msg), emqx_message:set_headers(Headers, Msg)}
     || Msg <- Messages
    ].

enrich(TopicFilter, Messages) ->
    [emqx_message:set_header(?HEADER_SUB_TOPIC, TopicFilter, Msg) || Msg <- Messages].

handle_worker_down(ClientInfo, Worker) ->
    put_monitors(maps:remove(Worker, get_monitors())),
    maps:foreach(
        fun
            (TopicFilter, #read{worker = W}) when W =:= Worker ->
                ok = put_reads(maps:remove(TopicFilter, get_reads())),
                ok = handover(ClientInfo, session_info_fn(), TopicFilter, {cursor, undefined}, []);
            (_TopicFilter, _Read) ->
                ok
        end,
        get_reads()
    ).

%% A channel sends all its requests to one worker, so one monitor per worker is enough.
monitor_worker(Worker) ->
    Monitors = get_monitors(),
    case Monitors of
        #{Worker := {MRef, N}} ->
            put_monitors(Monitors#{Worker := {MRef, N + 1}});
        _ ->
            MRef = erlang:monitor(process, Worker, [{tag, ?DOWN_TAG}]),
            put_monitors(Monitors#{Worker => {MRef, 1}})
    end.

release_worker(undefined) ->
    ok;
release_worker(Worker) ->
    case get_monitors() of
        #{Worker := {MRef, 1}} = Monitors ->
            true = erlang:demonitor(MRef, [flush]),
            put_monitors(maps:remove(Worker, Monitors));
        #{Worker := {MRef, N}} = Monitors ->
            put_monitors(Monitors#{Worker := {MRef, N - 1}})
    end.

check_clientid_banned(Msg) ->
    case emqx_banned:check_clientid(Msg#message.from) of
        false ->
            true;
        true ->
            ?tp(debug, ignore_retained_message_due_to_banned, #{
                reason => publisher_client_banned,
                clientid => Msg#message.from
            }),
            false
    end.

get_reads() ->
    case get(?READS_PD_KEY) of
        undefined -> #{};
        Reads -> Reads
    end.

%% Erase the key when no read waits, so that a channel without reads keeps no state.
put_reads(Reads) when map_size(Reads) =:= 0 ->
    _ = erase(?READS_PD_KEY),
    ok;
put_reads(Reads) ->
    _ = put(?READS_PD_KEY, Reads),
    ok.

put_read(TopicFilter, Read) ->
    put_reads(maps:put(TopicFilter, Read, get_reads())).

get_monitors() ->
    case get(?MONITORS_PD_KEY) of
        undefined -> #{};
        Monitors -> Monitors
    end.

put_monitors(Monitors) when map_size(Monitors) =:= 0 ->
    _ = erase(?MONITORS_PD_KEY),
    ok;
put_monitors(Monitors) ->
    _ = put(?MONITORS_PD_KEY, Monitors),
    ok.
