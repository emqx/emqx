%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Stomp Protocol Processor.
-module(emqx_stomp_protocol).

-include("emqx_stomp.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-logger_header("[Stomp-Proto]").

-import(proplists, [get_value/2, get_value/3]).

%% API
-export([ init/2
        , info/1
        ]).

-export([ received/2
        , send/2
        , shutdown/2
        , timeout/3
        ]).

-record(stomp_proto, {
          peername,
          heartfun,
          sendfun,
          connected = false,
          proto_ver,
          proto_name,
          heart_beats,
          login,
          allow_anonymous,
          default_user,
          subscriptions = [],
          timers :: #{atom() => disable | undefined | reference()}
         }).

-define(TIMER_TABLE, #{
          incoming_timer => incoming,
          outgoing_timer => outgoing
        }).

-type(stomp_proto() :: #stomp_proto{}).

%% @doc Init protocol
init(#{peername := Peername,
       sendfun := SendFun,
       heartfun := HeartFun}, Env) ->
    AllowAnonymous = get_value(allow_anonymous, Env, false),
    DefaultUser = get_value(default_user, Env),
	#stomp_proto{peername = Peername,
                 heartfun = HeartFun,
                 sendfun = SendFun,
                 timers = #{},
                 allow_anonymous = AllowAnonymous,
                 default_user = DefaultUser}.

info(#stomp_proto{connected     = Connected,
                  proto_ver     = ProtoVer,
                  proto_name    = ProtoName,
                  heart_beats   = Heartbeats,
                  login         = Login,
                  subscriptions = Subscriptions}) ->
    [{connected, Connected},
     {proto_ver, ProtoVer},
     {proto_name, ProtoName},
     {heart_beats, Heartbeats},
     {login, Login},
     {subscriptions, Subscriptions}].

-spec(received(stomp_frame(), stomp_proto())
    -> {ok, stomp_proto()}
     | {error, any(), stomp_proto()}
     | {stop, any(), stomp_proto()}).
received(Frame = #stomp_frame{command = <<"STOMP">>}, State) ->
    received(Frame#stomp_frame{command = <<"CONNECT">>}, State);

received(#stomp_frame{command = <<"CONNECT">>, headers = Headers},
         State = #stomp_proto{connected = false, allow_anonymous = AllowAnonymous, default_user = DefaultUser}) ->
    case negotiate_version(header(<<"accept-version">>, Headers)) of
        {ok, Version} ->
            Login = header(<<"login">>, Headers),
            Passc = header(<<"passcode">>, Headers),
            case check_login(Login, Passc, AllowAnonymous, DefaultUser) of
                true ->
                    Heartbeats = parse_heartbeats(header(<<"heart-beat">>, Headers, <<"0,0">>)),
                    NState = start_heartbeart_timer(Heartbeats, State#stomp_proto{connected = true,
                                                                                  proto_ver = Version, login = Login}),
                    send(connected_frame([{<<"version">>, Version},
                                          {<<"heart-beat">>, reverse_heartbeats(Heartbeats)}]), NState);
                false ->
                    send(error_frame(undefined, <<"Login or passcode error!">>), State),
                    {error, login_or_passcode_error, State}
             end;
        {error, Msg} ->
            send(error_frame([{<<"version">>, <<"1.0,1.1,1.2">>},
                              {<<"content-type">>, <<"text/plain">>}], undefined, Msg), State),
            {error, unsupported_version, State}
    end;

received(#stomp_frame{command = <<"CONNECT">>}, State = #stomp_proto{connected = true}) ->
    {error, unexpected_connect, State};

received(#stomp_frame{command = <<"SEND">>, headers = Headers, body = Body}, State) ->
    Topic = header(<<"destination">>, Headers),
    Action = fun(State0) ->
                 maybe_send_receipt(receipt_id(Headers), State0),
                 emqx_broker:publish(
                     make_mqtt_message(Topic, Headers, iolist_to_binary(Body))
                 ),
                 State0
             end,
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, Action(State)};
        TransactionId -> add_action(TransactionId, Action, receipt_id(Headers), State)
    end;

received(#stomp_frame{command = <<"SUBSCRIBE">>, headers = Headers},
            State = #stomp_proto{subscriptions = Subscriptions}) ->
    Id    = header(<<"id">>, Headers),
    Topic = header(<<"destination">>, Headers),
    Ack   = header(<<"ack">>, Headers, <<"auto">>),
    {ok, State1} = case lists:keyfind(Id, 1, Subscriptions) of
                       {Id, Topic, Ack} ->
                           {ok, State};
                       false ->
                           emqx_broker:subscribe(Topic),
                           {ok, State#stomp_proto{subscriptions = [{Id, Topic, Ack}|Subscriptions]}}
                   end,
    maybe_send_receipt(receipt_id(Headers), State1);

received(#stomp_frame{command = <<"UNSUBSCRIBE">>, headers = Headers},
            State = #stomp_proto{subscriptions = Subscriptions}) ->
    Id = header(<<"id">>, Headers),

    {ok, State1} = case lists:keyfind(Id, 1, Subscriptions) of
                       {Id, Topic, _Ack} ->
                           ok = emqx_broker:unsubscribe(Topic),
                           {ok, State#stomp_proto{subscriptions = lists:keydelete(Id, 1, Subscriptions)}};
                       false ->
                           {ok, State}
                   end,
    maybe_send_receipt(receipt_id(Headers), State1);

%% ACK
%% id:12345
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"ACK">>, headers = Headers}, State) ->
    Id = header(<<"id">>, Headers),
    Action = fun(State0) -> 
                 maybe_send_receipt(receipt_id(Headers), State0),
                 ack(Id, State0) 
             end,
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, Action(State)};
        TransactionId -> add_action(TransactionId, Action, receipt_id(Headers), State)
    end;

%% NACK
%% id:12345
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"NACK">>, headers = Headers}, State) ->
    Id = header(<<"id">>, Headers),
    Action = fun(State0) -> 
                 maybe_send_receipt(receipt_id(Headers), State0),
                 nack(Id, State0) 
             end,
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, Action(State)};
        TransactionId -> add_action(TransactionId, Action, receipt_id(Headers), State)
    end;

%% BEGIN
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"BEGIN">>, headers = Headers}, State) ->
    Id        = header(<<"transaction">>, Headers),
    %% self() ! TimeoutMsg
    TimeoutMsg = {transaction, {timeout, Id}},
    case emqx_stomp_transaction:start(Id, TimeoutMsg) of
        {ok, _Transaction} ->
            maybe_send_receipt(receipt_id(Headers), State);
        {error, already_started} ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " already started"]), State)
    end;

%% COMMIT
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"COMMIT">>, headers = Headers}, State) ->
    Id = header(<<"transaction">>, Headers),
    case emqx_stomp_transaction:commit(Id, State) of
        {ok, NState} ->
            maybe_send_receipt(receipt_id(Headers), NState);
        {error, not_found} ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " not found"]), State)
    end;

%% ABORT
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"ABORT">>, headers = Headers}, State) ->
    Id = header(<<"transaction">>, Headers),
    case emqx_stomp_transaction:abort(Id) of
        ok ->
            maybe_send_receipt(receipt_id(Headers), State);
        {error, not_found} ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " not found"]), State)
    end;

received(#stomp_frame{command = <<"DISCONNECT">>, headers = Headers}, State) ->
    maybe_send_receipt(receipt_id(Headers), State),
    {stop, normal, State}.

send(Msg = #message{topic = Topic, headers = Headers, payload = Payload},
     State = #stomp_proto{subscriptions = Subscriptions}) ->
    case lists:keyfind(Topic, 2, Subscriptions) of
        {Id, Topic, Ack} ->
            Headers0 = [{<<"subscription">>, Id},
                        {<<"message-id">>, next_msgid()},
                        {<<"destination">>, Topic},
                        {<<"content-type">>, <<"text/plain">>}],
            Headers1 = case Ack of 
                           _ when Ack =:= <<"client">> orelse Ack =:= <<"client-individual">> -> 
                               Headers0 ++ [{<<"ack">>, next_ackid()}];
                           _ ->
                               Headers0
                       end,
            Frame = #stomp_frame{command = <<"MESSAGE">>,
                                 headers = Headers1 ++ maps:get(stomp_headers, Headers, []),
                                 body = Payload},
            send(Frame, State);
        false ->
            ?LOG(error, "Stomp dropped: ~p", [Msg]),
            {error, dropped, State}
    end;

send(Frame, State = #stomp_proto{sendfun = {Fun, Args}}) ->
    ?LOG(info, "SEND Frame: ~s", [emqx_stomp_frame:format(Frame)]),
    Data = emqx_stomp_frame:serialize(Frame),
    ?LOG(debug, "SEND ~p", [Data]),
    erlang:apply(Fun, [Data] ++ Args),
    {ok, State}.

shutdown(_Reason, _State) ->
    ok.

timeout(_TRef, {incoming, NewVal},
        State = #stomp_proto{heart_beats = HrtBt}) ->
    case emqx_stomp_heartbeat:check(incoming, NewVal, HrtBt) of
        {error, timeout} ->
            {shutdown, heartbeat_timeout, State};
        {ok, NHrtBt} ->
            {ok, reset_timer(incoming_timer, State#stomp_proto{heart_beats = NHrtBt})}
    end;

timeout(_TRef, {outgoing, NewVal},
        State = #stomp_proto{heart_beats = HrtBt,
                             heartfun = {Fun, Args}}) ->
    case emqx_stomp_heartbeat:check(outgoing, NewVal, HrtBt) of
        {error, timeout} ->
            _ = erlang:apply(Fun, Args),
            {ok, State};
        {ok, NHrtBt} ->
            {ok, reset_timer(outgoing_timer, State#stomp_proto{heart_beats = NHrtBt})}
    end.

negotiate_version(undefined) ->
    {ok, <<"1.0">>};
negotiate_version(Accepts) ->
     negotiate_version(?STOMP_VER,
                        lists:reverse(
                          lists:sort(
                            binary:split(Accepts, <<",">>, [global])))).

negotiate_version(Ver, []) ->
    {error, <<"Supported protocol versions < ", Ver/binary>>};
negotiate_version(Ver, [AcceptVer|_]) when Ver >= AcceptVer ->
    {ok, AcceptVer};
negotiate_version(Ver, [_|T]) ->
    negotiate_version(Ver, T).

check_login(undefined, _, AllowAnonymous, _) ->
    AllowAnonymous;
check_login(_, _, _, undefined) ->
    false;
check_login(Login, Passcode, _, DefaultUser) ->
    case {list_to_binary(get_value(login, DefaultUser)),
          list_to_binary(get_value(passcode, DefaultUser))} of
        {Login, Passcode} -> true;
        {_,     _       } -> false
    end.

add_action(Id, Action, ReceiptId, State) ->
    case emqx_stomp_transaction:add(Id, Action) of
        {ok, _}           ->
            {ok, State};
        {error, not_found} ->
            send(error_frame(ReceiptId, ["Transaction ", Id, " not found"]), State)
    end.

maybe_send_receipt(undefined, State) ->
    {ok, State};
maybe_send_receipt(ReceiptId, State) ->
    send(receipt_frame(ReceiptId), State).

ack(_Id, State) -> 
    State.

nack(_Id, State) -> State.

header(Name, Headers) ->
    get_value(Name, Headers).
header(Name, Headers, Val) ->
    get_value(Name, Headers, Val).

connected_frame(Headers) ->
    emqx_stomp_frame:make(<<"CONNECTED">>, Headers).

receipt_frame(ReceiptId) ->
    emqx_stomp_frame:make(<<"RECEIPT">>, [{<<"receipt-id">>, ReceiptId}]).

error_frame(ReceiptId, Msg) ->
    error_frame([{<<"content-type">>, <<"text/plain">>}], ReceiptId, Msg).

error_frame(Headers, undefined, Msg) ->
    emqx_stomp_frame:make(<<"ERROR">>, Headers, Msg);
error_frame(Headers, ReceiptId, Msg) ->
    emqx_stomp_frame:make(<<"ERROR">>, [{<<"receipt-id">>, ReceiptId} | Headers], Msg).

next_msgid() ->
    MsgId = case get(msgid) of
                undefined -> 1;
                I         -> I
            end,
    put(msgid, MsgId + 1), 
    MsgId.

next_ackid() ->
    AckId = case get(ackid) of
                undefined -> 1;
                I         -> I
            end,
    put(ackid, AckId + 1), 
    AckId.

make_mqtt_message(Topic, Headers, Body) ->
    Msg = emqx_message:make(stomp, Topic, Body),
    Headers1 = lists:foldl(fun(Key, Headers0) ->
                               proplists:delete(Key, Headers0)
                           end, Headers, [<<"destination">>, 
                                          <<"content-length">>, 
                                          <<"content-type">>, 
                                          <<"transaction">>,
                                          <<"receipt">>]),
    emqx_message:set_headers(#{stomp_headers => Headers1}, Msg).

receipt_id(Headers) ->
    header(<<"receipt">>, Headers).

%%--------------------------------------------------------------------
%% Heartbeat

parse_heartbeats(Heartbeats) ->
    CxCy = re:split(Heartbeats, <<",">>, [{return, list}]),
    list_to_tuple([list_to_integer(S) || S <- CxCy]).

reverse_heartbeats({Cx, Cy}) ->
    iolist_to_binary(io_lib:format("~w,~w", [Cy, Cx])).

start_heartbeart_timer(Heartbeats, State) ->
    ensure_timer(
      [incoming_timer, outgoing_timer],
      State#stomp_proto{heart_beats = emqx_stomp_heartbeat:init(Heartbeats)}).

%%--------------------------------------------------------------------
%% Timer

ensure_timer([Name], State) ->
    ensure_timer(Name, State);
ensure_timer([Name | Rest], State) ->
    ensure_timer(Rest, ensure_timer(Name, State));

ensure_timer(Name, State = #stomp_proto{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, State),
    case TRef == undefined andalso is_integer(Time) andalso Time > 0 of
        true  -> ensure_timer(Name, Time, State);
        false -> State %% Timer disabled or exists
    end.

ensure_timer(Name, Time, State = #stomp_proto{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_misc:start_timer(Time, Msg),
    State#stomp_proto{timers = Timers#{Name => TRef}}.

reset_timer(Name, State) ->
    ensure_timer(Name, clean_timer(Name, State)).

reset_timer(Name, Time, State) ->
    ensure_timer(Name, Time, clean_timer(Name, State)).

clean_timer(Name, State = #stomp_proto{timers = Timers}) ->
    State#stomp_proto{timers = maps:remove(Name, Timers)}.

interval(incoming_timer, #stomp_proto{heart_beats = HrtBt}) ->
    emqx_stomp_heartbeat:interval(incoming, HrtBt);
interval(outgoing_timer, #stomp_proto{heart_beats = HrtBt}) ->
    emqx_stomp_heartbeat:interval(outgoing, HrtBt).
