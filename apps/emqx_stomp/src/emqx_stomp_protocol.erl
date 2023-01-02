%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-logger_header("[Stomp-Proto]").

-import(proplists, [get_value/2, get_value/3]).

%% API
-export([ init/2
        , info/1
        , info/2
        , stats/1
        ]).

-export([ received/2
        , send/2
        , shutdown/2
        , timeout/3
        ]).

-export([ handle_info/2
        ]).

%% for trans callback
-export([ handle_recv_send_frame/2
        , handle_recv_ack_frame/2
        , handle_recv_nack_frame/2
        ]).

-type stomp_conninfo() :: #{socktype := emqx_types:socktype(),
                            sockname := emqx_types:peername(),
                            peername := emqx_types:peername(),
                            peercert := nossl | undefined | esockd_peercert:peercert(),
                            conn_mod := module(),
                            proto_name => binary(),
                            proto_ver => emqx_types:ver(),
                            clean_start => boolean(),
                            clientid => emqx_types:clientid(),
                            username => emqx_types:username(),
                            conn_props => emqx_types:properties(),
                            connected => boolean(),
                            connected_at => undefined | non_neg_integer(),
                            disconnected_at => non_neg_integer(),
                            keepalive => undefined | 0..16#FFFF,
                            receive_maximum => non_neg_integer(),
                            expiry_interval => non_neg_integer(),
                            atom() => term()
                           }.

-record(pstate, {
          %% Stomp ConnInfo
          conninfo :: stomp_conninfo(),
          %% Stomp ClientInfo
          clientinfo :: emqx_types:clientinfo(),
          %% Stomp Heartbeats
          heart_beats :: maybe(emqx_stomp_hearbeat:heartbeat()),
          %% Stomp Connection State
          connected = false,
          %% Timers
          timers :: #{atom() => disable | undefined | reference()},
          %% Transaction
          transaction :: #{binary() => list()},
          %% Subscriptions
          subscriptions = #{},
          %% Send function
          sendfun :: {function(), list()},
          %% Heartbeat function
          heartfun :: {function(), list()},
          %% Get Socket stat function
          statfun :: {function(), list()},
          %% The confs for the connection
          %% TODO: put these configs into a public mem?
          allow_anonymous :: maybe(boolean()),
          default_user :: maybe(list())
         }).

-define(DEFAULT_SUB_ACK, <<"auto">>).

-define(INCOMING_TIMER_BACKOFF, 1.25).
-define(OUTCOMING_TIMER_BACKOFF, 0.75).

-define(TIMER_TABLE, #{
          incoming_timer => incoming,
          outgoing_timer => outgoing,
          clean_trans_timer => clean_trans
        }).

-define(TRANS_TIMEOUT, 60000).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

-define(STATS_KEYS, [subscriptions_cnt,
                     subscriptions_max,
                     inflight_cnt,
                     inflight_max,
                     mqueue_len,
                     mqueue_max,
                     mqueue_dropped,
                     next_pkt_id,
                     awaiting_rel_cnt,
                     awaiting_rel_max
                    ]).

-type(pstate() :: #pstate{}).

%% @doc Init protocol
init(ConnInfo = #{peername := {PeerHost, _Port},
                  sockname := {_Host, SockPort},
                  statfun  := StatFun,
                  sendfun  := SendFun,
                  heartfun := HeartFun}, Opts) ->

    NConnInfo = default_conninfo(ConnInfo),

    ClientInfo = #{zone => undefined,
                   protocol => stomp,
                   peerhost => PeerHost,
                   sockport => SockPort,
                   clientid => undefined,
                   username => undefined,
                   mountpoint => undefined, %% XXX: not supported now
                   is_bridge => false,
                   is_superuser => false
                  },

    AllowAnonymous = get_value(allow_anonymous, Opts, false),
    DefaultUser = get_value(default_user, Opts),
    #pstate{
       conninfo = NConnInfo,
       clientinfo = ClientInfo,
       heartfun = HeartFun,
       sendfun = SendFun,
       statfun = StatFun,
       timers = #{},
       transaction = #{},
       allow_anonymous = AllowAnonymous,
       default_user = DefaultUser
      }.

default_conninfo(ConnInfo) ->
    NConnInfo = maps:without([sendfun, heartfun], ConnInfo),
    NConnInfo#{
      proto_name => <<"STOMP">>,
      proto_ver => <<"1.2">>,
      clean_start => true,
      clientid => undefined,
      username => undefined,
      conn_props => #{},
      connected => false,
      connected_at => undefined,
      keepalive => undefined,
      receive_maximum => 0,
      expiry_interval => 0
     }.

-spec info(pstate()) -> emqx_types:infos().
info(State) ->
    maps:from_list(info(?INFO_KEYS, State)).

-spec info(list(atom()) | atom(), pstate()) -> term().
info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(conninfo, #pstate{conninfo = ConnInfo}) ->
    ConnInfo;
info(socktype, #pstate{conninfo = ConnInfo}) ->
    maps:get(socktype, ConnInfo, undefined);
info(peername, #pstate{conninfo = ConnInfo}) ->
    maps:get(peername, ConnInfo, undefined);
info(sockname, #pstate{conninfo = ConnInfo}) ->
    maps:get(sockname, ConnInfo, undefined);
info(proto_name, #pstate{conninfo = ConnInfo}) ->
    maps:get(proto_name, ConnInfo, undefined);
info(proto_ver, #pstate{conninfo = ConnInfo}) ->
    maps:get(proto_ver, ConnInfo, undefined);
info(connected_at, #pstate{conninfo = ConnInfo}) ->
    maps:get(connected_at, ConnInfo, undefined);
info(clientinfo, #pstate{clientinfo = ClientInfo}) ->
    ClientInfo;
info(zone, _) ->
    undefined;
info(clientid, #pstate{clientinfo = ClientInfo}) ->
    maps:get(clientid, ClientInfo, undefined);
info(username, #pstate{clientinfo = ClientInfo}) ->
    maps:get(username, ClientInfo, undefined);
info(session, State) ->
    session_info(State);
info(conn_state, #pstate{connected = true}) ->
    connected;
info(conn_state, _) ->
    disconnected;
info(will_msg, _) ->
    undefined.

session_info(#pstate{conninfo = ConnInfo, subscriptions = Subs}) ->
    #{subscriptions => Subs,
      upgrade_qos => false,
      retry_interval => 0,
      await_rel_timeout => 0,
      created_at => maps:get(connected_at, ConnInfo, 0)
     }.

-spec stats(pstate()) -> emqx_types:stats().
stats(#pstate{subscriptions = Subs}) ->
    [{subscriptions_cnt, maps:size(Subs)},
     {subscriptions_max, 0},
     {inflight_cnt, 0},
     {inflight_max, 0},
     {mqueue_len, 0},
     {mqueue_max, 0},
     {mqueue_dropped, 0},
     {next_pkt_id, 0},
     {awaiting_rel_cnt, 0},
     {awaiting_rel_max, 0}].

-spec(received(stomp_frame(), pstate())
    -> {ok, pstate()}
     | {error, any(), pstate()}
     | {stop, any(), pstate()}).
received(Frame = #stomp_frame{command = <<"STOMP">>}, State) ->
    received(Frame#stomp_frame{command = <<"CONNECT">>}, State);

received(#stomp_frame{command = <<"CONNECT">>, headers = Headers},
         State = #pstate{connected = false}) ->
    case negotiate_version(header(<<"accept-version">>, Headers)) of
        {ok, Version} ->
            Login = header(<<"login">>, Headers),
            Passc = header(<<"passcode">>, Headers),
            case check_login(Login, Passc,
                             allow_anonymous(State),
                             default_user(State)
                            ) of
                true ->
                    Heartbeats = parse_heartbeats(
                                   header(<<"heart-beat">>, Headers, <<"0,0">>)),
                    ClientId = emqx_guid:to_base62(emqx_guid:gen()),
                    emqx_logger:set_metadata_clientid(ClientId),
                    ConnInfo = State#pstate.conninfo,
                    ClitInfo = State#pstate.clientinfo,
                    NConnInfo = ConnInfo#{
                                  proto_ver => Version,
                                  clientid => ClientId,
                                  keepalive => element(1, Heartbeats) div 1000,
                                  username => Login
                                 },
                    NClitInfo = ClitInfo#{
                                  clientid => ClientId,
                                  username => Login
                                 },

                    ConnPid = self(),
                    _ = emqx_cm_locker:trans(ClientId, fun(_) ->
                        emqx_cm:discard_session(ClientId),
                        emqx_cm:register_channel(ClientId, ConnPid, NConnInfo)
                    end),
                    NState = start_heartbeart_timer(
                               Heartbeats,
                               State#pstate{
                                 conninfo = NConnInfo,
                                 clientinfo = NClitInfo}
                              ),
                    ConnectedFrame = connected_frame(
                                       [{<<"version">>, Version},
                                        {<<"heart-beat">>, reverse_heartbeats(Heartbeats)}
                                       ]),
                    send(ConnectedFrame, ensure_connected(NState));
                false ->
                    _ = send(error_frame(undefined, <<"Login or passcode error!">>), State),
                    {error, login_or_passcode_error, State}
             end;
        {error, Msg} ->
            _ = send(error_frame([{<<"version">>, <<"1.0,1.1,1.2">>},
                                  {<<"content-type">>, <<"text/plain">>}], undefined, Msg), State),
            {error, unsupported_version, State}
    end;

received(#stomp_frame{command = <<"CONNECT">>}, State = #pstate{connected = true}) ->
    ?LOG(error, "Received CONNECT frame on connected=true state"),
    {error, unexpected_connect, State};

received(Frame = #stomp_frame{command = <<"SEND">>, headers = Headers}, State) ->
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, handle_recv_send_frame(Frame, State)};
        TransactionId ->
            add_action(TransactionId,
                       {fun ?MODULE:handle_recv_send_frame/2, [Frame]},
                       receipt_id(Headers),
                       State
                      )
    end;

received(#stomp_frame{command = <<"SUBSCRIBE">>, headers = Headers},
            State = #pstate{subscriptions = Subs}) ->
    Id    = header(<<"id">>, Headers),
    Topic = header(<<"destination">>, Headers),
    Ack   = header(<<"ack">>, Headers, ?DEFAULT_SUB_ACK),

    case find_sub_by_id(Id, Subs) of
        {Topic, #{sub_props := #{id := Id}}} ->
            ?LOG(info, "Subscription has established: ~s", [Topic]),
            maybe_send_receipt(receipt_id(Headers), State);
        {InuseTopic, #{sub_props := #{id := InuseId}}} ->
            ?LOG(info, "Subscription id ~p inused by topic: ~s, "
                       "request topic: ~s", [InuseId, InuseTopic, Topic]),
            send(error_frame(receipt_id(Headers),
                             ["Request sub-id ", Id, " inused "]), State);
        undefined ->
            case check_acl(subscribe, Topic, State) of
                allow ->
                    ClientInfo = State#pstate.clientinfo,

                    [{TopicFilter, SubOpts}] = parse_topic_filters(
                                                 [{Topic, ?DEFAULT_SUBOPTS}
                                               ]),
                    NSubOpts = SubOpts#{sub_props => #{id => Id, ack => Ack}},
                    _ = run_hooks('client.subscribe',
                                  [ClientInfo, _SubProps = #{}],
                                  [{TopicFilter, NSubOpts}]),
                    NState = do_subscribe(TopicFilter, NSubOpts, State),
                    maybe_send_receipt(receipt_id(Headers), NState)
            end
    end;

received(#stomp_frame{command = <<"UNSUBSCRIBE">>, headers = Headers},
            State = #pstate{subscriptions = Subs, clientinfo = ClientInfo}) ->
    Id = header(<<"id">>, Headers),
    {ok, NState} = case find_sub_by_id(Id, Subs) of
            {Topic, #{sub_props := #{id := Id}}} ->
                _ = run_hooks('client.unsubscribe',
                              [ClientInfo, #{}],
                              [{Topic, #{}}]),
                State1 = do_unsubscribe(Topic, ?DEFAULT_SUBOPTS, State),
                {ok, State1};
            undefined ->
                {ok, State}
        end,
    maybe_send_receipt(receipt_id(Headers), NState);

%% ACK
%% id:12345
%% transaction:tx1
%%
%% ^@
received(Frame = #stomp_frame{command = <<"ACK">>, headers = Headers}, State) ->
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, handle_recv_ack_frame(Frame, State)};
        TransactionId ->
            add_action(TransactionId,
                       {fun ?MODULE:handle_recv_ack_frame/2, [Frame]},
                       receipt_id(Headers),
                       State)
    end;

%% NACK
%% id:12345
%% transaction:tx1
%%
%% ^@
received(Frame = #stomp_frame{command = <<"NACK">>, headers = Headers}, State) ->
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, handle_recv_nack_frame(Frame, State)};
        TransactionId ->
            add_action(TransactionId,
                       {fun ?MODULE:handle_recv_nack_frame/2, [Frame]},
                       receipt_id(Headers),
                       State)
    end;

%% BEGIN
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"BEGIN">>, headers = Headers},
         State = #pstate{transaction = Trans}) ->
    Id = header(<<"transaction">>, Headers),
    case maps:get(Id, Trans, undefined) of
        undefined ->
            Ts = erlang:system_time(millisecond),
            NState = ensure_clean_trans_timer(State#pstate{transaction = Trans#{Id => {Ts, []}}}),
            maybe_send_receipt(receipt_id(Headers), NState);
        _ ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " already started"]), State)
    end;

%% COMMIT
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"COMMIT">>, headers = Headers},
         State = #pstate{transaction = Trans}) ->
    Id = header(<<"transaction">>, Headers),
    case maps:get(Id, Trans, undefined) of
        {_, Actions} ->
            NState = lists:foldr(fun({Func, Args}, S) ->
                erlang:apply(Func, Args ++ [S])
            end, State#pstate{transaction = maps:remove(Id, Trans)}, Actions),
            maybe_send_receipt(receipt_id(Headers), NState);
        _ ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " not found"]), State)
    end;

%% ABORT
%% transaction:tx1
%%
%% ^@
received(#stomp_frame{command = <<"ABORT">>, headers = Headers},
         State = #pstate{transaction = Trans}) ->
    Id = header(<<"transaction">>, Headers),
    case maps:get(Id, Trans, undefined) of
        {_, _Actions} ->
            NState = State#pstate{transaction = maps:remove(Id, Trans)},
            maybe_send_receipt(receipt_id(Headers), NState);
        _ ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " not found"]), State)
    end;

received(#stomp_frame{command = <<"DISCONNECT">>, headers = Headers}, State) ->
    _ = maybe_send_receipt(receipt_id(Headers), State),
    {stop, normal, State}.

send(Msg0 = #message{},
     State = #pstate{clientinfo = ClientInfo, subscriptions = Subs}) ->
    ok = emqx_metrics:inc('messages.delivered'),
    Msg = emqx_hooks:run_fold('message.delivered', [ClientInfo], Msg0),
    #message{topic = Topic,
             headers = Headers,
             payload = Payload} = Msg,
    case find_sub_by_topic(Topic, Subs) of
        {Topic, #{sub_props := #{id := Id, ack := Ack}}} ->
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
        undefined ->
            ?LOG(error, "Stomp dropped: ~p", [Msg]),
            {error, dropped, State}
    end;

send(Frame, State = #pstate{sendfun = {Fun, Args}}) when is_record(Frame, stomp_frame) ->
    erlang:apply(Fun, [Frame] ++ Args),
    {ok, State}.

shutdown(Reason, State = #pstate{connected = true}) ->
    _ = ensure_disconnected(Reason, State),
    ok;
shutdown(_Reason, _State) ->
    ok.

timeout(_TRef, {incoming, NewVal},
        State = #pstate{heart_beats = HrtBt}) ->
    case emqx_stomp_heartbeat:check(incoming, NewVal, HrtBt) of
        {error, timeout} ->
            {shutdown, heartbeat_timeout, State};
        {ok, NHrtBt} ->
            {ok, reset_timer(incoming_timer, State#pstate{heart_beats = NHrtBt})}
    end;

timeout(_TRef, {outgoing, NewVal},
        State = #pstate{heart_beats = HrtBt,
                        statfun = {StatFun, StatArgs},
                        heartfun = {Fun, Args}}) ->
    case emqx_stomp_heartbeat:check(outgoing, NewVal, HrtBt) of
        {error, timeout} ->
            _ = erlang:apply(Fun, Args),
            case erlang:apply(StatFun, [send_oct] ++ StatArgs) of
                {ok, NewVal2} ->
                    NHrtBt = emqx_stomp_heartbeat:reset(outgoing, NewVal2, HrtBt),
                    {ok, reset_timer(outgoing_timer, State#pstate{heart_beats = NHrtBt})};
                {error, Reason} ->
                    {shutdown, {error, {get_stats_error, Reason}}, State}
            end;
        {ok, NHrtBt} ->
            {ok, reset_timer(outgoing_timer, State#pstate{heart_beats = NHrtBt})}
    end;

timeout(_TRef, clean_trans, State = #pstate{transaction = Trans}) ->
    Now = erlang:system_time(millisecond),
    NTrans = maps:filter(fun(_, {Ts, _}) -> Ts + ?TRANS_TIMEOUT < Now end, Trans),
    {ok, ensure_clean_trans_timer(State#pstate{transaction = NTrans})}.


-spec(handle_info(Info :: term(), pstate())
      -> ok | {ok, pstate()} | {shutdown, Reason :: term(), pstate()}).

handle_info({subscribe, TopicFilters}, State) ->
    NState = lists:foldl(
        fun({TopicFilter, SubOpts}, StateAcc = #pstate{subscriptions = Subs}) ->
            NSubOpts = enrich_sub_opts(SubOpts, Subs),
            do_subscribe(TopicFilter, NSubOpts, StateAcc)
        end, State, parse_topic_filters(TopicFilters)),
    {ok, NState};

handle_info({unsubscribe, TopicFilters}, State) ->
    NState = lists:foldl(fun({TopicFilter, SubOpts}, StateAcc) ->
                do_unsubscribe(TopicFilter, SubOpts, StateAcc)
             end, State, parse_topic_filters(TopicFilters)),
    {ok, NState};

handle_info(Info, State) ->
    ?LOG(warning, "Unexpected info ~p", [Info]),
    {ok, State}.

negotiate_version(undefined) ->
    {ok, <<"1.0">>};
negotiate_version(Accepts) ->
     negotiate_version(?STOMP_VER,
                        lists:reverse(
                          lists:sort(
                            binary:split(Accepts, <<",">>, [global])))).

negotiate_version(Ver, []) ->
    {error, <<"Supported protocol versions < ", Ver/binary>>};
negotiate_version(Ver, [AcceptVer | _]) when Ver >= AcceptVer ->
    {ok, AcceptVer};
negotiate_version(Ver, [_ | T]) ->
    negotiate_version(Ver, T).

check_login(Login, _, AllowAnonymous, _)
  when Login == <<>>;
       Login == undefined ->
    AllowAnonymous;
check_login(_, _, _, undefined) ->
    false;
check_login(Login, Passcode, _, DefaultUser) ->
    case {iolist_to_binary(get_value(login, DefaultUser)),
          iolist_to_binary(get_value(passcode, DefaultUser))} of
        {Login, Passcode} -> true;
        {_,     _       } -> false
    end.

add_action(Id, Action, ReceiptId, State = #pstate{transaction = Trans}) ->
    case maps:get(Id, Trans, undefined) of
        {Ts, Actions} ->
            NTrans = Trans#{Id => {Ts, [Action | Actions]}},
            {ok, State#pstate{transaction = NTrans}};
        _ ->
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

make_mqtt_message(Topic, Headers, Body,
                  #pstate{
                     conninfo = #{proto_ver := ProtoVer},
                     clientinfo = #{
                         protocol := Protocol,
                         clientid := ClientId,
                         username := Username,
                         peerhost := PeerHost}}) ->
    Msg = emqx_message:make(
            ClientId, ?QOS_0,
            Topic, Body, #{},
            #{proto_ver => ProtoVer,
              protocol => Protocol,
              username => Username,
              peerhost => PeerHost}),
    Headers1 = lists:foldl(
                 fun(Key, Headers0) ->
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
%% Transaction Handle

handle_recv_send_frame(#stomp_frame{command = <<"SEND">>, headers = Headers, body = Body}, State) ->
    Topic = header(<<"destination">>, Headers),
    case check_acl(publish, Topic, State) of
        allow ->
            _ = maybe_send_receipt(receipt_id(Headers), State),
            _ = emqx_broker:publish(
                make_mqtt_message(Topic, Headers, iolist_to_binary(Body), State)
            ),
            State;
        deny ->
            ErrFrame = error_frame(receipt_id(Headers), <<"Not Authorized">>),
            {ok, NState} = send(ErrFrame, State),
            NState
    end.

handle_recv_ack_frame(#stomp_frame{command = <<"ACK">>, headers = Headers}, State) ->
    Id = header(<<"id">>, Headers),
    _ = maybe_send_receipt(receipt_id(Headers), State),
    ack(Id, State).

handle_recv_nack_frame(#stomp_frame{command = <<"NACK">>, headers = Headers}, State) ->
    Id = header(<<"id">>, Headers),
     _ = maybe_send_receipt(receipt_id(Headers), State),
     nack(Id, State).

ensure_clean_trans_timer(State = #pstate{transaction = Trans}) ->
    case maps:size(Trans) of
        0 -> State;
        _ -> ensure_timer(clean_trans_timer, State)
    end.

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
      State#pstate{heart_beats = emqx_stomp_heartbeat:init(backoff(Heartbeats))}).

backoff({Cx, Cy}) ->
    {erlang:ceil(Cx * ?INCOMING_TIMER_BACKOFF),
     erlang:ceil(Cy * ?OUTCOMING_TIMER_BACKOFF)}.

%%--------------------------------------------------------------------
%% pub & sub helpers

parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

check_acl(PubSub, Topic, #pstate{clientinfo = ClientInfo}) ->
    emqx_access_control:check_acl(ClientInfo, PubSub, Topic).

do_subscribe(TopicFilter, SubOpts,
             State = #pstate{clientinfo = ClientInfo, subscriptions = Subs}) ->
    ClientId = maps:get(clientid, ClientInfo),
    _ = emqx_broker:subscribe(TopicFilter, ClientId),
    NSubOpts = SubOpts#{is_new => true},
    _ = run_hooks('session.subscribed',
                  [ClientInfo, TopicFilter, NSubOpts]),
    send_event_to_self(updated),
    State#pstate{subscriptions = maps:put(TopicFilter, SubOpts, Subs)}.

do_unsubscribe(TopicFilter, SubOpts,
               State = #pstate{clientinfo = ClientInfo, subscriptions = Subs}) ->
    ok = emqx_broker:unsubscribe(TopicFilter),
    _ = run_hooks('session.unsubscribe',
                  [ClientInfo, TopicFilter, SubOpts]),
    send_event_to_self(updated),
    State#pstate{subscriptions = maps:remove(TopicFilter, Subs)}.

find_sub_by_topic(Topic, Subs) ->
    case maps:get(Topic, Subs, undefined) of
        undefined -> undefined;
        SubOpts -> {Topic, SubOpts}
    end.

find_sub_by_id(Id, Subs) ->
    Found = maps:filter(fun(_, SubOpts) ->
               %% FIXME: datatype??
               maps:get(id, maps:get(sub_props, SubOpts, #{}), -1) == Id
            end, Subs),
    case maps:to_list(Found) of
        [] -> undefined;
        [Sub | _] -> Sub
    end.

%% automaticly fill the next sub-id and ack if sub-id is absent
enrich_sub_opts(SubOpts0, Subs) ->
    SubOpts = maps:merge(?DEFAULT_SUBOPTS, SubOpts0),
    SubProps = maps:get(sub_props, SubOpts, #{}),
    SubOpts#{sub_props =>
             maps:merge(#{id => next_sub_id(Subs),
                          ack => ?DEFAULT_SUB_ACK}, SubProps)}.

next_sub_id(Subs) ->
    Ids = maps:fold(fun(_, SubOpts, Acc) ->
        [binary_to_integer(
           maps:get(id, maps:get(sub_props, SubOpts, #{}), <<"0">>)) | Acc]
    end, [], Subs),
    integer_to_binary(lists:max(Ids) + 1).

%%--------------------------------------------------------------------
%% helpers

default_user(#pstate{default_user = DefaultUser}) ->
    DefaultUser.
allow_anonymous(#pstate{allow_anonymous = AllowAnonymous}) ->
    AllowAnonymous.

ensure_connected(State = #pstate{conninfo = ConnInfo,
                                 clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{
                  connected => true,
                  connected_at => erlang:system_time(millisecond)
                 },
    send_event_to_self(connected),
    ok = run_hooks('client.connected', [ClientInfo, NConnInfo]),
    State#pstate{conninfo  = NConnInfo,
                 connected = true
                }.

ensure_disconnected(Reason, State = #pstate{conninfo = ConnInfo, clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.disconnected', [ClientInfo, Reason, NConnInfo]),
    State#pstate{conninfo = NConnInfo, connected = false}.

send_event_to_self(Name) ->
    self() ! {event, Name}, ok.

run_hooks(Name, Args) ->
    emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc) ->
    emqx_hooks:run_fold(Name, Args, Acc).

%%--------------------------------------------------------------------
%% Timer

ensure_timer([Name], State) ->
    ensure_timer(Name, State);
ensure_timer([Name | Rest], State) ->
    ensure_timer(Rest, ensure_timer(Name, State));

ensure_timer(Name, State = #pstate{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, State),
    case TRef == undefined andalso is_integer(Time) andalso Time > 0 of
        true  -> ensure_timer(Name, Time, State);
        false -> State %% Timer disabled or exists
    end.

ensure_timer(Name, Time, State = #pstate{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_misc:start_timer(Time, Msg),
    State#pstate{timers = Timers#{Name => TRef}}.

reset_timer(Name, State) ->
    ensure_timer(Name, clean_timer(Name, State)).

clean_timer(Name, State = #pstate{timers = Timers}) ->
    State#pstate{timers = maps:remove(Name, Timers)}.

interval(incoming_timer, #pstate{heart_beats = HrtBt}) ->
    emqx_stomp_heartbeat:interval(incoming, HrtBt);
interval(outgoing_timer, #pstate{heart_beats = HrtBt}) ->
    emqx_stomp_heartbeat:interval(outgoing, HrtBt);
interval(clean_trans_timer, _) ->
    ?TRANS_TIMEOUT.
