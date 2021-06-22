%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp_channel).

-include("src/stomp/include/emqx_stomp.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[Stomp-Proto]").

-import(proplists, [get_value/2, get_value/3]).

%% API
-export([ info/1
        , info/2
        , stats/1
        ]).

-export([ init/2
        , handle_in/2
        , handle_out/3
        , set_conn_state/2
        ]).

-export([ handle_call/2
        , handle_info/2
        ]).

-export([ send/2
        , shutdown/2
        , timeout/3
        ]).

%% for trans callback
-export([ handle_recv_send_frame/2
        , handle_recv_ack_frame/2
        , handle_recv_nack_frame/2
        ]).

-record(channel, {
          %% Context
          ctx           :: emqx_gateway_ctx:context(),
          %% Stomp Connection Info
          conninfo      :: emqx_types:conninfo(),
          %% Stomp Client Info
          clientinfo    :: emqx_types:clientinfo(),
          %% ClientInfo override specs
          clientinfo_override :: map(),
          %% Connection Channel
          conn_state    :: idle,
          %% Heartbeat
          heartbeat     :: emqx_stomp_heartbeat:heartbeat(),
          %connected = false,
          %proto_ver,
          %proto_name,
          %login,
          subscriptions = [],
          timers :: #{atom() => disable | undefined | reference()},
          transaction :: #{binary() => list()},
          %% Function ref
          heartfun,
          sendfun
         }).

-type(channel() :: #channel{}).

-type(conn_state() :: idle | connecting | connected | disconnected).

-type(reply() :: {outgoing, stomp_frame()}
               | {outgoing, [stomp_frame()]}
               | {event, conn_state()|updated}
               | {close, Reason :: atom()}).

-type(replies() :: emqx_stomp_frame:packet() | reply() | [reply()]).

-define(TIMER_TABLE, #{
          incoming_timer => incoming,
          outgoing_timer => outgoing,
          clean_trans_timer => clean_trans
        }).

-define(TRANS_TIMEOUT, 60000).

-define(DEFAULT_OVERRIDE,
        #{ username => <<"${Packet.headers.login}">>
         , clientid => <<"${Packet.headers.login}">>
         , password => <<"${Packet.headers.passcode}">>
         }).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

%% @doc Init protocol
init(ConnInfo0 = #{peername := {PeerHost, _},
                   sockname := {_, SockPort},
                   sendfun  := SendFun,
                   heartfun := HeartFun}, Option) ->

    ConnInfo = maps:without([sendfun, heartfun], ConnInfo0),
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Option, undefined),
    ClientInfo = setting_peercert_infos(
                   Peercert,
                   #{ zone => undefined
                    , protocol => stomp
                    , peerhost => PeerHost
                    , sockport => SockPort
                    , clientid => undefined
                    , username => undefined
                    , is_bridge => false
                    , is_superuser => false
                    , mountpoint => Mountpoint
                    }
                  ),

    Ctx = maps:get(ctx, Option),
    Override = maps:merge(?DEFAULT_OVERRIDE,
                          maps:get(clientinfo_override, Option, #{})
                         ),
	#channel{ ctx = Ctx
            , conninfo = ConnInfo
            , clientinfo = ClientInfo
            , clientinfo_override = Override
            , heartfun = HeartFun
            , sendfun = SendFun
            , timers = #{}
            , transaction = #{}
            }.

setting_peercert_infos(NoSSL, ClientInfo)
  when NoSSL =:= nossl;
       NoSSL =:= undefined ->
    ClientInfo;
setting_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert),
                esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec(info(list(atom())|atom(), channel()) -> term()).
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];

info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;

info(gwid, #channel{ctx = #{gwid := GwId}}) ->
    %% XXX: emqx_gateway_ctx:gatewayid(GwId)
    GwId.

stats(_Channel) ->
    %% TODO:
    [].

set_conn_state(ConnState, Channel) ->
    Channel#channel{conn_state = ConnState}.

enrich_conninfo(_Packet,
                Channel = #channel{conninfo = ConnInfo}) ->
    %% XXX: How enrich more infos?
    NConnInfo = ConnInfo#{ proto_name => <<"STOMP">>
                         , proto_ver => undefined
                         , clean_start => true
                         },
    {ok, Channel#channel{conninfo = NConnInfo}}.

run_conn_hooks(Packet, Channel = #channel{conninfo = ConnInfo}) ->
    %% XXX: Assign headers of Packet to ConnProps
    ConnProps = #{},
    case run_hooks('client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps ->
            {ok, Packet, Channel}
    end.

negotiate_version(#stomp_frame{headers = Headers},
                  Channel = #channel{conninfo = ConnInfo}) ->
    %% XXX:
    case do_negotiate_version(header(<<"accept-version">>, Headers)) of
        {ok, Version} ->
            {ok, Channel#channel{conninfo = ConnInfo#{proto_ver => Version}}};
        {error, Reason}->
            {error, Reason}
    end.

enrich_clientinfo(Packet,
                  Channel = #channel{
                               conninfo = ConnInfo,
                               clientinfo = ClientInfo0,
                               clientinfo_override = Override}) ->
    ClientInfo = write_clientinfo(
                   feedvar(Override, Packet, ConnInfo, ClientInfo0),
                   ClientInfo0
                  ),
    {ok, NPacket, NClientInfo} = emqx_misc:pipeline(
                                   [ fun maybe_assign_clientid/2
                                   , fun parse_heartbeat/2
                                   %% FIXME: CALL After authentication successfully
                                   , fun fix_mountpoint/2
                                   ], Packet, ClientInfo
                                  ),

    {ok, NPacket, Channel#channel{clientinfo = NClientInfo}}.

feedvar(Override, Packet, ConnInfo, ClientInfo) ->
    Envs = #{ 'ConnInfo' => ConnInfo
            , 'ClientInfo' => ClientInfo
            , 'Packet' => connect_packet_to_map(Packet)
            },
    maps:map(fun(_K, V) ->
        Tokens = emqx_rule_utils:preproc_tmpl(V),
        emqx_rule_utils:proc_tmpl(Tokens, Envs)
    end, Override).

connect_packet_to_map(#stomp_frame{headers = Headers}) ->
    #{headers => maps:from_list(Headers)}.

write_clientinfo(Override, ClientInfo) ->
    Override1 = maps:with([username, password, clientid], Override),
    maps:merge(ClientInfo, Override1).

maybe_assign_clientid(_Packet, ClientInfo = #{clientid := undefined}) ->
    {ok, ClientInfo#{clientid => emqx_guid:to_base62(emqx_guid:gen())}};

maybe_assign_clientid(_Packet, ClientInfo) ->
    {ok, ClientInfo}.

parse_heartbeat(#stomp_frame{headers = Headers}, ClientInfo) ->
    Heartbeat0 = header(<<"heart-beat">>, Headers, <<"0,0">>),
    CxCy = re:split(Heartbeat0, <<",">>, [{return, list}]),
    Heartbeat = list_to_tuple([list_to_integer(S) || S <- CxCy]),
    {ok, ClientInfo#{heartbeat => Heartbeat}}.

fix_mountpoint(_Packet, #{mountpoint := undefined}) -> ok;
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := MountPoint}) ->
    %% XXX: Enrich the varibale replacement????
    %%      i.e: ${ClientInfo.auth_result.productKey}
    MountPoint1 = emqx_mountpoint:replvar(MountPoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := MountPoint1}}.

set_log_meta(_Packet, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

auth_connect(_Packet, Channel = #channel{ctx = Ctx,
                                         clientinfo = ClientInfo}) ->
    #{clientid := ClientId,
      username := Username} = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~0p",
                          [ClientId, Username, Reason]),
            {error, Reason}
    end.

ensure_connected(Channel = #channel{conninfo = ConnInfo,
                                 clientinfo = ClientInfo
                                }) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.connected', [ClientInfo, NConnInfo]),
    Channel#channel{conninfo = NConnInfo,
                 conn_state = connected
                }.

process_connect(Channel = #channel{
                           ctx = Ctx,
                           conninfo = ConnInfo,
                           clientinfo = ClientInfo
                          }) ->
    SessFun = fun(_,_) -> #{} end,
    case emqx_gateway_ctx:open_session(
           Ctx,
           false,
           ClientInfo,
           ConnInfo,
           SessFun
          ) of
        {ok, _Sess} -> %% The stomp protocol doesn't have session
            #{proto_ver := Version,
              heartbeat := Heartbeat} = ClientInfo,
            Headers = [{<<"version">>, Version},
                       {<<"heart-beat">>, reverse_heartbeats(Heartbeat)}],
            handle_out(connected, Headers, Channel);
        {error, Reason} ->
            ?LOG(error, "Failed to open session du to ~p", [Reason]),
            Headers = [{<<"version">>, <<"1.0,1.1,1.2">>},
                       {<<"content-type">>, <<"text/plain">>}],
            handle_out(connerr, {Headers, undefined, <<"Not Authenticated">>}, Channel)
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

%% TODO: Return the packets instead of sendfun
-spec(handle_in(stomp_frame(), channel())
    -> {ok, channel()}
     | {error, any(), channel()}
     | {stop, any(), channel()}).
handle_in(Frame = #stomp_frame{command = <<"STOMP">>}, Channel) ->
    handle_in(Frame#stomp_frame{command = <<"CONNECT">>}, Channel);

handle_in(#stomp_frame{command = <<"CONNECT">>}, Channel = #channel{conn_state = connected}) ->
    {error, unexpected_connect, Channel};

handle_in(Packet = #stomp_frame{command = <<"CONNECT">>}, Channel) ->
    case emqx_misc:pipeline(
           [ fun enrich_conninfo/2
           , fun run_conn_hooks/2
           , fun negotiate_version/2
           , fun enrich_clientinfo/2
           , fun set_log_meta/2
           %% FIXME: How to implement the banned in the gateway instance?
           %, fun check_banned/2
           , fun auth_connect/2
           ], Packet, Channel#channel{conn_state = connecting}) of
        {ok, _NPacket, NChannel} ->
            process_connect(ensure_connected(NChannel));
        {error, ReasonCode, NChannel} ->
            handle_out(connerr, {[], undefined, io_lib:format("Login Failed: ~0p", [ReasonCode])}, NChannel)
    end;

handle_in(Frame = #stomp_frame{command = <<"SEND">>, headers = Headers}, Channel) ->
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, handle_recv_send_frame(Frame, Channel)};
        TransactionId -> add_action(TransactionId, {fun ?MODULE:handle_recv_send_frame/2, [Frame]}, receipt_id(Headers), Channel)
    end;

handle_in(#stomp_frame{command = <<"SUBSCRIBE">>, headers = Headers},
            Channel = #channel{subscriptions = Subscriptions}) ->
    Id    = header(<<"id">>, Headers),
    Topic = header(<<"destination">>, Headers),
    Ack   = header(<<"ack">>, Headers, <<"auto">>),
    {ok, Channel1} = case lists:keyfind(Id, 1, Subscriptions) of
                       {Id, Topic, Ack} ->
                           {ok, Channel};
                       false ->
                           emqx_broker:subscribe(Topic),
                           {ok, Channel#channel{subscriptions = [{Id, Topic, Ack}|Subscriptions]}}
                   end,
    maybe_send_receipt(receipt_id(Headers), Channel1);

handle_in(#stomp_frame{command = <<"UNSUBSCRIBE">>, headers = Headers},
            Channel = #channel{subscriptions = Subscriptions}) ->
    Id = header(<<"id">>, Headers),

    {ok, Channel1} = case lists:keyfind(Id, 1, Subscriptions) of
                       {Id, Topic, _Ack} ->
                           ok = emqx_broker:unsubscribe(Topic),
                           {ok, Channel#channel{subscriptions = lists:keydelete(Id, 1, Subscriptions)}};
                       false ->
                           {ok, Channel}
                   end,
    maybe_send_receipt(receipt_id(Headers), Channel1);

%% ACK
%% id:12345
%% transaction:tx1
%%
%% ^@
handle_in(Frame = #stomp_frame{command = <<"ACK">>, headers = Headers}, Channel) ->
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, handle_recv_ack_frame(Frame, Channel)};
        TransactionId -> add_action(TransactionId, {fun ?MODULE:handle_recv_ack_frame/2, [Frame]}, receipt_id(Headers), Channel)
    end;

%% NACK
%% id:12345
%% transaction:tx1
%%
%% ^@
handle_in(Frame = #stomp_frame{command = <<"NACK">>, headers = Headers}, Channel) ->
    case header(<<"transaction">>, Headers) of
        undefined     -> {ok, handle_recv_nack_frame(Frame, Channel)};
        TransactionId -> add_action(TransactionId, {fun ?MODULE:handle_recv_nack_frame/2, [Frame]}, receipt_id(Headers), Channel)
    end;

%% BEGIN
%% transaction:tx1
%%
%% ^@
handle_in(#stomp_frame{command = <<"BEGIN">>, headers = Headers},
         Channel = #channel{transaction = Trans}) ->
    Id = header(<<"transaction">>, Headers),
    case maps:get(Id, Trans, undefined) of
        undefined ->
            Ts = erlang:system_time(millisecond),
            NChannel = ensure_clean_trans_timer(Channel#channel{transaction = Trans#{Id => {Ts, []}}}),
            maybe_send_receipt(receipt_id(Headers), NChannel);
        _ ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " already started"]), Channel)
    end;

%% COMMIT
%% transaction:tx1
%%
%% ^@
handle_in(#stomp_frame{command = <<"COMMIT">>, headers = Headers},
         Channel = #channel{transaction = Trans}) ->
    Id = header(<<"transaction">>, Headers),
    case maps:get(Id, Trans, undefined) of
        {_, Actions} ->
            NChannel = lists:foldr(fun({Func, Args}, S) ->
                erlang:apply(Func, Args ++ [S])
            end, Channel#channel{transaction = maps:remove(Id, Trans)}, Actions),
            maybe_send_receipt(receipt_id(Headers), NChannel);
        _ ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " not found"]), Channel)
    end;

%% ABORT
%% transaction:tx1
%%
%% ^@
handle_in(#stomp_frame{command = <<"ABORT">>, headers = Headers},
         Channel = #channel{transaction = Trans}) ->
    Id = header(<<"transaction">>, Headers),
    case maps:get(Id, Trans, undefined) of
        {_, _Actions} ->
            NChannel = Channel#channel{transaction = maps:remove(Id, Trans)},
            maybe_send_receipt(receipt_id(Headers), NChannel);
        _ ->
            send(error_frame(receipt_id(Headers), ["Transaction ", Id, " not found"]), Channel)
    end;

handle_in(#stomp_frame{command = <<"DISCONNECT">>, headers = Headers}, Channel) ->
    _ = maybe_send_receipt(receipt_id(Headers), Channel),
    {stop, normal, Channel}.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec(handle_out(atom(), term(), channel())
      -> {ok, channel()}
       | {ok, replies(), channel()}
       | {shutdown, Reason :: term(), channel()}
       | {shutdown, Reason :: term(), replies(), channel()}).

handle_out(connerr, {Headers, ReceiptId, ErrMsg}, Channel) ->
    Frame = error_frame(Headers, ReceiptId, ErrMsg),
    shutdown(ErrMsg, Frame, Channel);

handle_out(error, {ReceiptId, ErrMsg}, Channel) ->
    Frame = error_frame(ReceiptId, ErrMsg),
    %% FIXME: Conver SendFunc to a packets array
    send(Frame, Channel);

handle_out(connected, Headers, Channel = #channel{conninfo = ConnInfo}) ->
    %% XXX: connection_accepted is not defined by stomp protocol
    _ = run_hooks('client.connack', [ConnInfo, connection_accepted, []]),
    Replies = [{outgoing, connected_frame(Headers)},
               {event, connected}
              ],
    {ok, Replies, ensure_heartbeart_timer(Channel)};

handle_out(receipt, ReceiptId, Channel) ->
    Frame = receipt_frame(ReceiptId),
    send(Frame, Channel).

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec(handle_call(Req :: term(), channel())
      -> {reply, Reply :: term(), channel()}
       | {shutdown, Reason :: term(), Reply :: term(), channel()}
       | {shutdown, Reason :: term(), Reply :: term(), emqx_types:packet(), channel()}).
handle_call(kick, Channel) ->
    Channel1 = ensure_disconnected(kicked, Channel),
    shutdown_and_reply(kicked, ok, Channel1);

handle_call(discard, Channel) ->
    shutdown_and_reply(discarded, ok, Channel);

%% XXX: No Session Takeover
%handle_call({takeover, 'begin'}, Channel = #channel{session = Session}) ->
%    reply(Session, Channel#channel{takeover = true});
%
%handle_call({takeover, 'end'}, Channel = #channel{session  = Session,
%                                                  pendings = Pendings}) ->
%    ok = emqx_session:takeover(Session),
%    %% TODO: Should not drain deliver here (side effect)
%    Delivers = emqx_misc:drain_deliver(),
%    AllPendings = lists:append(Delivers, Pendings),
%    shutdown_and_reply(takeovered, AllPendings, Channel);

handle_call(list_acl_cache, Channel) ->
    {reply, emqx_acl_cache:list_acl_cache(), Channel};

%% XXX: No Quota Now
% handle_call({quota, Policy}, Channel) ->
%     Zone = info(zone, Channel),
%     Quota = emqx_limiter:init(Zone, Policy),
%     reply(ok, Channel#channel{quota = Quota});

handle_call(Req, Channel) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec(handle_info(Info :: term(), channel())
      -> ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}).

%handle_info({subscribe, TopicFilters}, Channel ) ->
%    {_, NChannel} = lists:foldl(
%        fun({TopicFilter, SubOpts}, {_, ChannelAcc}) ->
%            do_subscribe(TopicFilter, SubOpts, ChannelAcc)
%        end, {[], Channel}, parse_topic_filters(TopicFilters)),
%    {ok, NChannel};
%
%handle_info({unsubscribe, TopicFilters}, Channel) ->
%    {_RC, NChannel} = process_unsubscribe(TopicFilters, #{}, Channel),
%    {ok, NChannel};
%
%handle_info({sock_closed, Reason}, Channel = #channel{conn_state = idle}) ->
%    shutdown(Reason, Channel);
%
%handle_info({sock_closed, Reason}, Channel = #channel{conn_state = connecting}) ->
%    shutdown(Reason, Channel);
%
%handle_info({sock_closed, Reason}, Channel =
%            #channel{conn_state = connected,
%                     clientinfo = ClientInfo = #{zone := Zone}}) ->
%    emqx_zone:enable_flapping_detect(Zone)
%        andalso emqx_flapping:detect(ClientInfo),
%    Channel1 = ensure_disconnected(Reason, mabye_publish_will_msg(Channel)),
%    case maybe_shutdown(Reason, Channel1) of
%        {ok, Channel2} -> {ok, {event, disconnected}, Channel2};
%        Shutdown -> Shutdown
%    end;
%
%handle_info({sock_closed, Reason}, Channel = #channel{conn_state = disconnected}) ->
%    ?LOG(error, "Unexpected sock_closed: ~p", [Reason]),
%    {ok, Channel};
%
%handle_info(clean_acl_cache, Channel) ->
%    ok = emqx_acl_cache:empty_acl_cache(),
%    {ok, Channel};

handle_info(Info, Channel) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(Reason, Channel = #channel{conninfo = ConnInfo,
                                               clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.disconnected', [ClientInfo, Reason, NConnInfo]),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

%% TODO: Remove this func
send(Msg = #message{topic = Topic, headers = Headers, payload = Payload},
     Channel = #channel{subscriptions = Subscriptions}) ->
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
            send(Frame, Channel);
        false ->
            ?LOG(error, "Stomp dropped: ~p", [Msg]),
            {error, dropped, Channel}
    end;

send(Frame, Channel = #channel{sendfun = {Fun, Args}}) ->
    ?LOG(info, "SEND Frame: ~s", [emqx_stomp_frame:format(Frame)]),
    Data = emqx_stomp_frame:serialize(Frame),
    ?LOG(debug, "SEND ~p", [Data]),
    erlang:apply(Fun, [Data] ++ Args),
    {ok, Channel}.

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

shutdown(_Reason, _Channel) ->
    ok.

shutdown(Reason, AckFrame, Channel) ->
    {shutdown, Reason, AckFrame, Channel}.

shutdown_and_reply(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

timeout(_TRef, {incoming, NewVal},
        Channel = #channel{heartbeat = HrtBt}) ->
    case emqx_stomp_heartbeat:check(incoming, NewVal, HrtBt) of
        {error, timeout} ->
            {shutdown, heartbeat_timeout, Channel};
        {ok, NHrtBt} ->
            {ok, reset_timer(incoming_timer, Channel#channel{heartbeat = NHrtBt})}
    end;

timeout(_TRef, {outgoing, NewVal},
        Channel = #channel{heartbeat = HrtBt,
                             heartfun = {Fun, Args}}) ->
    case emqx_stomp_heartbeat:check(outgoing, NewVal, HrtBt) of
        {error, timeout} ->
            _ = erlang:apply(Fun, Args),
            {ok, Channel};
        {ok, NHrtBt} ->
            {ok, reset_timer(outgoing_timer, Channel#channel{heartbeat = NHrtBt})}
    end;

timeout(_TRef, clean_trans, Channel = #channel{transaction = Trans}) ->
    Now = erlang:system_time(millisecond),
    NTrans = maps:filter(fun(_, {Ts, _}) -> Ts + ?TRANS_TIMEOUT < Now end, Trans),
    {ok, ensure_clean_trans_timer(Channel#channel{transaction = NTrans})}.

do_negotiate_version(undefined) ->
    {ok, <<"1.0">>};

do_negotiate_version(Accepts) ->
     do_negotiate_version(
       ?STOMP_VER,
       lists:reverse(lists:sort(binary:split(Accepts, <<",">>, [global])))
      ).

do_negotiate_version(Ver, []) ->
    {error, <<"Supported protocol versions < ", Ver/binary>>};
do_negotiate_version(Ver, [AcceptVer|_]) when Ver >= AcceptVer ->
    {ok, AcceptVer};
do_negotiate_version(Ver, [_|T]) ->
    do_negotiate_version(Ver, T).

add_action(Id, Action, ReceiptId, Channel = #channel{transaction = Trans}) ->
    case maps:get(Id, Trans, undefined) of
        {Ts, Actions} ->
            NTrans = Trans#{Id => {Ts, [Action|Actions]}},
            {ok, Channel#channel{transaction = NTrans}};
        _ ->
            send(error_frame(ReceiptId, ["Transaction ", Id, " not found"]), Channel)
    end.

maybe_send_receipt(undefined, Channel) ->
    {ok, Channel};
maybe_send_receipt(ReceiptId, Channel) ->
    send(receipt_frame(ReceiptId), Channel).

ack(_Id, Channel) ->
    Channel.

nack(_Id, Channel) -> Channel.

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
%% Transaction Handle

handle_recv_send_frame(#stomp_frame{command = <<"SEND">>, headers = Headers, body = Body}, Channel) ->
    Topic = header(<<"destination">>, Headers),
    _ = maybe_send_receipt(receipt_id(Headers), Channel),
    _ = emqx_broker:publish(
        make_mqtt_message(Topic, Headers, iolist_to_binary(Body))
    ),
    Channel.

handle_recv_ack_frame(#stomp_frame{command = <<"ACK">>, headers = Headers}, Channel) ->
    Id = header(<<"id">>, Headers),
    _ = maybe_send_receipt(receipt_id(Headers), Channel),
    ack(Id, Channel).

handle_recv_nack_frame(#stomp_frame{command = <<"NACK">>, headers = Headers}, Channel) ->
    Id = header(<<"id">>, Headers),
     _ = maybe_send_receipt(receipt_id(Headers), Channel),
     nack(Id, Channel).

ensure_clean_trans_timer(Channel = #channel{transaction = Trans}) ->
    case maps:size(Trans) of
        0 -> Channel;
        _ -> ensure_timer(clean_trans_timer, Channel)
    end.

%%--------------------------------------------------------------------
%% Heartbeat

reverse_heartbeats({Cx, Cy}) ->
    iolist_to_binary(io_lib:format("~w,~w", [Cy, Cx])).

ensure_heartbeart_timer(Channel = #channel{clientinfo = ClientInfo}) ->
    Heartbeat = maps:get(heartbeat, ClientInfo),
    ensure_timer(
      [incoming_timer, outgoing_timer],
      Channel#channel{heartbeat = emqx_stomp_heartbeat:init(Heartbeat)}).

%%--------------------------------------------------------------------
%% Timer

ensure_timer([Name], Channel) ->
    ensure_timer(Name, Channel);
ensure_timer([Name | Rest], Channel) ->
    ensure_timer(Rest, ensure_timer(Name, Channel));

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso is_integer(Time) andalso Time > 0 of
        true  -> ensure_timer(Name, Time, Channel);
        false -> Channel %% Timer disabled or exists
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_misc:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(incoming_timer, #channel{heartbeat = HrtBt}) ->
    emqx_stomp_heartbeat:interval(incoming, HrtBt);
interval(outgoing_timer, #channel{heartbeat = HrtBt}) ->
    emqx_stomp_heartbeat:interval(outgoing, HrtBt);
interval(clean_trans_timer, _) ->
    ?TRANS_TIMEOUT.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

run_hooks(Name, Args) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run_fold(Name, Args, Acc).
