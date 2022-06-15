%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(emqx_gateway_channel).

-include("src/stomp/include/emqx_stomp.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-import(proplists, [get_value/2, get_value/3]).

%% API
-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    init/2,
    handle_in/2,
    handle_out/3,
    handle_deliver/2,
    handle_timeout/3,
    terminate/2,
    set_conn_state/2
]).

-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% for trans callback
-export([
    handle_recv_send_frame/2,
    handle_recv_ack_frame/2,
    handle_recv_nack_frame/2
]).

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% Stomp Connection Info
    conninfo :: emqx_types:conninfo(),
    %% Stomp Client Info
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: undefined | map(),
    %% ClientInfo override specs
    clientinfo_override :: map(),
    %% Channel State
    conn_state :: conn_state(),
    %% Heartbeat
    heartbeat :: emqx_stomp_heartbeat:heartbeat(),
    %% Subscriptions
    subscriptions = [],
    %% Timer
    timers :: #{atom() => disable | undefined | reference()},
    %% Transaction
    transaction :: #{binary() => list()}
}).

-type channel() :: #channel{}.

-type conn_state() :: idle | connecting | connected | disconnected.

-type reply() ::
    {outgoing, stomp_frame()}
    | {outgoing, [stomp_frame()]}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: reply() | [reply()].

-define(TIMER_TABLE, #{
    incoming_timer => keepalive,
    outgoing_timer => keepalive_send,
    clean_trans_timer => clean_trans
}).

-define(TRANS_TIMEOUT, 60000).

-define(DEFAULT_OVERRIDE,
    %% Generate clientid by default
    #{
        clientid => <<"">>,
        username => <<"${Packet.headers.login}">>,
        password => <<"${Packet.headers.passcode}">>
    }
).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

%% @doc Init protocol
init(
    ConnInfo = #{
        peername := {PeerHost, _},
        sockname := {_, SockPort}
    },
    Option
) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Option, undefined),
    ListenerId =
        case maps:get(listener, Option, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,
    EnableAuthn = maps:get(enable_authn, Option, true),
    ClientInfo = setting_peercert_infos(
        Peercert,
        #{
            zone => default,
            listener => ListenerId,
            protocol => stomp,
            peerhost => PeerHost,
            sockport => SockPort,
            clientid => undefined,
            username => undefined,
            is_bridge => false,
            is_superuser => false,
            enable_authn => EnableAuthn,
            mountpoint => Mountpoint
        }
    ),

    Ctx = maps:get(ctx, Option),
    Override = maps:merge(
        ?DEFAULT_OVERRIDE,
        maps:get(clientinfo_override, Option, #{})
    ),
    #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        clientinfo_override = Override,
        timers = #{},
        transaction = #{},
        conn_state = idle
    }.

setting_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
setting_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom(), channel()) -> term().
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, _) ->
    #{};
info(will_msg, _) ->
    undefined;
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(ctx, #channel{ctx = Ctx}) ->
    Ctx.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{subscriptions = Subs}) ->
    [{subscriptions_cnt, length(Subs)}].

set_conn_state(ConnState, Channel) ->
    Channel#channel{conn_state = ConnState}.

enrich_conninfo(
    _Packet,
    Channel = #channel{conninfo = ConnInfo}
) ->
    %% XXX: How enrich more infos?
    NConnInfo = ConnInfo#{
        proto_name => <<"STOMP">>,
        proto_ver => <<"1.2">>,
        clean_start => true,
        keepalive => 0,
        expiry_interval => 0,
        conn_props => #{},
        receive_maximum => 0
    },
    {ok, Channel#channel{conninfo = NConnInfo}}.

run_conn_hooks(
    Packet,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    %% XXX: Assign headers of Packet to ConnProps
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, Packet, Channel}
    end.

negotiate_version(
    #stomp_frame{headers = Headers},
    Channel = #channel{conninfo = ConnInfo}
) ->
    %% XXX:
    case do_negotiate_version(header(<<"accept-version">>, Headers)) of
        {ok, Version} ->
            {ok, Channel#channel{conninfo = ConnInfo#{proto_ver => Version}}};
        {error, Reason} ->
            {error, Reason}
    end.

enrich_clientinfo(
    Packet,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo0,
        clientinfo_override = Override
    }
) ->
    ClientInfo = write_clientinfo(
        feedvar(Override, Packet, ConnInfo, ClientInfo0),
        ClientInfo0
    ),
    {ok, NPacket, NClientInfo} = emqx_misc:pipeline(
        [
            fun maybe_assign_clientid/2,
            fun parse_heartbeat/2,
            %% FIXME: CALL After authentication successfully
            fun fix_mountpoint/2
        ],
        Packet,
        ClientInfo
    ),
    {ok, NPacket, Channel#channel{clientinfo = NClientInfo}}.

assign_clientid_to_conninfo(
    Packet,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    ClientId = maps:get(clientid, ClientInfo),
    NConnInfo = maps:put(clientid, ClientId, ConnInfo),
    {ok, Packet, Channel#channel{conninfo = NConnInfo}}.

feedvar(Override, Packet, ConnInfo, ClientInfo) ->
    Envs = #{
        'ConnInfo' => ConnInfo,
        'ClientInfo' => ClientInfo,
        'Packet' => connect_packet_to_map(Packet)
    },
    maps:map(
        fun(_K, V) ->
            Tokens = emqx_plugin_libs_rule:preproc_tmpl(V),
            emqx_plugin_libs_rule:proc_tmpl(Tokens, Envs)
        end,
        Override
    ).

connect_packet_to_map(#stomp_frame{headers = Headers}) ->
    #{headers => maps:from_list(Headers)}.

write_clientinfo(Override, ClientInfo) ->
    Override1 = maps:with([username, password, clientid], Override),
    maps:merge(ClientInfo, Override1).

maybe_assign_clientid(_Packet, ClientInfo = #{clientid := ClientId}) when
    ClientId == undefined;
    ClientId == <<>>
->
    {ok, ClientInfo#{clientid => emqx_guid:to_base62(emqx_guid:gen())}};
maybe_assign_clientid(_Packet, ClientInfo) ->
    {ok, ClientInfo}.

parse_heartbeat(#stomp_frame{headers = Headers}, ClientInfo) ->
    Heartbeat0 = header(<<"heart-beat">>, Headers, <<"0,0">>),
    CxCy = re:split(Heartbeat0, <<",">>, [{return, list}]),
    Heartbeat = list_to_tuple([list_to_integer(S) || S <- CxCy]),
    {ok, ClientInfo#{heartbeat => Heartbeat}}.

fix_mountpoint(_Packet, #{mountpoint := undefined}) ->
    ok;
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    %% TODO: Enrich the variable replacement????
    %%       i.e: ${ClientInfo.auth_result.productKey}
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

set_log_meta(_Packet, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

auth_connect(
    _Packet,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    #{
        clientid := ClientId,
        username := Username
    } = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "client_login_failed",
                clientid => ClientId,
                username => Username,
                reason => Reason
            }),
            {error, Reason}
    end.

ensure_connected(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks(Ctx, 'client.connected', [ClientInfo, NConnInfo]),
    Channel#channel{
        conninfo = NConnInfo,
        conn_state = connected
    }.

process_connect(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    SessFun = fun(_, _) -> #{} end,
    case
        emqx_gateway_ctx:open_session(
            Ctx,
            true,
            ClientInfo,
            ConnInfo,
            SessFun
        )
    of
        {ok, #{session := Session}} ->
            #{proto_ver := Version} = ConnInfo,
            #{heartbeat := Heartbeat} = ClientInfo,
            Headers = [
                {<<"version">>, Version},
                {<<"heart-beat">>, reverse_heartbeats(Heartbeat)}
            ],
            handle_out(connected, Headers, Channel#channel{session = Session});
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_open_session",
                reason => Reason
            }),
            Headers = [
                {<<"version">>, <<"1.0,1.1,1.2">>},
                {<<"content-type">>, <<"text/plain">>}
            ],
            handle_out(connerr, {Headers, undefined, <<"Not Authenticated">>}, Channel)
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(stomp_frame() | {frame_error, any()}, channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.

handle_in(Frame = ?PACKET(?CMD_STOMP), Channel) ->
    handle_in(Frame#stomp_frame{command = <<"CONNECT">>}, Channel);
handle_in(
    ?PACKET(?CMD_CONNECT),
    Channel = #channel{conn_state = connected}
) ->
    {error, unexpected_connect, Channel};
handle_in(Packet = ?PACKET(?CMD_CONNECT), Channel) ->
    case
        emqx_misc:pipeline(
            [
                fun enrich_conninfo/2,
                fun negotiate_version/2,
                fun enrich_clientinfo/2,
                fun assign_clientid_to_conninfo/2,
                fun run_conn_hooks/2,
                fun set_log_meta/2,
                %% TODO: How to implement the banned in the gateway instance?
                %, fun check_banned/2
                fun auth_connect/2
            ],
            Packet,
            Channel#channel{conn_state = connecting}
        )
    of
        {ok, _NPacket, NChannel} ->
            process_connect(ensure_connected(NChannel));
        {error, ReasonCode, NChannel} ->
            ErrMsg = io_lib:format("Login Failed: ~ts", [ReasonCode]),
            handle_out(connerr, {[], undefined, ErrMsg}, NChannel)
    end;
handle_in(
    Frame = ?PACKET(?CMD_SEND, Headers),
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    Topic = header(<<"destination">>, Headers),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, publish, Topic) of
        deny ->
            handle_out(error, {receipt_id(Headers), "Authorization Deny"}, Channel);
        allow ->
            case header(<<"transaction">>, Headers) of
                undefined ->
                    handle_recv_send_frame(Frame, Channel);
                TxId ->
                    add_action(
                        TxId,
                        {fun ?MODULE:handle_recv_send_frame/2, [Frame]},
                        receipt_id(Headers),
                        Channel
                    )
            end
    end;
handle_in(
    ?PACKET(?CMD_SUBSCRIBE, Headers),
    Channel = #channel{
        ctx = Ctx,
        subscriptions = Subs,
        clientinfo = ClientInfo
    }
) ->
    SubId = header(<<"id">>, Headers),
    Topic = header(<<"destination">>, Headers),
    Ack = header(<<"ack">>, Headers, <<"auto">>),
    case
        emqx_misc:pipeline(
            [
                fun parse_topic_filter/2,
                fun check_subscribed_status/2,
                fun check_sub_acl/2
            ],
            {SubId, Topic},
            Channel
        )
    of
        {ok, {_, TopicFilter}, NChannel} ->
            TopicFilters = [TopicFilter],
            NTopicFilters = run_hooks(
                Ctx,
                'client.subscribe',
                [ClientInfo, #{}],
                TopicFilters
            ),
            case do_subscribe(NTopicFilters, NChannel) of
                [] ->
                    ErrMsg = "Permission denied",
                    handle_out(error, {receipt_id(Headers), ErrMsg}, Channel);
                [{MountedTopic, SubOpts} | _] ->
                    NSubs = [{SubId, MountedTopic, Ack, SubOpts} | Subs],
                    NChannel1 = NChannel#channel{subscriptions = NSubs},
                    handle_out(receipt, receipt_id(Headers), NChannel1)
            end;
        {error, ErrMsg, NChannel} ->
            ?SLOG(error, #{
                msg => "failed_top_subscribe_topic",
                topic => Topic,
                reason => ErrMsg
            }),
            handle_out(error, {receipt_id(Headers), ErrMsg}, NChannel)
    end;
handle_in(
    ?PACKET(?CMD_UNSUBSCRIBE, Headers),
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{mountpoint := Mountpoint},
        subscriptions = Subs
    }
) ->
    SubId = header(<<"id">>, Headers),
    {ok, NChannel} =
        case lists:keyfind(SubId, 1, Subs) of
            {SubId, MountedTopic, _Ack, _SubOpts} ->
                Topic = emqx_mountpoint:unmount(Mountpoint, MountedTopic),
                %% XXX: eval the return topics?
                _ = run_hooks(
                    Ctx,
                    'client.unsubscribe',
                    [ClientInfo, #{}],
                    [{Topic, #{}}]
                ),
                ok = emqx_broker:unsubscribe(MountedTopic),
                _ = run_hooks(
                    Ctx,
                    'session.unsubscribe',
                    [ClientInfo, MountedTopic, #{}]
                ),
                {ok, Channel#channel{subscriptions = lists:keydelete(SubId, 1, Subs)}};
            false ->
                {ok, Channel}
        end,
    handle_out(receipt, receipt_id(Headers), NChannel);
%% XXX: How to ack a frame ???
handle_in(Frame = ?PACKET(?CMD_ACK, Headers), Channel) ->
    case header(<<"transaction">>, Headers) of
        undefined ->
            handle_recv_ack_frame(Frame, Channel);
        TxId ->
            add_action(
                TxId, {fun ?MODULE:handle_recv_ack_frame/2, [Frame]}, receipt_id(Headers), Channel
            )
    end;
%% NACK
%% id:12345
%% transaction:tx1
%%
%% ^@
handle_in(Frame = ?PACKET(?CMD_NACK, Headers), Channel) ->
    case header(<<"transaction">>, Headers) of
        undefined ->
            handle_recv_nack_frame(Frame, Channel);
        TxId ->
            add_action(
                TxId, {fun ?MODULE:handle_recv_nack_frame/2, [Frame]}, receipt_id(Headers), Channel
            )
    end;
%% The transaction header is REQUIRED, and the transaction identifier
%% will be used for SEND, COMMIT, ABORT, ACK, and NACK frames to bind
%% them to the named transaction.
%%
%% BEGIN
%% transaction:tx1
%%
%% ^@
handle_in(
    ?PACKET(?CMD_BEGIN, Headers),
    Channel = #channel{transaction = Trans}
) ->
    TxId = header(<<"transaction">>, Headers),
    case maps:get(TxId, Trans, undefined) of
        undefined ->
            StartedAt = erlang:system_time(millisecond),
            NChannel = ensure_clean_trans_timer(
                Channel#channel{
                    transaction = Trans#{TxId => {StartedAt, []}}
                }
            ),
            handle_out(receipt, receipt_id(Headers), NChannel);
        _ ->
            ErrMsg = ["Transaction ", TxId, " already started"],
            handle_out(error, {receipt_id(Headers), ErrMsg}, Channel)
    end;
%% COMMIT
%% transaction:tx1
%%
%% ^@
handle_in(?PACKET(?CMD_COMMIT, Headers), Channel) ->
    with_transaction(Headers, Channel, fun(TxId, Actions) ->
        Chann0 = remove_trans(TxId, Channel),
        case trans_pipeline(lists:reverse(Actions), [], Chann0) of
            {ok, Outgoings, Chann1} ->
                maybe_outgoing_receipt(receipt_id(Headers), Outgoings, Chann1);
            {error, Reason, Chann1} ->
                %% FIXME: atomic for transaction ??
                ErrMsg = io_lib:format(
                    "Execute transaction ~ts failed: ~0p",
                    [TxId, Reason]
                ),
                handle_out(error, {receipt_id(Headers), ErrMsg}, Chann1)
        end
    end);
%% ABORT
%% transaction:tx1
%%
%% ^@
handle_in(
    ?PACKET(?CMD_ABORT, Headers),
    Channel = #channel{transaction = Trans}
) ->
    with_transaction(Headers, Channel, fun(Id, _Actions) ->
        NChannel = Channel#channel{transaction = maps:remove(Id, Trans)},
        handle_out(receipt, receipt_id(Headers), NChannel)
    end);
handle_in(
    ?PACKET(?CMD_DISCONNECT, Headers),
    Channel = #channel{conn_state = connected}
) ->
    Outgoings =
        case receipt_id(Headers) of
            undefined ->
                [{close, normal}];
            ReceiptId ->
                [
                    {outgoing, receipt_frame(ReceiptId)},
                    {close, normal}
                ]
        end,
    {ok, Outgoings, Channel};
handle_in({frame_error, Reason}, Channel = #channel{conn_state = _ConnState}) ->
    ?SLOG(error, #{
        msg => "unexpected_frame_error",
        reason => Reason
    }),
    shutdown(Reason, Channel).

with_transaction(Headers, Channel = #channel{transaction = Trans}, Fun) ->
    Id = header(<<"transaction">>, Headers),
    ReceiptId = receipt_id(Headers),
    case maps:get(Id, Trans, undefined) of
        {_, Actions} ->
            Fun(Id, Actions);
        _ ->
            ErrMsg = ["Transaction ", Id, " not found"],
            handle_out(error, {ReceiptId, ErrMsg}, Channel)
    end.

remove_trans(Id, Channel = #channel{transaction = Trans}) ->
    Channel#channel{transaction = maps:remove(Id, Trans)}.

trans_pipeline([], Outgoings, Channel) ->
    {ok, Outgoings, Channel};
trans_pipeline([{Func, Args} | More], Outgoings, Channel) ->
    case erlang:apply(Func, Args ++ [Channel]) of
        {ok, NChannel} ->
            trans_pipeline(More, Outgoings, NChannel);
        {ok, Outgoings1, NChannel} ->
            trans_pipeline(More, Outgoings ++ Outgoings1, NChannel);
        {error, Reason} ->
            {error, Reason, Channel}
    end.

%%--------------------------------------------------------------------
%% Subs

parse_topic_filter({SubId, Topic}, Channel) ->
    {ParsedTopic, SubOpts} = emqx_topic:parse(Topic),
    NSubOpts = SubOpts#{sub_props => #{subid => SubId}},
    {ok, {SubId, {ParsedTopic, NSubOpts}}, Channel}.

check_subscribed_status(
    {SubId, {ParsedTopic, _SubOpts}},
    #channel{
        subscriptions = Subs,
        clientinfo = #{mountpoint := Mountpoint}
    }
) ->
    MountedTopic = emqx_mountpoint:mount(Mountpoint, ParsedTopic),
    case lists:keyfind(SubId, 1, Subs) of
        {SubId, MountedTopic, _Ack, _} ->
            ok;
        {SubId, _OtherTopic, _Ack, _} ->
            {error, "Conflict subscribe id"};
        false ->
            ok
    end.

check_sub_acl(
    {_SubId, {ParsedTopic, _SubOpts}},
    #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, subscribe, ParsedTopic) of
        deny -> {error, "ACL Deny"};
        allow -> ok
    end.

do_subscribe(TopicFilters, Channel) ->
    do_subscribe(TopicFilters, Channel, []).

do_subscribe([], _Channel, Acc) ->
    lists:reverse(Acc);
do_subscribe(
    [{ParsedTopic, SubOpts0} | More],
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{
                    clientid := ClientId,
                    mountpoint := Mountpoint
                }
    },
    Acc
) ->
    SubOpts = maps:merge(emqx_gateway_utils:default_subopts(), SubOpts0),
    MountedTopic = emqx_mountpoint:mount(Mountpoint, ParsedTopic),
    _ = emqx_broker:subscribe(MountedTopic, ClientId, SubOpts),
    run_hooks(Ctx, 'session.subscribed', [ClientInfo, MountedTopic, SubOpts]),
    do_subscribe(More, Channel, [{MountedTopic, SubOpts} | Acc]).

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec handle_out(atom(), term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.

handle_out(connerr, {Headers, ReceiptId, ErrMsg}, Channel) ->
    Frame = error_frame(Headers, ReceiptId, ErrMsg),
    shutdown(ErrMsg, Frame, Channel);
handle_out(error, {ReceiptId, ErrMsg}, Channel) ->
    Frame = error_frame(ReceiptId, ErrMsg),
    {ok, {outgoing, Frame}, Channel};
handle_out(
    connected,
    Headers,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    %% XXX: connection_accepted is not defined by stomp protocol
    _ = run_hooks(Ctx, 'client.connack', [ConnInfo, connection_accepted, #{}]),
    Replies = [
        {outgoing, connected_frame(Headers)},
        {event, connected}
    ],
    {ok, Replies, ensure_heartbeart_timer(Channel)};
handle_out(receipt, undefined, Channel) ->
    {ok, Channel};
handle_out(receipt, ReceiptId, Channel) ->
    Frame = receipt_frame(ReceiptId),
    {ok, {outgoing, Frame}, Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), From :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {reply, Reply :: term(), replies(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), stomp_frame(), channel()}.
handle_call(
    {subscribe, Topic, SubOpts},
    _From,
    Channel = #channel{
        subscriptions = Subs
    }
) ->
    case
        maps:get(
            subid,
            maps:get(sub_props, SubOpts, #{}),
            undefined
        )
    of
        undefined ->
            reply({error, no_subid}, Channel);
        SubId ->
            case
                emqx_misc:pipeline(
                    [
                        fun parse_topic_filter/2,
                        fun check_subscribed_status/2
                    ],
                    {SubId, {Topic, SubOpts}},
                    Channel
                )
            of
                {ok, {_, TopicFilter}, NChannel} ->
                    [{MountedTopic, NSubOpts}] = do_subscribe(
                        [TopicFilter],
                        NChannel
                    ),
                    NSubs = [{SubId, MountedTopic, <<"auto">>, NSubOpts} | Subs],
                    NChannel1 = NChannel#channel{subscriptions = NSubs},
                    reply({ok, {MountedTopic, NSubOpts}}, NChannel1);
                {error, ErrMsg, NChannel} ->
                    ?SLOG(error, #{
                        msg => "failed_to_subscribe_topic",
                        topic => Topic,
                        reason => ErrMsg
                    }),
                    reply({error, ErrMsg}, NChannel)
            end
    end;
handle_call(
    {unsubscribe, Topic},
    _From,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo = #{mountpoint := Mountpoint},
        subscriptions = Subs
    }
) ->
    {ParsedTopic, _SubOpts} = emqx_topic:parse(Topic),
    MountedTopic = emqx_mountpoint:mount(Mountpoint, ParsedTopic),
    ok = emqx_broker:unsubscribe(MountedTopic),
    _ = run_hooks(
        Ctx,
        'session.unsubscribe',
        [ClientInfo, MountedTopic, #{}]
    ),
    reply(
        ok,
        Channel#channel{
            subscriptions = lists:keydelete(MountedTopic, 2, Subs)
        }
    );
%% Reply :: [{emqx_types:topic(), emqx_types:subopts()}]
handle_call(subscriptions, _From, Channel = #channel{subscriptions = Subs}) ->
    NSubs = lists:map(
        fun({_SubId, Topic, _Ack, SubOpts}) ->
            {Topic, SubOpts}
        end,
        Subs
    ),
    reply({ok, NSubs}, Channel);
handle_call(kick, _From, Channel) ->
    NChannel = ensure_disconnected(kicked, Channel),
    Frame = error_frame(undefined, <<"Kicked out">>),
    shutdown_and_reply(kicked, ok, Frame, NChannel);
handle_call(discard, _From, Channel) ->
    Frame = error_frame(undefined, <<"Discarded">>),
    shutdown_and_reply(discarded, ok, Frame, Channel);
%% XXX: No Session Takeover
%handle_call({takeover, 'begin'}, _From, Channel = #channel{session = Session}) ->
%    reply(Session, Channel#channel{takeover = true});
%
%handle_call({takeover, 'end'}, _From, Channel = #channel{session  = Session,
%                                                  pendings = Pendings}) ->
%    ok = emqx_session:takeover(Session),
%    %% TODO: Should not drain deliver here (side effect)
%    Delivers = emqx_misc:drain_deliver(),
%    AllPendings = lists:append(Delivers, Pendings),
%    shutdown_and_reply(takenover, AllPendings, Channel);

handle_call(list_authz_cache, _From, Channel) ->
    %% This won't work
    {reply, emqx_authz_cache:list_authz_cache(), Channel};
%% XXX: No Quota Now
% handle_call({quota, Policy}, Channel) ->
%     Zone = info(zone, Channel),
%     Quota = emqx_limiter:init(Zone, Policy),
%     reply(ok, Channel#channel{quota = Quota});

handle_call(Req, _From, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_call",
        call => Req
    }),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle cast
%%--------------------------------------------------------------------

-spec handle_cast(Req :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_cast(_Req, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec handle_info(Info :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.

handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = idle}
) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = connecting}
) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{
        conn_state = connected,
        clientinfo = _ClientInfo
    }
) ->
    %% XXX: Flapping detect ???
    %% How to get the flapping detect policy ???
    %emqx_zone:enable_flapping_detect(Zone)
    %    andalso emqx_flapping:detect(ClientInfo),
    NChannel = ensure_disconnected(Reason, Channel),
    shutdown(Reason, NChannel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = disconnected}
) ->
    ?SLOG(error, #{
        msg => "unexpected_sock_closed",
        reason => Reason
    }),
    {ok, Channel};
handle_info(clean_authz_cache, Channel) ->
    ok = emqx_authz_cache:empty_authz_cache(),
    {ok, Channel};
handle_info({subscribe, _}, Channel) ->
    {ok, Channel};
handle_info(Info, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_info",
        info => Info
    }),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(
    Reason,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks(
        Ctx,
        'client.disconnected',
        [ClientInfo, Reason, NConnInfo]
    ),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------

-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}.

handle_deliver(
    Delivers,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo,
        subscriptions = Subs
    }
) ->
    %% TODO: Re-deliver ???
    %%       Shared-subscription support ???

    Frames0 = lists:foldl(
        fun({_, _, Message}, Acc) ->
            Topic0 = emqx_message:topic(Message),
            case lists:keyfind(Topic0, 2, Subs) of
                {Id, Topic, Ack, _SubOpts} ->
                    %% XXX: refactor later
                    metrics_inc('messages.delivered', Channel),
                    NMessage = run_hooks_without_metrics(
                        Ctx,
                        'message.delivered',
                        [ClientInfo],
                        Message
                    ),
                    Topic = emqx_message:topic(NMessage),
                    Headers = emqx_message:get_headers(NMessage),
                    Payload = emqx_message:payload(NMessage),
                    Headers0 = [
                        {<<"subscription">>, Id},
                        {<<"message-id">>, next_msgid()},
                        {<<"destination">>, Topic},
                        {<<"content-type">>, <<"text/plain">>}
                    ],
                    Headers1 =
                        case Ack of
                            _ when
                                Ack =:= <<"client">>;
                                Ack =:= <<"client-individual">>
                            ->
                                Headers0 ++ [{<<"ack">>, next_ackid()}];
                            _ ->
                                Headers0
                        end,
                    Frame = #stomp_frame{
                        command = <<"MESSAGE">>,
                        headers = Headers1 ++ maps:get(stomp_headers, Headers, []),
                        body = Payload
                    },
                    [Frame | Acc];
                false ->
                    ?SLOG(error, #{
                        msg => "dropped_message_due_to_subscription_not_found",
                        message => Message,
                        topic => emqx_message:topic(Message)
                    }),
                    metrics_inc('delivery.dropped', Channel),
                    metrics_inc('delivery.dropped.no_subid', Channel),
                    Acc
            end
        end,
        [],
        Delivers
    ),
    {ok, [{outgoing, lists:reverse(Frames0)}], Channel}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec handle_timeout(reference(), Msg :: term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.

handle_timeout(
    _TRef,
    {keepalive, NewVal},
    Channel = #channel{heartbeat = HrtBt}
) ->
    case emqx_stomp_heartbeat:check(incoming, NewVal, HrtBt) of
        {error, timeout} ->
            shutdown(heartbeat_timeout, Channel);
        {ok, NHrtBt} ->
            {ok,
                reset_timer(
                    incoming_timer,
                    Channel#channel{heartbeat = NHrtBt}
                )}
    end;
handle_timeout(
    _TRef,
    {keepalive_send, NewVal},
    Channel = #channel{heartbeat = HrtBt}
) ->
    case emqx_stomp_heartbeat:check(outgoing, NewVal, HrtBt) of
        {error, timeout} ->
            NHrtBt = emqx_stomp_heartbeat:reset(outgoing, NewVal, HrtBt),
            NChannel = Channel#channel{heartbeat = NHrtBt},
            {ok, {outgoing, emqx_stomp_frame:make(?CMD_HEARTBEAT)},
                reset_timer(outgoing_timer, NChannel)};
        {ok, NHrtBt} ->
            {ok,
                reset_timer(
                    outgoing_timer,
                    Channel#channel{heartbeat = NHrtBt}
                )}
    end;
handle_timeout(_TRef, clean_trans, Channel = #channel{transaction = Trans}) ->
    Now = erlang:system_time(millisecond),
    NTrans = maps:filter(
        fun(_, {StartedAt, _}) ->
            StartedAt + ?TRANS_TIMEOUT < Now
        end,
        Trans
    ),
    {ok, ensure_clean_trans_timer(Channel#channel{transaction = NTrans})}.

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(Reason, #channel{
    ctx = Ctx,
    session = Session,
    clientinfo = ClientInfo
}) ->
    run_hooks(Ctx, 'session.terminated', [ClientInfo, Reason, Session]).

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown(Reason, AckFrame, Channel) ->
    {shutdown, Reason, AckFrame, Channel}.

shutdown_and_reply(Reason, Reply, OutPkt, Channel) ->
    {shutdown, Reason, Reply, OutPkt, Channel}.

do_negotiate_version(undefined) ->
    {ok, <<"1.0">>};
do_negotiate_version(Accepts) ->
    do_negotiate_version(
        ?STOMP_VER,
        lists:reverse(lists:sort(binary:split(Accepts, <<",">>, [global])))
    ).

do_negotiate_version(Ver, []) ->
    {error, <<"Supported protocol versions < ", Ver/binary>>};
do_negotiate_version(Ver, [AcceptVer | _]) when Ver >= AcceptVer ->
    {ok, AcceptVer};
do_negotiate_version(Ver, [_ | T]) ->
    do_negotiate_version(Ver, T).

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
    MsgId =
        case get(msgid) of
            undefined -> 1;
            I -> I
        end,
    put(msgid, MsgId + 1),
    MsgId.

next_ackid() ->
    AckId =
        case get(ackid) of
            undefined -> 1;
            I -> I
        end,
    put(ackid, AckId + 1),
    AckId.

frame2message(
    ?PACKET(?CMD_SEND, Headers, Body),
    #channel{
        conninfo = #{proto_ver := ProtoVer},
        clientinfo = #{
            protocol := Protocol,
            clientid := ClientId,
            username := Username,
            peerhost := PeerHost,
            mountpoint := Mountpoint
        }
    }
) ->
    Topic = header(<<"destination">>, Headers),
    Msg = emqx_message:make(ClientId, Topic, Body),
    StompHeaders = lists:foldl(
        fun(Key, Headers0) ->
            proplists:delete(Key, Headers0)
        end,
        Headers,
        [
            <<"destination">>,
            <<"content-length">>,
            <<"content-type">>,
            <<"transaction">>,
            <<"receipt">>
        ]
    ),
    %% Pass-through of custom headers on the sending side
    NMsg = emqx_message:set_headers(
        #{
            proto_ver => ProtoVer,
            protocol => Protocol,
            username => Username,
            peerhost => PeerHost,
            stomp_headers => StompHeaders
        },
        Msg
    ),
    emqx_mountpoint:mount(Mountpoint, NMsg).

receipt_id(Headers) ->
    header(<<"receipt">>, Headers).

%%--------------------------------------------------------------------
%% Trans

add_action(TxId, Action, ReceiptId, Channel = #channel{transaction = Trans}) ->
    case maps:get(TxId, Trans, undefined) of
        {_StartedAt, Actions} ->
            NTrans = Trans#{TxId => {_StartedAt, [Action | Actions]}},
            {ok, Channel#channel{transaction = NTrans}};
        _ ->
            ErrFrame = error_frame(
                ReceiptId,
                ["Transaction ", TxId, " not found"]
            ),
            {ok, {outgoing, ErrFrame}, Channel}
    end.

%%--------------------------------------------------------------------
%% Transaction Handle

handle_recv_send_frame(Frame = ?PACKET(?CMD_SEND, Headers), Channel) ->
    Msg = frame2message(Frame, Channel),
    _ = emqx_broker:publish(Msg),
    maybe_outgoing_receipt(receipt_id(Headers), Channel).

handle_recv_ack_frame(?PACKET(?CMD_ACK, Headers), Channel) ->
    maybe_outgoing_receipt(receipt_id(Headers), Channel).

handle_recv_nack_frame(?PACKET(?CMD_NACK, Headers), Channel) ->
    maybe_outgoing_receipt(receipt_id(Headers), Channel).

maybe_outgoing_receipt(undefined, Channel) ->
    {ok, [], Channel};
maybe_outgoing_receipt(ReceiptId, Channel) ->
    {ok, [{outgoing, receipt_frame(ReceiptId)}], Channel}.

maybe_outgoing_receipt(undefined, Outgoings, Channel) ->
    {ok, Outgoings, Channel};
maybe_outgoing_receipt(ReceiptId, Outgoings, Channel) ->
    {ok, lists:reverse([receipt_frame(ReceiptId) | Outgoings]), Channel}.

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
        Channel#channel{heartbeat = emqx_stomp_heartbeat:init(Heartbeat)}
    ).

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
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
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

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

run_hooks_without_metrics(_Ctx, Name, Args, Acc) ->
    emqx_hooks:run_fold(Name, Args, Acc).

metrics_inc(Name, #channel{ctx = Ctx}) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name).
