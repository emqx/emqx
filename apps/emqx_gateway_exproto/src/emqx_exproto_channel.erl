%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_channel).

-include("emqx_exproto.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    init/2,
    handle_in/2,
    handle_frame_error/2,
    handle_deliver/2,
    handle_timeout/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export_type([channel/0]).

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% gRPC channel options
    gcli :: emqx_exproto_gcli:grpc_client_state(),
    %% Conn info
    conninfo :: emqx_types:conninfo(),
    %% Client info from `register` function
    clientinfo :: option(map()),
    %% Connection state
    conn_state :: conn_state(),
    %% Subscription
    subscriptions = #{},
    %% Keepalive
    keepalive :: option(emqx_keepalive:keepalive()),
    %% Timers
    timers :: #{atom() => disabled | option(reference())},
    %% Closed reason
    closed_reason = undefined
}).

-opaque channel() :: #channel{}.

-type conn_state() :: idle | connecting | connected | disconnected.

-type reply() ::
    {outgoing, binary()}
    | {outgoing, [binary()]}
    | {close, Reason :: atom()}.

-type replies() :: emqx_types:packet() | reply() | [reply()].

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom(), channel()) -> term().
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(clientid, #channel{clientinfo = ClientInfo}) ->
    maps:get(clientid, ClientInfo, undefined);
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{
    subscriptions = Subs,
    conninfo = ConnInfo
}) ->
    #{
        subscriptions => Subs,
        upgrade_qos => false,
        retry_interval => 0,
        await_rel_timeout => 0,
        created_at => maps:get(connected_at, ConnInfo)
    };
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(will_msg, _) ->
    undefined;
info(ctx, #channel{ctx = Ctx}) ->
    Ctx.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{subscriptions = Subs}) ->
    [
        {subscriptions_cnt, maps:size(Subs)},
        {subscriptions_max, infinity},
        {inflight_cnt, 0},
        {inflight_max, infinity},
        {mqueue_len, 0},
        {mqueue_max, infinity},
        {mqueue_dropped, 0},
        {next_pkt_id, 0},
        {awaiting_rel_cnt, 0},
        {awaiting_rel_max, 0}
    ].

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec init(emqx_types:conninfo(), map()) -> channel().
init(
    ConnInfo = #{
        socktype := Socktype,
        peername := Peername,
        sockname := Sockname,
        peercert := Peercert
    },
    Options
) ->
    GRpcChann = maps:get(grpc_client_channel, Options),
    ServiceName = maps:get(grpc_client_service_name, Options),
    GRpcClient = emqx_exproto_gcli:init(ServiceName, #{channel => GRpcChann}),

    Ctx = maps:get(ctx, Options),
    IdleTimeout = emqx_gateway_utils:idle_timeout(Options),

    NConnInfo = default_conninfo(ConnInfo#{idle_timeout => IdleTimeout}),
    ListenerId =
        case maps:get(listener, Options, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,

    EnableAuthn = maps:get(enable_authn, Options, true),
    DefaultClientInfo = default_clientinfo(NConnInfo),
    ClientInfo = DefaultClientInfo#{
        listener => ListenerId,
        enable_authn => EnableAuthn,
        mountpoint => maps:get(mountpoint, Options, undefined)
    },
    Channel = #channel{
        ctx = Ctx,
        gcli = GRpcClient,
        conninfo = NConnInfo,
        clientinfo = ClientInfo,
        conn_state = connecting,
        timers = #{}
    },

    Req = #{
        conninfo =>
            peercert(
                Peercert,
                #{
                    socktype => socktype(Socktype),
                    peername => address(Peername),
                    sockname => address(Sockname)
                }
            )
    },
    dispatch(on_socket_created, Req, start_idle_checking_timer(Channel)).

%% @private
peercert(NoSsl, ConnInfo) when
    NoSsl == nossl;
    NoSsl == undefined
->
    ConnInfo;
peercert(Peercert, ConnInfo) ->
    Fn = fun(_, V) -> V =/= undefined end,
    Infos = maps:filter(
        Fn,
        #{
            cn => esockd_peercert:common_name(Peercert),
            dn => esockd_peercert:subject(Peercert)
        }
    ),
    case maps:size(Infos) of
        0 ->
            ConnInfo;
        _ ->
            ConnInfo#{peercert => Infos}
    end.

%% @private
socktype(tcp) -> 'TCP';
socktype(ssl) -> 'SSL';
socktype(udp) -> 'UDP';
socktype(dtls) -> 'DTLS'.

%% @private
address({Host, Port}) ->
    #{host => inet:ntoa(Host), port => Port}.

%% avoid udp connection process leak
start_idle_checking_timer(Channel = #channel{conninfo = #{socktype := udp}}) ->
    ensure_timer(force_close_idle, Channel);
start_idle_checking_timer(Channel) ->
    Channel.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(binary(), channel()) ->
    {ok, channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_in(Data, Channel) ->
    Req = #{bytes => Data},
    {ok, dispatch(on_received_bytes, Req, Channel)}.

handle_frame_error(Reason, Channel) ->
    {shutdown, Reason, Channel}.

-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_deliver(
    Delivers,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    %% XXX: ?? Nack delivers from shared subscriptions
    Mountpoint = maps:get(mountpoint, ClientInfo),
    NodeStr = atom_to_binary(node(), utf8),
    Msgs = lists:map(
        fun({_, _, Msg}) ->
            ok = metrics_inc(Ctx, 'messages.delivered'),
            Msg1 = emqx_hooks:run_fold(
                'message.delivered',
                [ClientInfo],
                Msg
            ),
            NMsg = emqx_mountpoint:unmount(Mountpoint, Msg1),
            #{
                node => NodeStr,
                id => emqx_guid:to_hexstr(emqx_message:id(NMsg)),
                qos => emqx_message:qos(NMsg),
                from => fmt_from(emqx_message:from(NMsg)),
                topic => emqx_message:topic(NMsg),
                payload => emqx_message:payload(NMsg),
                timestamp => emqx_message:timestamp(NMsg)
            }
        end,
        Delivers
    ),
    Req = #{messages => Msgs},
    {ok, dispatch(on_received_messages, Req, Channel)}.

-spec handle_timeout(reference(), Msg :: term(), channel()) ->
    {ok, channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_timeout(
    _TRef,
    {keepalive, _StatVal},
    Channel = #channel{keepalive = undefined}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    {keepalive, StatVal},
    Channel = #channel{keepalive = Keepalive}
) ->
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(keepalive, NChannel)};
        {error, timeout} ->
            Req = #{type => 'KEEPALIVE'},
            NChannel = remove_timer_ref(keepalive, Channel),
            %% close connection if keepalive timeout
            Replies = [{event, disconnected}, {close, keepalive_timeout}],
            NChannel1 = dispatch(on_timer_timeout, Req, NChannel#channel{
                closed_reason = keepalive_timeout
            }),
            {ok, Replies, NChannel1}
    end;
handle_timeout(_TRef, force_close, Channel = #channel{closed_reason = Reason}) ->
    {shutdown, Reason, Channel};
handle_timeout(_TRef, force_close_idle, Channel) ->
    {shutdown, idle_timeout, Channel};
handle_timeout(_TRef, connection_expire, Channel) ->
    NChannel = remove_timer_ref(connection_expire, Channel),
    {ok, [{event, disconnected}, {close, expired}], NChannel};
handle_timeout(_TRef, Msg, Channel) ->
    ?SLOG(warning, #{
        msg => "unexpected_timeout_signal",
        signal => Msg
    }),
    {ok, Channel}.

-spec handle_call(Req :: any(), From :: any(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {reply, Reply :: term(), replies(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}.

handle_call({send, Data}, _From, Channel) ->
    {reply, ok, [{outgoing, Data}], Channel};
handle_call(close, _From, Channel = #channel{conn_state = connected}) ->
    {reply, ok, [{event, disconnected}, {close, normal}], Channel};
handle_call(close, _From, Channel) ->
    {reply, ok, [{close, normal}], Channel};
handle_call(
    {auth, ClientInfo, _Password},
    _From,
    Channel = #channel{conn_state = connected}
) ->
    ?SLOG(warning, #{
        msg => "ingore_duplicated_authenticate_command",
        request_clientinfo => ClientInfo
    }),
    {reply, {error, ?RESP_PERMISSION_DENY, <<"Duplicated authenticate command">>}, Channel};
handle_call(
    {auth, ClientInfo, _Password},
    _From,
    Channel = #channel{conn_state = disconnected}
) ->
    ?SLOG(warning, #{
        msg => "authenticate_command_after_socket_disconnected",
        request_clientinfo => ClientInfo
    }),
    {reply, {error, ?RESP_PERMISSION_DENY, <<"Client socket disconnected">>}, Channel};
handle_call({auth, ClientInfo0, Password}, _From, Channel) ->
    ClientInfo1 = ClientInfo0#{password => Password},
    case
        emqx_utils:pipeline(
            [
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun enrich_clientinfo/2,
                fun set_log_meta/2,
                fun auth_connect/2
            ],
            ClientInfo1,
            Channel#channel{conn_state = connecting}
        )
    of
        {ok, _, NChannel} ->
            process_connect(NChannel);
        {error, Reason, NChannel} ->
            ?SLOG(warning, #{
                msg => "client_login_failed",
                clientid => maps:get(clientid, ClientInfo0, undefined),
                username => maps:get(username, ClientInfo0, undefined),
                reason => Reason
            }),
            {reply, {error, ?RESP_PERMISSION_DENY, Reason}, NChannel}
    end;
handle_call(
    {start_timer, keepalive, Interval},
    _From,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{keepalive => Interval},
    NClientInfo = ClientInfo#{keepalive => Interval},
    NChannel = Channel#channel{conninfo = NConnInfo, clientinfo = NClientInfo},
    {reply, ok, [{event, updated}], ensure_keepalive(cancel_timer(force_close_idle, NChannel))};
handle_call(
    {subscribe_from_client, TopicFilter, Qos},
    _From,
    Channel = #channel{
        ctx = Ctx,
        conn_state = connected,
        clientinfo = ClientInfo
    }
) ->
    Action = ?AUTHZ_SUBSCRIBE(Qos),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, TopicFilter) of
        deny ->
            {reply, {error, ?RESP_PERMISSION_DENY, <<"Authorization deny">>}, Channel};
        _ ->
            {ok, _, NChannel} = do_subscribe([{TopicFilter, #{qos => Qos}}], Channel),
            {reply, ok, [{event, updated}], NChannel}
    end;
handle_call({subscribe, Topic, SubOpts}, _From, Channel) ->
    {ok, [{NTopicFilter, NSubOpts}], NChannel} = do_subscribe([{Topic, SubOpts}], Channel),
    {reply, {ok, {NTopicFilter, NSubOpts}}, [{event, updated}], NChannel};
handle_call(
    {unsubscribe_from_client, TopicFilter},
    _From,
    Channel = #channel{conn_state = connected}
) ->
    {ok, NChannel} = do_unsubscribe([{TopicFilter, #{}}], Channel),
    {reply, ok, [{event, updated}], NChannel};
handle_call({unsubscribe, Topic}, _From, Channel) ->
    {ok, NChannel} = do_unsubscribe([Topic], Channel),
    {reply, ok, [{event, update}], NChannel};
handle_call(subscriptions, _From, Channel = #channel{subscriptions = Subs}) ->
    {reply, {ok, maps:to_list(Subs)}, Channel};
handle_call(
    {publish, Topic, Qos, Payload},
    _From,
    Channel = #channel{
        ctx = Ctx,
        conn_state = connected,
        clientinfo =
            ClientInfo =
                #{
                    clientid := From,
                    mountpoint := Mountpoint
                }
    }
) ->
    Action = ?AUTHZ_PUBLISH(Qos),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, Topic) of
        deny ->
            {reply, {error, ?RESP_PERMISSION_DENY, <<"Authorization deny">>}, Channel};
        _ ->
            Msg = emqx_message:make(From, Qos, Topic, Payload),
            NMsg = emqx_mountpoint:mount(Mountpoint, Msg),
            _ = emqx:publish(NMsg),
            {reply, ok, Channel}
    end;
handle_call(kick, _From, Channel) ->
    {reply, ok, [{event, disconnected}, {close, kicked}], Channel};
handle_call(discard, _From, Channel) ->
    {shutdown, discarded, ok, Channel};
handle_call(
    Req,
    _From,
    Channel = #channel{
        conn_state = ConnState,
        clientinfo = ClientInfo,
        closed_reason = ClosedReason
    }
) ->
    ?SLOG(warning, #{
        msg => "unexpected_call",
        call => Req,
        conn_state => ConnState,
        clientid => maps:get(clientid, ClientInfo, undefined),
        closed_reason => ClosedReason
    }),
    {reply, {error, unexpected_call}, Channel}.

-spec handle_cast(any(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_cast(Req, Channel) ->
    ?SLOG(warning, #{
        msg => "unexpected_call",
        call => Req
    }),
    {ok, Channel}.

-spec handle_info(any(), channel()) ->
    {ok, channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_info(
    {sock_closed, Reason},
    Channel = #channel{gcli = GClient, closed_reason = ClosedReason}
) ->
    case emqx_exproto_gcli:is_empty(GClient) of
        true ->
            Channel1 = ensure_disconnected(Reason, Channel),
            {shutdown, Reason, Channel1};
        _ ->
            %% delayed close process for flushing all callback funcs to gRPC server
            Channel1 =
                case ClosedReason of
                    undefined ->
                        Channel#channel{closed_reason = Reason};
                    _ ->
                        Channel
                end,
            Channel2 = ensure_timer(force_close, Channel1),
            {ok, ensure_disconnected(Reason, Channel2)}
    end;
handle_info(
    {hreply, FunName, Result},
    Channel0 = #channel{gcli = GClient0, timers = Timers}
) when
    FunName =:= on_socket_created;
    FunName =:= on_socket_closed;
    FunName =:= on_received_bytes;
    FunName =:= on_received_messages;
    FunName =:= on_timer_timeout
->
    GClient = emqx_exproto_gcli:ack(FunName, GClient0),
    Channel = Channel0#channel{gcli = GClient},

    ShutdownNow =
        emqx_exproto_gcli:is_empty(GClient) andalso
            maps:get(force_close, Timers, undefined) =/= undefined,
    case Result of
        ok when not ShutdownNow ->
            GClient1 = emqx_exproto_gcli:maybe_shoot(GClient),
            {ok, Channel#channel{gcli = GClient1}};
        ok when ShutdownNow ->
            Channel1 = cancel_timer(force_close, Channel),
            {shutdown, Channel1#channel.closed_reason, Channel1};
        {error, Reason} ->
            {shutdown, {error, {FunName, Reason}}, Channel}
    end;
handle_info({subscribe, _}, Channel) ->
    {ok, Channel};
handle_info(Info, Channel) ->
    ?SLOG(warning, #{
        msg => "unexpected_info",
        info => Info
    }),
    {ok, Channel}.

-spec terminate(any(), channel()) -> channel().
terminate(Reason, Channel) ->
    Req = #{reason => stringfy(Reason)},
    %% XXX: close streams?
    dispatch(on_socket_closed, Req, Channel).

%%--------------------------------------------------------------------
%% Sub/UnSub
%%--------------------------------------------------------------------

do_subscribe(TopicFilters, Channel) ->
    {MadeSubs, NChannel} = lists:foldl(
        fun({TopicFilter, SubOpts}, {MadeSubs, ChannelAcc}) ->
            {Sub, Channel1} = do_subscribe(TopicFilter, SubOpts, ChannelAcc),
            {MadeSubs ++ [Sub], Channel1}
        end,
        {[], Channel},
        parse_topic_filters(TopicFilters)
    ),
    {ok, MadeSubs, NChannel}.

%% @private
do_subscribe(
    TopicFilter,
    SubOpts,
    Channel =
        #channel{
            clientinfo = ClientInfo = #{mountpoint := Mountpoint},
            subscriptions = Subs
        }
) ->
    %% Mountpoint first
    NTopicFilter = emqx_mountpoint:mount(Mountpoint, TopicFilter),
    NSubOpts = maps:merge(emqx_gateway_utils:default_subopts(), SubOpts),
    SubId = maps:get(clientid, ClientInfo, undefined),
    %% XXX: is_new?
    IsNew = not maps:is_key(NTopicFilter, Subs),
    case IsNew of
        true ->
            ok = emqx:subscribe(NTopicFilter, SubId, NSubOpts),
            ok = emqx_hooks:run(
                'session.subscribed',
                [ClientInfo, NTopicFilter, NSubOpts#{is_new => IsNew}]
            ),
            {{NTopicFilter, NSubOpts}, Channel#channel{
                subscriptions = Subs#{NTopicFilter => NSubOpts}
            }};
        _ ->
            %% Update subopts
            ok = emqx:subscribe(NTopicFilter, SubId, NSubOpts),
            {{NTopicFilter, NSubOpts}, Channel#channel{
                subscriptions = Subs#{NTopicFilter => NSubOpts}
            }}
    end.

do_unsubscribe(TopicFilters, Channel) ->
    NChannel = lists:foldl(
        fun({TopicFilter, SubOpts}, ChannelAcc) ->
            do_unsubscribe(TopicFilter, SubOpts, ChannelAcc)
        end,
        Channel,
        parse_topic_filters(TopicFilters)
    ),
    {ok, NChannel}.

%% @private
do_unsubscribe(
    TopicFilter,
    UnSubOpts,
    Channel =
        #channel{
            clientinfo = ClientInfo = #{mountpoint := Mountpoint},
            subscriptions = Subs
        }
) ->
    NTopicFilter = emqx_mountpoint:mount(Mountpoint, TopicFilter),
    case maps:find(NTopicFilter, Subs) of
        {ok, SubOpts} ->
            ok = emqx:unsubscribe(NTopicFilter),
            ok = emqx_hooks:run(
                'session.unsubscribed',
                [ClientInfo, TopicFilter, maps:merge(SubOpts, UnSubOpts)]
            ),
            Channel#channel{subscriptions = maps:remove(NTopicFilter, Subs)};
        _ ->
            Channel
    end.

%% @private
parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Ensure & Hooks
%%--------------------------------------------------------------------

ensure_connected(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks(Ctx, 'client.connected', [ClientInfo, NConnInfo]),
    schedule_connection_expire(Channel#channel{
        conninfo = NConnInfo,
        conn_state = connected
    }).

schedule_connection_expire(Channel = #channel{ctx = Ctx, clientinfo = ClientInfo}) ->
    case emqx_gateway_ctx:connection_expire_interval(Ctx, ClientInfo) of
        undefined ->
            Channel;
        Interval ->
            ensure_timer(connection_expire, Interval, Channel)
    end.

ensure_disconnected(
    Reason,
    Channel = #channel{
        ctx = Ctx,
        conn_state = connected,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks(Ctx, 'client.disconnected', [ClientInfo, Reason, NConnInfo]),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected};
ensure_disconnected(_Reason, Channel = #channel{conninfo = ConnInfo}) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

metrics_inc(Ctx, Name) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name).

%%--------------------------------------------------------------------
%% Enrich Keepalive

ensure_keepalive(Channel = #channel{clientinfo = ClientInfo}) ->
    ensure_keepalive_timer(maps:get(keepalive, ClientInfo, 0), Channel).

ensure_keepalive_timer(Interval, Channel) when Interval =< 0 ->
    Channel;
ensure_keepalive_timer(Interval, Channel) ->
    StatVal = emqx_gateway_conn:keepalive_stats(recv),
    Keepalive = emqx_keepalive:init(default, StatVal, Interval),
    ensure_timer(keepalive, Channel#channel{keepalive = Keepalive}).

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso Time > 0 of
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    TRef = emqx_utils:start_timer(Time, Name),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, remove_timer_ref(Name, Channel)).

cancel_timer(Name, Channel = #channel{timers = Timers}) ->
    emqx_utils:cancel_timer(maps:get(Name, Timers, undefined)),
    remove_timer_ref(Name, Channel).

remove_timer_ref(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(force_close_idle, #channel{conninfo = #{idle_timeout := IdleTimeout}}) ->
    IdleTimeout;
interval(force_close, _) ->
    15000;
interval(keepalive, #channel{keepalive = Keepalive}) ->
    emqx_keepalive:info(check_interval, Keepalive).

%%--------------------------------------------------------------------
%% Dispatch
%%--------------------------------------------------------------------

dispatch(FunName, Req, Channel = #channel{gcli = GClient}) ->
    Req1 = Req#{conn => base64:encode(term_to_binary(self()))},
    NGClient = emqx_exproto_gcli:maybe_shoot(FunName, Req1, GClient),
    Channel#channel{gcli = NGClient}.

%%--------------------------------------------------------------------
%% Format
%%--------------------------------------------------------------------

enrich_conninfo(
    InClientInfo,
    Channel = #channel{
        conninfo = ConnInfo
    }
) ->
    Ks = [proto_name, proto_ver, clientid, username],
    NConnInfo = maps:merge(ConnInfo, maps:with(Ks, InClientInfo)),
    {ok, Channel#channel{conninfo = NConnInfo}}.

enrich_clientinfo(
    InClientInfo = #{proto_name := ProtoName},
    Channel = #channel{
        clientinfo = ClientInfo
    }
) ->
    Ks = [clientid, username],
    case maps:get(mountpoint, InClientInfo, <<>>) of
        <<>> ->
            ok;
        Mp ->
            ?SLOG(
                warning,
                #{
                    msg => "failed_to_override_mountpoint",
                    reason =>
                        "The mountpoint in AuthenticateRequest has been deprecated. "
                        "Please use the `gateway.exproto.mountpoint` configuration.",
                    requested_mountpoint => Mp,
                    configured_mountpoint => maps:get(mountpoint, ClientInfo)
                }
            )
    end,
    NClientInfo = maps:merge(ClientInfo, maps:with(Ks, InClientInfo)),
    NClientInfo1 = NClientInfo#{protocol => proto_name_to_protocol(ProtoName)},
    {ok, Channel#channel{clientinfo = NClientInfo1}}.

set_log_meta(_InClientInfo, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

run_conn_hooks(
    InClientInfo,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, InClientInfo, Channel}
    end.

auth_connect(
    _InClientInfo = #{password := Password},
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    #{
        clientid := ClientId,
        username := Username
    } = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo#{password => Password}) of
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

process_connect(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    #{
        clientid := ClientId,
        username := Username
    } = ClientInfo,
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
        {ok, _Session} ->
            ?SLOG(debug, #{
                msg => "client_login_succeed",
                clientid => ClientId,
                username => Username
            }),
            {reply, ok, [{event, connected}], ensure_connected(Channel)};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "client_login_failed",
                clientid => ClientId,
                username => Username,
                reason => Reason
            }),
            {reply, {error, ?RESP_PERMISSION_DENY, Reason}, Channel}
    end.

default_conninfo(ConnInfo) ->
    ConnInfo#{
        clean_start => true,
        clientid => emqx_gateway_utils:random_clientid(exproto),
        username => undefined,
        conn_props => #{},
        connected => true,
        proto_name => <<"exproto">>,
        proto_ver => <<"1.0">>,
        connected_at => erlang:system_time(millisecond),
        keepalive => 0,
        receive_maximum => 0,
        expiry_interval => 0
    }.

default_clientinfo(#{
    peername := {PeerHost, _} = PeerName,
    sockname := {_, SockPort},
    clientid := ClientId
}) ->
    #{
        zone => default,
        protocol => exproto,
        peerhost => PeerHost,
        peername => PeerName,
        sockport => SockPort,
        clientid => ClientId,
        username => undefined,
        is_bridge => false,
        is_superuser => false,
        mountpoint => undefined
    }.

stringfy(Reason) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Reason]))).

fmt_from(undefined) -> <<>>;
fmt_from(Bin) when is_binary(Bin) -> Bin;
fmt_from(T) -> stringfy(T).

proto_name_to_protocol(<<>>) ->
    exproto;
proto_name_to_protocol(ProtoName) when is_binary(ProtoName) ->
    binary_to_atom(ProtoName).
