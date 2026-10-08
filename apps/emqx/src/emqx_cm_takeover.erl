%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_takeover).

-include("emqx_cm.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    begin_/2,
    finish/1,
    begin_rpc/3,
    begin_rpc_legacy/2,
    begin_local/2,
    finish_rpc/3,
    finish_rpc_legacy/2,
    finish_local/2
]).

-export_type([
    protocol/0,
    channelref/0,
    state/0
]).

-export_type([session_legacy/0]).

%% Shared by gateway session takeover compatibility adapters.
-export([
    legacy_session_version/1,
    from_legacy_session/2,
    to_legacy_session/5
]).

-export_type([legacy_session_version/0]).

-type legacy_session_version() ::
    %% Pre-6.3 sessions:
    1
    %% Gateway sessions in 6.3.0/6.3.1:
    | 2.

-record(chanref, {
    proto :: local | protocol() | legacy,
    connmod :: module(),
    pid :: emqx_cm:chan_pid()
}).

-type protocol() :: #{vsn := pos_integer(), atom() := _}.
-type channelref() :: #chanref{}.
-type session() :: emqx_session_mem:exported().

-type state() :: session().

%% FIXME
-type session_legacy() :: tuple().

-define(BPAPI, emqx_cm).
-define(BPAPI_VSN_BASELINE, 4).

-define(VSN_TAKEOVER, 1).

%% v3 nodes:
%% -> emqx_cm_proto_v3 -> emqx_cm:takeover_session/2 ->
%%    {living, _ConnMod :: atom(), pid(), emqx_session:session()}

-spec current() -> protocol().
current() ->
    #{vsn => ?VSN_TAKEOVER}.

-doc "Begin a two-phase session takeover process".
-spec begin_(emqx_types:clientid(), pid()) ->
    {ok, channelref(), session()} | none.
begin_(ClientId, ChanPid) when node(ChanPid) =:= node() ->
    begin_local(ClientId, ChanPid);
begin_(ClientId, ChanPid) ->
    TargetNode = node(ChanPid),
    case emqx_bpapi:supported_version(TargetNode, ?BPAPI) of
        Vsn when is_integer(Vsn), Vsn >= ?BPAPI_VSN_BASELINE ->
            RequesterProto = current(),
            ?tp(emqx_cm_takeover_begin, #{
                clientid => ClientId,
                target_node => TargetNode,
                requester_proto => RequesterProto
            }),
            Ret = emqx_cm_proto_v4:takeover_begin(ClientId, ChanPid, RequesterProto),
            from_begin_ret(Ret);
        _ ->
            ?tp(emqx_cm_takeover_begin_legacy, #{
                clientid => ClientId,
                target_node => TargetNode
            }),
            Ret = emqx_cm_proto_v3:takeover_session(ClientId, ChanPid),
            upgrade_begin_ret(Ret)
    end.

-doc "Direct RPC target for `emqx_cm_proto_v4:takeover_begin/3`.".
-spec begin_rpc(emqx_types:clientid(), pid(), protocol()) ->
    {ok, channelref(), session()} | none.
begin_rpc(ClientId, ChanPid, RequesterProto) ->
    ?tp(emqx_cm_takeover_begin_rpc, #{
        clientid => ClientId,
        chanpid => ChanPid,
        requester_proto => RequesterProto
    }),
    Ret = begin_local(ClientId, ChanPid),
    to_begin_ret(RequesterProto, Ret).

-doc """
Indirect RPC target for `emqx_cm_proto_v{1..3}:takeover_session/2`.
See `emqx_cm:takeover_session/2`.
""".
-spec begin_rpc_legacy(emqx_types:clientid(), pid()) ->
    {living, module(), emqx_cm:chan_pid(), session_legacy()} | none.
begin_rpc_legacy(ClientId, ChanPid) ->
    ?tp(emqx_cm_takeover_begin_rpc_legacy, #{
        clientid => ClientId,
        chanpid => ChanPid
    }),
    case emqx_cm:do_get_chan_info(ClientId, ChanPid) of
        undefined ->
            none;
        ChanInfo ->
            Ret = begin_local(ClientId, ChanPid),
            downgrade_begin_ret(ClientId, ChanInfo, Ret)
    end.

begin_local(ClientId, ChanPid) when node(ChanPid) =:= node() ->
    case emqx_cm:do_get_chann_conn_mod(ClientId, ChanPid) of
        undefined ->
            none;
        ConnMod when is_atom(ConnMod) ->
            ChanRef = #chanref{proto = local, connmod = ConnMod, pid = ChanPid},
            case emqx_cm:request_stepdown({takeover, 'begin'}, ConnMod, ChanPid, ?T_TAKEOVER) of
                {ok, Session} ->
                    {ok, ChanRef, Session};
                {error, _Reason} ->
                    none
            end
    end.

-doc "Adapt takeover result received from remote node".
from_begin_ret(none) ->
    none;
from_begin_ret({ok, _ChanRef, _Session} = Ret) ->
    %% NOTE
    %% Any logic regarding adapting response from nodes running older EMQX version
    %% (according to `ChanRef#chanref.proto`) goes here. Currently, this is a no-op.
    Ret.

upgrade_begin_ret(none) ->
    none;
upgrade_begin_ret({living, ConnMod, ChanPid, Session}) ->
    %% NOTE: Convert pre-6.3.0 `#session{}` record into "exported" form.
    ChanRef = #chanref{proto = legacy, connmod = ConnMod, pid = ChanPid},
    {ok, ChanRef, from_legacy_session(Session, 1)};
upgrade_begin_ret({expired, _} = Ret) ->
    %% NOTE: Unsupported pre-5.3.0 stuff.
    error({unsupported, Ret});
upgrade_begin_ret({persistent, _} = Ret) ->
    %% NOTE: Unsupported pre-5.3.0 stuff.
    error({unsupported, Ret}).

to_begin_ret(#{vsn := _}, {ok, ChanRef, Session}) ->
    {ok, ChanRef#chanref{proto = current()}, Session};
to_begin_ret(_RequesterProto, none) ->
    none.

downgrade_begin_ret(ClientId, ChanInfo, {ok, ChanRef, Session}) ->
    %% NOTE: Turn back into pre-6.3.0 `#session{}` record.
    #chanref{connmod = ConnMod, pid = ChanPid} = ChanRef,
    {living, ConnMod, ChanPid, to_legacy_session(ClientId, ChanInfo, Session, #{}, 1)};
downgrade_begin_ret(_ClientId, _ChanInfo, none) ->
    none.

%%

-doc """
Conclude a two-phase session takeover process, of a channel specified by `channelref()`
obtained through `begin_/2`.
""".
-spec finish(channelref()) ->
    {ok, _ReplayContext} | {error, _Reason}.
finish(#chanref{proto = local, connmod = ConnMod, pid = ChanPid}) when node(ChanPid) =:= node() ->
    finish_local(ConnMod, ChanPid);
finish(#chanref{proto = #{} = ServerProto, connmod = ConnMod, pid = ChanPid}) ->
    RequesterProto = current(),
    ?tp(emqx_cm_takeover_finish, #{
        target_node => node(ChanPid),
        target_proto => ServerProto,
        requester_proto => RequesterProto
    }),
    Ret = finish_remote(fun() ->
        emqx_cm_proto_v4:takeover_finish(ConnMod, ChanPid, RequesterProto)
    end),
    from_finish_ret(ServerProto, Ret);
finish(#chanref{proto = legacy, connmod = ConnMod, pid = ChanPid}) ->
    ?tp(emqx_cm_takeover_finish_legacy, #{target_node => node(ChanPid)}),
    Ret = finish_remote(fun() ->
        emqx_cm_proto_v3:takeover_finish(ConnMod, ChanPid)
    end),
    from_finish_ret(legacy, Ret).

%% The proto calls are erpc-backed: a node dying between takeover-begin and
%% takeover-end raises instead of returning, which would propagate through the
%% new channel's CONNECT rather than hit the session-open branches that degrade
%% to local-only replay.  Convert the raises to `{error, _}'.
finish_remote(ProtoCall) ->
    try
        ProtoCall()
    catch
        error:{erpc, Reason} ->
            {error, {erpc, Reason}};
        error:{exception, Reason, _Stack} ->
            {error, Reason}
    end.

-doc "Direct RPC target for `emqx_cm_proto_v4:takeover_finish/3`.".
-spec finish_rpc(module(), emqx_cm:chan_pid(), legacy | protocol()) ->
    {ok, _Pendings} | {error, term()}.
finish_rpc(ConnMod, ChanPid, RequesterProto) ->
    ?tp(emqx_cm_takeover_finish_rpc, #{
        chanpid => ChanPid,
        requester_proto => RequesterProto
    }),
    Ret = finish_local(ConnMod, ChanPid),
    to_finish_ret(RequesterProto, Ret).

-doc """
Indirect RPC target for `emqx_cm_proto_v{1..3}:takeover_finish/2`.
See `emqx_cm:takeover_finish/2`.
""".
-spec finish_rpc_legacy(module(), emqx_cm:chan_pid()) ->
    {ok, _Pendings} | {error, term()}.
finish_rpc_legacy(ConnMod, ChanPid) ->
    ?tp(emqx_cm_takeover_finish_rpc_legacy, #{chanpid => ChanPid}),
    Ret = finish_local(ConnMod, ChanPid),
    to_finish_ret(legacy, Ret).

-spec finish_local(module(), emqx_cm:chan_pid()) ->
    {ok, _ReplayContext} | {error, _Reason}.
finish_local(ConnMod, ChanPid) ->
    emqx_cm:request_stepdown({takeover, 'end'}, ConnMod, ChanPid, ?T_TAKEOVER).

from_finish_ret(_Proto, {ok, ReplayContext}) ->
    {ok, ReplayContext};
from_finish_ret(_Proto, {error, Reason}) ->
    {error, Reason}.

to_finish_ret(_Proto, {ok, ReplayContext}) ->
    {ok, ReplayContext};
to_finish_ret(_Proto, {error, Reason}) ->
    {error, Reason}.

%% Compatibility

-spec legacy_session_version(node()) -> legacy_session_version().
legacy_session_version(Node) ->
    %% The MQTT takeover BPAPI was introduced together with the 6.3 record layout.
    %% Gateway takeover continued transferring raw records in 6.3.0 and 6.3.1.
    case emqx_bpapi:supported_version(Node, ?BPAPI) of
        Vsn when is_integer(Vsn), Vsn >= ?BPAPI_VSN_BASELINE ->
            2;
        _ ->
            1
    end.

%% Pre-6.3.0 in-memory session has the following shape:
%% -record(session, {
%%     clientid :: emqx_types:clientid(),
%%     id :: emqx_session:session_id(),
%%     is_persistent :: boolean(),
%%     subscriptions :: map(),
%%     max_subscriptions :: non_neg_integer() | infinity,
%%     upgrade_qos = false :: boolean(),
%%     inflight :: emqx_inflight:inflight(),
%%     mqueue :: emqx_mqueue:mqueue(),
%%     next_pkt_id = 1 :: emqx_types:packet_id(),
%%     retry_interval :: timeout(),
%%     awaiting_rel :: map(),
%%     max_awaiting_rel :: non_neg_integer() | infinity,
%%     await_rel_timeout :: timeout(),
%%     created_at :: pos_integer()
%% }).

%% 6.3.0/6.3.1 in-memory session has the following shape:
%% -record(session, {
%%     id :: emqx_session:session_id(),
%%     is_persistent :: boolean(),
%%     subscriptions :: emqx_session_mem:subscriptions(),
%%     max_subscriptions :: non_neg_integer() | infinity,
%%     upgrade_qos = false :: boolean(),
%%     inflight :: emqx_inflight:inflight(),
%%     mqueue :: emqx_mqueue:mqueue(),
%%     quota :: emqx_limiter_client_container:t() | false | {lazy, emqx_limiter:listener_id()},
%%     next_pkt_id = 1 :: emqx_types:packet_id(),
%%     retry_interval :: timeout(),
%%     awaiting_rel :: emqx_session_mem:awaiting_rel(),
%%     max_awaiting_rel :: non_neg_integer() | infinity,
%%     await_rel_timeout :: timeout(),
%%     created_at :: pos_integer()
%% }).

%% Legacy MQTT-SN peers resume raw session records without reapplying
%% limits or timers. Populate these fields from Conf when encoding.
%% erlfmt-ignore
to_legacy_session(ClientId, ChanInfo, Session, Conf, 1) ->
    {session, ClientId, _Id = maps:get(id, Session),
        _IsPersistent = maps:get(is_persistent, Session),
        _Subscriptions = maps:get(subscriptions, Session),
        _MaxSubscriptions = maps:get(max_subscriptions, Conf, infinity),
        _UpgradeQoS = maps:get(upgrade_qos, Conf, false),
        _Inflight = to_legacy_inflight(
            maps:get(inflight, Session),
            maps:get(receive_maximum, Conf, 0)
        ),
        _MQueue = to_legacy_mqueue(ChanInfo, maps:get(mqueue, Session), 1),
        _NextPktId = maps:get(next_pkt_id, Session),
        _RetryInterval = maps:get(retry_interval, Conf, infinity),
        _AwaitingRel = maps:get(awaiting_rel, Session),
        _MaxAwaitingRel = maps:get(max_awaiting_rel, Conf, 100),
        _AwaitRelTimeout = maps:get(await_rel_timeout, Conf, timer:seconds(300)),
        _CreatedAt = maps:get(created_at, Session)};
to_legacy_session(_ClientId, ChanInfo, Session, Conf, 2) ->
    {session, maps:get(id, Session), _IsPersistent = maps:get(is_persistent, Session),
        _Subscriptions = maps:get(subscriptions, Session),
        _MaxSubscriptions = maps:get(max_subscriptions, Conf, infinity),
        _UpgradeQoS = maps:get(upgrade_qos, Conf, false),
        _Inflight = to_legacy_inflight(
            maps:get(inflight, Session),
            maps:get(receive_maximum, Conf, 0)
        ),
        _MQueue = to_legacy_mqueue(ChanInfo, maps:get(mqueue, Session), 2),
        %% MQTT-SN uses quota = false:
        _Quota = false,
        _NextPktId = maps:get(next_pkt_id, Session),
        _RetryInterval = maps:get(retry_interval, Conf, infinity),
        _AwaitingRel = maps:get(awaiting_rel, Session),
        _MaxAwaitingRel = maps:get(max_awaiting_rel, Conf, 100),
        _AwaitRelTimeout = maps:get(await_rel_timeout, Conf, timer:seconds(300)),
        _CreatedAt = maps:get(created_at, Session)}.

%% erlfmt-ignore
from_legacy_session(Session, 1) ->
    {session,
        _ClientId,
        Id,
        IsPersistent,
        Subscriptions,
        _MaxSubscriptions,
        _UpgradeQoS,
        Inflight,
        MQueue,
        NextPacketId,
        _RetryInterval,
        AwaitingRel,
        _MaxAwaitingRel,
        _AwaitRelTimeout,
        CreatedAt
    } = Session,
    #{
        id => Id,
        is_persistent => IsPersistent,
        subscriptions => Subscriptions,
        inflight => export_legacy_inflight(Inflight),
        mqueue => export_legacy_mqueue(MQueue, 1),
        next_pkt_id => NextPacketId,
        awaiting_rel => AwaitingRel,
        created_at => CreatedAt
    };
from_legacy_session(Session, 2) ->
    {session,
        Id,
        IsPersistent,
        Subscriptions,
        _MaxSubscriptions,
        _UpgradeQoS,
        Inflight,
        MQueue,
        _Quota,
        NextPacketId,
        _RetryInterval,
        AwaitingRel,
        _MaxAwaitingRel,
        _AwaitRelTimeout,
        CreatedAt
    } = Session,
    #{
        id => Id,
        is_persistent => IsPersistent,
        subscriptions => Subscriptions,
        inflight => export_legacy_inflight(Inflight),
        mqueue => export_legacy_mqueue(MQueue, 2),
        next_pkt_id => NextPacketId,
        awaiting_rel => AwaitingRel,
        created_at => CreatedAt
    }.

%% -opaque inflight() :: {inflight, max_size(), gb_trees:tree()}.
%% -record(inflight_data, {
%%     phase :: inflight_data_phase(),
%%     message :: emqx_types:message(),
%%     timestamp :: non_neg_integer()
%% }).

to_legacy_inflight(Inflight, MaxInflight) ->
    Tree = lists:foldl(
        fun(
            #{
                packet_id := PacketId,
                phase := Phase,
                message := Message,
                timestamp := Timestamp
            },
            Acc
        ) ->
            gb_trees:insert(PacketId, {inflight_data, Phase, Message, Timestamp}, Acc)
        end,
        gb_trees:empty(),
        Inflight
    ),
    {inflight, MaxInflight, Tree}.

export_legacy_inflight({inflight, _, Tree}) ->
    [
        #{
            packet_id => PacketId,
            phase => Phase,
            message => Message,
            timestamp => Timestamp
        }
     || {PacketId, {inflight_data, Phase, Message, Timestamp}} <- gb_trees:to_list(Tree)
    ].

%% Pre-6.3.0 mqueue (version 1) has the following shape:
%% -type squeue() :: {queue, _In :: [any()], _Out :: [any()], _Len :: non_neg_integer()}.
%%  ^ `In` holds newest messages first, `Out` holds oldest messages first.
%% -type pq() :: squeue() | {pqueue, [{priority(), squeue()}]}.
%% -record(shift_opts, {multiplier :: non_neg_integer(), base :: integer()}).
%% -record(mqueue, {
%%     store_qos0 = false :: boolean(),
%%     max_len = ?MAX_LEN_INFINITY :: count(),
%%     len = 0 :: count(),
%%     dropped = 0 :: count(),
%%     p_table = ?NO_PRIORITY_TABLE :: p_table(),
%%     default_p = ?LOWEST_PRIORITY :: priority(),
%%     q = emqx_pqueue:new() :: pq(),
%%     shift_opts :: #shift_opts{},
%%     last_prio :: non_neg_integer() | undefined,
%%     p_credit :: non_neg_integer() | undefined
%% }).

%% 6.3.0/6.3.1 mqueue (version 2) has the following shape:
%% -record(prios, {
%%     t :: p_table(),
%%     default :: priority(),
%%     shift_mult :: non_neg_integer(),
%%     shift_base :: integer()
%% }).
%% -type pq() :: squeue() | cqueue() | {pqueue, [{priority(), squeue() | cqueue()}]}.
%% -record(mqueue, {
%%     store_qos0 = false :: boolean(),
%%     max_len = ?MAX_LEN_INFINITY :: count(),
%%     dropped = 0 :: count(),
%%     payload_bytes = 0 :: count(),
%%     q = emqx_pqueue:new() :: pq(),
%%     prios = ?NO_PRIORITY_TABLE :: #prios{} | ?NO_PRIORITY_TABLE,
%%     p_credit :: non_neg_integer() | undefined
%% }).
%% ^ Length is stored in q
%% ^ `payload_bytes` counts queued message payload bytes.
%% ^ Priority configuration and shift options moved into prios (or disabled).
%%
%% Version 2 also supports two-lane queues to distinguish QoS0 messages:
%% -type cqueue() :: {
%%     _HeadLane :: default | qos0,
%%     _TailLane :: default | qos0,
%%     _DefaultIn :: [any()],
%%     _DefaultOut :: [any()],
%%     _QoS0In :: [any()],
%%     _QoS0Out :: [any()],
%%     _Len :: non_neg_integer()
%% }.

%% erlfmt-ignore
to_legacy_mqueue(#{clientinfo := #{zone := Zone}}, Queue, 1) ->
    %% NOTE
    %% For simplicity, legacy conversion ignores message priorities and builds a simple queue.
    %% Sort by insertion timestamp so pagination remains valid after flattening priorities.
    Len = length(Queue),
    PQueue = {queue, [], sort_mqueue_by_timestamp(Queue), Len},
    MaxLen = emqx_config:get_zone_conf(Zone, [mqtt, max_mqueue_len]),
    StoreQoS0 = emqx_config:get_zone_conf(Zone, [mqtt, mqueue_store_qos0]),
    PTable =
        case emqx_config:get_zone_conf(Zone, [mqtt, mqueue_priorities]) of
            disabled ->
                disabled;
            Priorities ->
                %% topic from mqtt.mqueue_priorities(map()) is atom.
                emqx_utils_maps:binary_key_map(Priorities)
        end,
    DefaultPrio =
        case emqx_config:get_zone_conf(Zone, [mqtt, mqueue_default_priority]) of
            lowest -> 0;
            highest -> infinity;
            N -> N
        end,
    %% NOTE: Computing `#shift_opts{}` was subtly broken, just use baseline.
    ShiftOpts = {shift_opts, 10, 0},
    {mqueue, StoreQoS0, MaxLen, Len, _Dropped = 0, PTable, DefaultPrio, PQueue, ShiftOpts,
        _LastPrio = undefined, _PCredit = undefined};
to_legacy_mqueue(ChanInfo, Messages, 2) ->
    %% NOTE
    %% 6.3.0/6.3.1 queues store a boolean instead of the current QoS0 count.
    %% Their remaining fields and priority/class queue representation are unchanged.
    {mqueue,
        StoreQoS0,
        MaxLen,
        _Len,
        Dropped,
        PTable,
        DefaultPrio,
        PQueue,
        {shift_opts, ShiftMult, ShiftBase},
        _LastPrio,
        PCredit
    } = to_legacy_mqueue(ChanInfo, Messages, 1),
    Bytes = lists:sum([emqx_message:payload_size(Msg) || Msg <- Messages]),
    Prios =
        case PTable of
            disabled ->
                disabled;
            #{} ->
                {prios, PTable, DefaultPrio, ShiftMult, ShiftBase}
        end,
    {mqueue, StoreQoS0, MaxLen, Dropped, Bytes, PQueue, Prios, PCredit}.

%% erlfmt-ignore
export_legacy_mqueue(MQueue, 1) ->
    {mqueue,
        _StoreQoS0,
        _MaxLen,
        _Len,
        _Dropped,
        _PTable,
        _DefaultP,
        PQueue,
        _ShitOpts,
        _LastPrio,
        _PCredit
    } = MQueue,
    sort_mqueue_by_timestamp(export_legacy_pqueue(PQueue, 1));
export_legacy_mqueue(MQueue, 2) ->
    {mqueue, _StoreQoS0, _MaxLen, _Dropped, _Bytes, PQueue, _Prios, _Credit} = MQueue,
    sort_mqueue_by_timestamp(export_legacy_pqueue(PQueue, 2)).

%% Drain the pre-6.3 and 6.3.0/6.3.1 pqueue representations.
%% Flatten priorities in stored order, ignoring credits.
export_legacy_pqueue({queue, In, Out, _Len}, _Vsn) ->
    Out ++ lists:reverse(In);
export_legacy_pqueue({pqueue, Queues}, Vsn) ->
    lists:append([export_legacy_pqueue(Q, Vsn) || {_Priority, Q} <- Queues]);
export_legacy_pqueue(CQueue = {_, _, _, _, _, _, _}, 2) ->
    export_legacy_cqueue(CQueue, 2).

export_legacy_cqueue(CQueue, 2) ->
    case cqueue_out(CQueue) of
        {empty, _} ->
            [];
        {{value, Msg}, Rest} ->
            [Msg | export_legacy_cqueue(Rest, 2)]
    end.

sort_mqueue_by_timestamp(Messages) ->
    [Msg || {_, Msg} <- lists:keysort(1, [{mqueue_timestamp(Msg), Msg} || Msg <- Messages])].

mqueue_timestamp(#message{extra = #{mqueue_insert_ts := Ts}}) ->
    Ts.

%% 6.3.0/6.3.1 class-queue decoder, copied verbatim from `emqx_pqueue` @ 6.3.1.
%% Keep this copy independent of future queue implementation changes.

-define(switch, '$switch').
-define(switch_(N), {'$switch', N}).

cqueue_new() ->
    {default, default, [], [], [], [], 0}.

cqueue_out({_HeadLane, _TailLane, _, _, _, _, 0} = Q) ->
    {empty, Q};
cqueue_out({HeadLane, TailLane, In, Out, Q0In, Q0Out, L}) ->
    cqueue_out(HeadLane, TailLane, In, Out, Q0In, Q0Out, L).

cqueue_out(_HeadLane, _TailLane, _, _, _, _, 0) ->
    {empty, cqueue_new()};
cqueue_out(default, TL, In, Out, Q0In, Q0Out, L) ->
    case Out of
        [?switch | Rest] ->
            cqueue_out(qos0, TL, In, Rest, Q0In, Q0Out, L);
        [?switch_(N) | Rest] ->
            cqueue_out(qos0, TL, In, [cq_mk_switch(N - 1) | Rest], Q0In, Q0Out, L);
        [V | Rest] ->
            {{value, V}, {default, TL, In, Rest, Q0In, Q0Out, L - 1}};
        [] when In =:= [] ->
            cqueue_out(qos0, TL, In, Out, Q0In, Q0Out, L);
        [] ->
            NOut = lists:reverse(In, []),
            cqueue_out(default, TL, [], NOut, Q0In, Q0Out, L)
    end;
cqueue_out(qos0, TL, In, Out, Q0In, Q0Out, L) ->
    case Q0Out of
        [?switch | Rest] ->
            cqueue_out(default, TL, In, Out, Q0In, Rest, L);
        [?switch_(N) | Rest] ->
            cqueue_out(default, TL, In, Out, Q0In, [cq_mk_switch(N - 1) | Rest], L);
        [V | Rest] ->
            {{value, V}, {qos0, TL, In, Out, Q0In, Rest, L - 1}};
        [] when Q0In =:= [] ->
            cqueue_out(default, TL, In, Out, Q0In, Q0Out, L);
        [] ->
            NOut = lists:reverse(Q0In, []),
            cqueue_out(qos0, TL, In, Out, [], NOut, L)
    end.

cq_mk_switch(1) ->
    ?switch;
cq_mk_switch(N) ->
    ?switch_(N).
