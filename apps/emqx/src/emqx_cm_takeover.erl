%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_takeover).

-include("emqx_cm.hrl").
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
    %% NOTE: Convert pre-3.6.0 `#session{}` record into "exported" form.
    ChanRef = #chanref{proto = legacy, connmod = ConnMod, pid = ChanPid},
    {ok, ChanRef, from_legacy_session(Session)};
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
    {living, ConnMod, ChanPid, to_legacy_session(ClientId, ChanInfo, Session)};
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
    Ret = emqx_cm_proto_v4:takeover_finish(ConnMod, ChanPid, RequesterProto),
    from_finish_ret(ServerProto, Ret);
finish(#chanref{proto = legacy, connmod = ConnMod, pid = ChanPid}) ->
    ?tp(emqx_cm_takeover_finish_legacy, #{target_node => node(ChanPid)}),
    Ret = emqx_cm_proto_v3:takeover_finish(ConnMod, ChanPid),
    from_finish_ret(legacy, Ret).

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

%% erlfmt-ignore
to_legacy_session(ClientId, ChanInfo, Session) ->
    {session,
        ClientId,
        _Id = maps:get(id, Session),
        _IsPersistent = maps:get(is_persistent, Session),
        _Subscriptions = maps:get(subscriptions, Session),
        _MaxSubscriptions = infinity,
        _UpgradeQoS = false,
        _Inflight = to_legacy_inflight(maps:get(inflight, Session)),
        _MQueue = to_legacy_mqueue(ChanInfo, maps:get(mqueue, Session)),
        _NextPktId = maps:get(next_pkt_id, Session),
        _RetryInterval = infinity,
        _AwaitingRel = maps:get(awaiting_rel, Session),
        _MaxAwaitingRel = 100,
        _AwaitRelTimeout = timer:seconds(300),
        _CreatedAt = maps:get(created_at, Session)}.

%% erlfmt-ignore
from_legacy_session(Session) ->
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
        mqueue => export_legacy_mqueue(MQueue),
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

to_legacy_inflight(Inflight) ->
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
    {inflight, 0, Tree}.

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

%% -type squeue() :: {queue, [any()], [any()], non_neg_integer()}.
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

to_legacy_mqueue(#{clientinfo := #{zone := Zone}}, Queue) ->
    Len = length(Queue),
    PQueue = {queue, [], Queue, length(Queue)},
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
        _LastPrio = undefined, _PCredit = undefined}.

export_legacy_mqueue(MQueue) ->
    {mqueue, _StoreQoS0, _MaxLen, _Len, _Dropped, _PTable, _DefaultP, PQueue, _ShitOpts, _LastPrio,
        _PCredit} = MQueue,
    case PQueue of
        {queue, In, Out, _} ->
            Out ++ lists:reverse(In);
        {pqueue, Queues} ->
            lists:append([Out ++ lists:reverse(In) || {_P, {queue, In, Out, _}} <- Queues])
    end.
