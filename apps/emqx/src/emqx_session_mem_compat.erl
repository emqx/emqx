%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_mem_compat).

-moduledoc """
Layouts of the in-memory session (`emqx_session_mem`) that cross nodes in a
takeover, and the conversions between them.

A session crosses nodes in one of these forms:

- `exported`: the map of `emqx_session_mem:export/1`. Nodes of 6.3.0 and
  later send it for MQTT. Its `vsn` key names the version of the map. Nodes
  of 6.3.0 to 6.3.1 send no `vsn`, which is version 1. Increase the version
  in `emqx_session_mem:export/1` when a key is added, removed or renamed, or
  when the meaning of a value changes. `emqx_session_mem:import/2` ignores
  keys it does not know.
- `v63`: the `#session{}` record of EMQX 6.3.0 to 6.3.2. The MQTT-SN gateway
  of those versions sends and expects it.
- `pre63`: the `#session{}` record of EMQX 5.8 to 6.2. MQTT takeovers through
  `emqx_cm_proto_v1` to `v3`, and the MQTT-SN gateway of those versions, send
  and expect it.

Both records have the tag `session` on the wire. This module defines them as
`session_v63` and `session_pre63` and changes the tag when a record enters or
leaves this module. The same applies to the `#mqueue{}` records.

A record built by `from_exported/4` has built values in all fields: the
receiving versions have no clause for the `{empty, MaxLen}` mqueue or the
`{lazy, ListenerId}` limiter placeholders of this version.
""".

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([
    detect/1,
    to_exported/1,
    from_exported/3,
    from_exported/4,
    layout_for_peer/1,
    fields/1
]).

-export_type([
    layout/0,
    format/0,
    legacy_session/0,
    from_exported_opts/0
]).

-type layout() :: v63 | pre63.

-type format() :: exported | layout().

-type legacy_session() :: tuple().

-type from_exported_opts() :: #{
    %% The inflight window of the session. 0 means no limit.
    receive_maximum => non_neg_integer(),
    %% The session configuration. Defaults to the zone configuration.
    conf => emqx_session:conf()
}.

-record(inflight_data, {
    phase :: wait_ack | wait_comp,
    message :: emqx_types:message(),
    timestamp :: non_neg_integer()
}).

%% The `emqx_inflight:inflight()` of EMQX 5.8 to 6.3.2, with
%% `#inflight_data{}` values. Both names match the tags on the wire.
-record(inflight, {
    max_size :: non_neg_integer(),
    tree :: gb_trees:tree(emqx_types:packet_id(), #inflight_data{})
}).

%% The `#mqueue{}` of EMQX 6.3.0 and 6.3.1. EMQX 6.3.2 replaces `store_qos0`
%% with a QoS 0 message counter, which is `false` when QoS 0 messages are
%% not stored.
-record(mqueue_v63, {
    store_qos0 :: boolean() | non_neg_integer(),
    max_len :: non_neg_integer(),
    dropped :: non_neg_integer(),
    payload_bytes :: non_neg_integer(),
    q :: emqx_pqueue:q(),
    prios :: tuple() | disabled,
    p_credit :: non_neg_integer() | undefined
}).

-record(shift_opts, {
    multiplier :: non_neg_integer(),
    base :: integer()
}).

%% The `#mqueue{}` of EMQX 5.8 to 6.2.
-record(mqueue_pre63, {
    store_qos0 :: boolean(),
    max_len :: non_neg_integer(),
    len :: non_neg_integer(),
    dropped :: non_neg_integer(),
    p_table :: map() | disabled,
    default_p :: integer() | infinity,
    q :: tuple(),
    shift_opts :: #shift_opts{},
    last_prio :: non_neg_integer() | undefined,
    p_credit :: non_neg_integer() | undefined
}).

%% The `#session{}` of EMQX 6.3.0 to 6.3.2.
-record(session_v63, {
    id :: emqx_session:session_id(),
    is_persistent :: boolean(),
    subscriptions :: emqx_session_mem:subscriptions(),
    max_subscriptions :: non_neg_integer() | infinity,
    upgrade_qos :: boolean(),
    inflight :: #inflight{},
    %% A `#mqueue_v63{}` with the tag `mqueue`.
    mqueue :: tuple() | {empty, non_neg_integer()},
    quota :: false | tuple(),
    next_pkt_id :: emqx_types:packet_id(),
    retry_interval :: timeout(),
    awaiting_rel :: emqx_session_mem:awaiting_rel(),
    max_awaiting_rel :: non_neg_integer() | infinity,
    await_rel_timeout :: timeout(),
    created_at :: pos_integer()
}).

%% The `#session{}` of EMQX 5.8 to 6.2.
-record(session_pre63, {
    clientid :: emqx_types:clientid(),
    id :: emqx_session:session_id(),
    is_persistent :: boolean(),
    subscriptions :: map(),
    max_subscriptions :: non_neg_integer() | infinity,
    upgrade_qos :: boolean(),
    inflight :: #inflight{},
    %% A `#mqueue_pre63{}` with the tag `mqueue`.
    mqueue :: tuple(),
    next_pkt_id :: emqx_types:packet_id(),
    retry_interval :: timeout(),
    awaiting_rel :: map(),
    max_awaiting_rel :: non_neg_integer() | infinity,
    await_rel_timeout :: timeout(),
    created_at :: pos_integer()
}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-doc """
Return the layout of a session received from another node.

- A map with a `vsn` key is `{exported, Vsn}`.
- A map with the keys of `emqx_session_mem:export/1` and no `vsn` is
  `{exported, 1}`.
- Both records have 14 fields. The `v63` record starts with the session id
  and the persistence flag (a boolean). The `pre63` record starts with the
  client id and the session id (a binary).
""".
-spec detect(term()) -> {exported, pos_integer()} | {legacy, layout()} | unknown.
detect(#{vsn := Vsn}) ->
    {exported, Vsn};
detect(#{id := _, inflight := _, mqueue := _, next_pkt_id := _}) ->
    {exported, 1};
detect(Term) when is_tuple(Term), tuple_size(Term) =:= 15, element(1, Term) =:= session ->
    case element(3, Term) of
        Flag when is_boolean(Flag) -> {legacy, v63};
        SessionId when is_binary(SessionId) -> {legacy, pre63};
        _ -> unknown
    end;
detect(_Term) ->
    unknown.

-doc """
Convert a session record of the `v63` or `pre63` layout to the exported
form, for `emqx_session_mem:import/2`. An exported session is returned as
is.
""".
-spec to_exported(legacy_session() | emqx_session_mem:exported()) ->
    emqx_session_mem:exported().
to_exported(Session) ->
    case detect(Session) of
        {exported, _Vsn} -> Session;
        {legacy, v63} -> v63_to_exported(setelement(1, Session, session_v63));
        {legacy, pre63} -> pre63_to_exported(setelement(1, Session, session_pre63));
        unknown -> error({unknown_session_layout, Session})
    end.

-doc "Same as `from_exported/4` with the default options.".
-spec from_exported(format(), emqx_types:clientinfo(), emqx_session_mem:exported()) ->
    emqx_session_mem:exported() | legacy_session().
from_exported(Format, ClientInfo, Exported) ->
    from_exported(Format, ClientInfo, Exported, #{}).

-doc """
Convert an exported session to the given layout. `ClientInfo` must have the
`zone` key; the `pre63` layout also needs `clientid`. The mqueue is built
from the zone configuration. The session configuration and the inflight
window come from `Opts`, because a receiving MQTT-SN gateway uses the
record as it is. Both records have no delivery limiter (`quota` is `false`
in the `v63` layout), as the MQTT-SN gateway disables it.
""".
-spec from_exported(
    format(), emqx_types:clientinfo(), emqx_session_mem:exported(), from_exported_opts()
) ->
    emqx_session_mem:exported() | legacy_session().
from_exported(exported, _ClientInfo, Exported, _Opts) ->
    Exported;
from_exported(v63, ClientInfo, Exported, Opts) ->
    setelement(1, exported_to_v63(ClientInfo, Exported, Opts), session);
from_exported(pre63, ClientInfo, Exported, Opts) ->
    setelement(1, exported_to_pre63(ClientInfo, Exported, Opts), session).

-doc """
Return the session layout that a takeover requester on `Node` expects when
it asks without options. A requester on this node gets `exported`.

| `emqx_gateway_cm` BPAPI | `emqx_cm` BPAPI | Versions     | Layout     |
|-------------------------|-----------------|--------------|------------|
| 3 or later              | any             | 6.3.2 and up | `exported` |
| 1 or 2                  | 4 or later      | 6.3.0, 6.3.1 | `v63`      |
| 1 or 2                  | 1 to 3          | 5.8 to 6.2   | `pre63`    |
""".
-spec layout_for_peer(node()) -> format().
layout_for_peer(Node) when Node =:= node() ->
    exported;
layout_for_peer(Node) ->
    case emqx_bpapi:supported_version(Node, emqx_gateway_cm) of
        GwVsn when is_integer(GwVsn), GwVsn >= 3 ->
            exported;
        _ ->
            case emqx_bpapi:supported_version(Node, emqx_cm) of
                CmVsn when is_integer(CmVsn), CmVsn >= 4 -> v63;
                _ -> pre63
            end
    end.

-doc """
Return the field names of a record layout. The `v63` fields must equal the
fields of the `#session{}` record of this version.
""".
-spec fields(layout()) -> [atom()].
fields(v63) ->
    record_info(fields, session_v63);
fields(pre63) ->
    record_info(fields, session_pre63).

%%--------------------------------------------------------------------
%% v63
%%--------------------------------------------------------------------

v63_to_exported(#session_v63{
    id = Id,
    is_persistent = IsPersistent,
    subscriptions = Subscriptions,
    inflight = Inflight,
    mqueue = MQueue,
    next_pkt_id = NextPktId,
    awaiting_rel = AwaitingRel,
    created_at = CreatedAt
}) ->
    #{
        id => Id,
        is_persistent => IsPersistent,
        subscriptions => Subscriptions,
        inflight => inflight_to_list(Inflight),
        mqueue => v63_mqueue_to_list(MQueue),
        next_pkt_id => NextPktId,
        awaiting_rel => AwaitingRel,
        created_at => CreatedAt
    }.

exported_to_v63(ClientInfo, Exported, Opts) ->
    Conf = session_conf(ClientInfo, Opts),
    #session_v63{
        id = maps:get(id, Exported),
        is_persistent = maps:get(is_persistent, Exported),
        subscriptions = maps:get(subscriptions, Exported),
        max_subscriptions = maps:get(max_subscriptions, Conf),
        upgrade_qos = maps:get(upgrade_qos, Conf),
        inflight = list_to_inflight(Opts, maps:get(inflight, Exported)),
        mqueue = list_to_v63_mqueue(ClientInfo, maps:get(mqueue, Exported)),
        quota = false,
        next_pkt_id = maps:get(next_pkt_id, Exported),
        retry_interval = maps:get(retry_interval, Conf),
        awaiting_rel = maps:get(awaiting_rel, Exported),
        max_awaiting_rel = maps:get(max_awaiting_rel, Conf),
        await_rel_timeout = maps:get(await_rel_timeout, Conf),
        created_at = maps:get(created_at, Exported)
    }.

%% A live session of this version may hold the `{empty, MaxLen}` placeholder.
v63_mqueue_to_list({empty, _MaxLen}) ->
    [];
v63_mqueue_to_list(MQueue) when element(1, MQueue) =:= mqueue ->
    #mqueue_v63{store_qos0 = StoreQoS0, q = Q} = MQ = setelement(1, MQueue, mqueue_v63),
    NumQoS0 =
        case StoreQoS0 of
            true -> count_qos0(Q);
            _ -> StoreQoS0
        end,
    emqx_mqueue:to_list(setelement(1, MQ#mqueue_v63{store_qos0 = NumQoS0}, mqueue)).

list_to_v63_mqueue(ClientInfo, Messages) ->
    MQ0 = lists:foldl(
        fun(Msg, Acc) ->
            case emqx_mqueue:in(Msg, Acc) of
                {_Dropped, Acc1} -> Acc1;
                false -> Acc
            end
        end,
        emqx_session_mem:new_mqueue(ClientInfo),
        Messages
    ),
    StoreQoS0 = emqx_mqueue:info(store_qos0, MQ0),
    MQ = setelement(1, MQ0, mqueue_v63),
    setelement(1, MQ#mqueue_v63{store_qos0 = StoreQoS0}, mqueue).

count_qos0(Q) ->
    emqx_pqueue:fold(
        fun
            (#message{qos = ?QOS_0}, _Prio, N) -> N + 1;
            (_Msg, _Prio, N) -> N
        end,
        0,
        Q
    ).

%%--------------------------------------------------------------------
%% pre63
%%--------------------------------------------------------------------

pre63_to_exported(#session_pre63{
    id = Id,
    is_persistent = IsPersistent,
    subscriptions = Subscriptions,
    inflight = Inflight,
    mqueue = MQueue,
    next_pkt_id = NextPktId,
    awaiting_rel = AwaitingRel,
    created_at = CreatedAt
}) ->
    #{
        id => Id,
        is_persistent => IsPersistent,
        subscriptions => Subscriptions,
        inflight => inflight_to_list(Inflight),
        mqueue => pre63_mqueue_to_list(MQueue),
        next_pkt_id => NextPktId,
        awaiting_rel => AwaitingRel,
        created_at => CreatedAt
    }.

exported_to_pre63(ClientInfo = #{clientid := ClientId}, Exported, Opts) ->
    Conf = session_conf(ClientInfo, Opts),
    #session_pre63{
        clientid = ClientId,
        id = maps:get(id, Exported),
        is_persistent = maps:get(is_persistent, Exported),
        subscriptions = maps:get(subscriptions, Exported),
        max_subscriptions = maps:get(max_subscriptions, Conf),
        upgrade_qos = maps:get(upgrade_qos, Conf),
        inflight = list_to_inflight(Opts, maps:get(inflight, Exported)),
        mqueue = list_to_pre63_mqueue(ClientInfo, maps:get(mqueue, Exported)),
        next_pkt_id = maps:get(next_pkt_id, Exported),
        retry_interval = maps:get(retry_interval, Conf),
        awaiting_rel = maps:get(awaiting_rel, Exported),
        max_awaiting_rel = maps:get(max_awaiting_rel, Conf),
        await_rel_timeout = maps:get(await_rel_timeout, Conf),
        created_at = maps:get(created_at, Exported)
    }.

pre63_mqueue_to_list(MQueue) ->
    #mqueue_pre63{q = PQueue} = setelement(1, MQueue, mqueue_pre63),
    case PQueue of
        {queue, In, Out, _Len} ->
            Out ++ lists:reverse(In);
        {pqueue, Queues} ->
            lists:append([Out ++ lists:reverse(In) || {_P, {queue, In, Out, _}} <- Queues])
    end.

list_to_pre63_mqueue(#{zone := Zone}, Messages) ->
    PTable =
        case get_mqtt_conf(Zone, mqueue_priorities) of
            disabled ->
                disabled;
            Priorities ->
                %% The topics in mqtt.mqueue_priorities are atoms.
                emqx_utils_maps:binary_key_map(Priorities)
        end,
    DefaultPrio =
        case get_mqtt_conf(Zone, mqueue_default_priority) of
            lowest -> 0;
            highest -> infinity;
            N -> N
        end,
    Len = length(Messages),
    MQ = #mqueue_pre63{
        store_qos0 = get_mqtt_conf(Zone, mqueue_store_qos0),
        max_len = get_mqtt_conf(Zone, max_mqueue_len),
        len = Len,
        dropped = 0,
        p_table = PTable,
        default_p = DefaultPrio,
        q = {queue, [], Messages, Len},
        %% The baseline. Those versions compute it from the priorities, and
        %% recompute it on the next shift.
        shift_opts = #shift_opts{multiplier = 10, base = 0},
        last_prio = undefined,
        p_credit = undefined
    },
    setelement(1, MQ, mqueue).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

inflight_to_list(#inflight{tree = Tree}) ->
    [
        #{
            packet_id => PacketId,
            phase => Phase,
            message => Message,
            timestamp => Timestamp
        }
     || {PacketId, #inflight_data{phase = Phase, message = Message, timestamp = Timestamp}} <-
            gb_trees:to_list(Tree)
    ].

list_to_inflight(Opts, Entries) ->
    Tree = lists:foldl(
        fun(
            #{packet_id := PacketId, phase := Phase, message := Message, timestamp := Timestamp},
            Acc
        ) ->
            Data = #inflight_data{phase = Phase, message = Message, timestamp = Timestamp},
            gb_trees:insert(PacketId, Data, Acc)
        end,
        gb_trees:empty(),
        Entries
    ),
    #inflight{max_size = maps:get(receive_maximum, Opts, 0), tree = Tree}.

session_conf(ClientInfo, Opts) ->
    case Opts of
        #{conf := Conf} -> Conf;
        #{} -> emqx_session:get_session_conf(ClientInfo)
    end.

get_mqtt_conf(Zone, Key) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key]).
