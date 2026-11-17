%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_spb_state).

%% API
-export([
    parse_spb_topic/1,
    register_aliases/2,
    scan_aliases/1,
    load_aliases/1,

    get_current_alias_mapping/0
]).

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-include("emqx_schema_registry_internal_spb.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CURR_MAPPING_PD_KEY, {?MODULE, alias_mapping}).
-define(ALL_MAPPINGS_PD_KEY, {?MODULE, all_alias_mappings}).
-define(ENTRY(KEY, MAPPING, PID), {KEY, MAPPING, PID}).

-type spb_message() :: spb_birth_message() | spb_data_message().
-type spb_birth_message() :: #nbirth{} | #dbirth{}.
-type spb_data_message() :: #ndata{} | #ddata{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% Note: only message types of interest for this module are parsed, not necessarily all
%% SpB types.
-spec parse_spb_topic(binary()) -> {ok, spb_message()} | error.
parse_spb_topic(Topic) ->
    case emqx_topic:words(Topic) of
        [Namespace, GroupId, <<"NBIRTH">>, EdgeNodeId] ->
            {ok, #nbirth{
                namespace = Namespace,
                group_id = GroupId,
                edge_node_id = EdgeNodeId
            }};
        [Namespace, GroupId, <<"NDATA">>, EdgeNodeId] ->
            {ok, #ndata{
                namespace = Namespace,
                group_id = GroupId,
                edge_node_id = EdgeNodeId
            }};
        [Namespace, GroupId, <<"DBIRTH">>, EdgeNodeId, DeviceId] ->
            {ok, #dbirth{
                namespace = Namespace,
                group_id = GroupId,
                edge_node_id = EdgeNodeId,
                device_id = DeviceId
            }};
        [Namespace, GroupId, <<"DDATA">>, EdgeNodeId, DeviceId] ->
            {ok, #ddata{
                namespace = Namespace,
                group_id = GroupId,
                edge_node_id = EdgeNodeId,
                device_id = DeviceId
            }};
        _ ->
            error
    end.

-spec register_aliases(emqx_types:message(), spb_birth_message()) -> ok.
register_aliases(#message{payload = Payload}, BirthMsg) ->
    try scan_aliases(Payload) of
        Mapping ->
            do_register_aliases(BirthMsg, Mapping)
    catch
        Kind:Error:Stacktrace ->
            ?SLOG(warning, #{
                msg => "bad_spb_birth_message",
                exception => Kind,
                cause => Error,
                stacktrace => Stacktrace,
                note => <<"No alias mapping will be stored from this message.">>
            }),
            ok
    end.

-doc """
Return the alias to name mapping of the top-level metrics in an encoded Sparkplug B
`Payload` message.

A metric goes into the mapping only when it has both a name and an alias. When two
metrics have the same alias, the later metric wins.

This function reads only the name and the alias of each metric, and skips all other
fields without decoding them. A birth message can carry many metrics with large
property sets, and a full decode of such a message can need many times the payload
size in heap. This function runs in the publisher's channel process, which has a
maximum heap size.

Raise an error when the payload is not valid protobuf, or when it contains a group
(wire types 3 and 4). The Sparkplug B schema has no groups.
""".
-spec scan_aliases(binary()) -> #{non_neg_integer() => binary()}.
scan_aliases(Payload) ->
    scan_payload(Payload, #{}).

do_register_aliases(BirthMsg, Mapping) ->
    Key = key(BirthMsg),
    case map_size(Mapping) > 0 of
        true ->
            insert_alias_mapping(Key, Mapping);
        false ->
            ok
    end.

-spec load_aliases(spb_data_message()) -> ok.
load_aliases(DataMsg) ->
    Key = key(DataMsg),
    case get_known_mappings_pd() of
        #{Key := Mapping} ->
            put_current_mapping_pd(Mapping);
        _ ->
            ok
    end.

get_current_alias_mapping() ->
    case get_current_mapping_pd() of
        undefined -> #{};
        Mapping -> Mapping
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% Field keys (field number bsl 3 bor wire type) in `priv/sparkplug_b.proto'.
%% `Payload.metrics' (2, length-delimited).
-define(KEY_PAYLOAD_METRICS, ((2 bsl 3) bor ?WT_LEN)).
%% `Payload.Metric.name' (1, length-delimited).
-define(KEY_METRIC_NAME, ((1 bsl 3) bor ?WT_LEN)).
%% `Payload.Metric.alias' (2, varint).
-define(KEY_METRIC_ALIAS, ((2 bsl 3) bor ?WT_VARINT)).
-define(WT_VARINT, 0).
-define(WT_I64, 1).
-define(WT_LEN, 2).
-define(WT_I32, 5).
-define(UINT64_MAX, ((1 bsl 64) - 1)).

scan_payload(<<>>, Acc) ->
    Acc;
scan_payload(Bin0, Acc) ->
    case read_varint(Bin0) of
        {?KEY_PAYLOAD_METRICS, Bin1} ->
            {Metric, Bin} = read_len_delimited(Bin1),
            scan_payload(Bin, add_alias(scan_metric(Metric, undefined, undefined), Acc));
        {Key, Bin1} ->
            scan_payload(skip_field(Key band 7, Bin1), Acc)
    end.

scan_metric(<<>>, Name, Alias) ->
    {Name, Alias};
scan_metric(Bin0, Name, Alias) ->
    case read_varint(Bin0) of
        {?KEY_METRIC_NAME, Bin1} ->
            {NewName, Bin} = read_len_delimited(Bin1),
            scan_metric(Bin, NewName, Alias);
        {?KEY_METRIC_ALIAS, Bin1} ->
            {NewAlias, Bin} = read_varint(Bin1),
            scan_metric(Bin, Name, NewAlias band ?UINT64_MAX);
        {Key, Bin1} ->
            scan_metric(skip_field(Key band 7, Bin1), Name, Alias)
    end.

add_alias({Name, Alias}, Acc) when is_binary(Name), is_integer(Alias) ->
    %% Copy the name so that the mapping does not keep the whole payload binary alive.
    Acc#{Alias => binary:copy(Name)};
add_alias(_, Acc) ->
    Acc.

skip_field(?WT_VARINT, Bin0) ->
    {_, Bin} = read_varint(Bin0),
    Bin;
skip_field(?WT_LEN, Bin0) ->
    {_, Bin} = read_len_delimited(Bin0),
    Bin;
skip_field(?WT_I64, <<_:64, Bin/binary>>) ->
    Bin;
skip_field(?WT_I32, <<_:32, Bin/binary>>) ->
    Bin;
skip_field(WireType, _Bin) when WireType =:= ?WT_I64; WireType =:= ?WT_I32 ->
    error({bad_protobuf, truncated});
skip_field(WireType, _Bin) ->
    error({bad_protobuf, {wire_type, WireType}}).

read_len_delimited(Bin0) ->
    {Len, Bin1} = read_varint(Bin0),
    case Bin1 of
        <<Value:Len/binary, Bin/binary>> ->
            {Value, Bin};
        _ ->
            error({bad_protobuf, truncated})
    end.

%% A varint has at most 10 bytes (64 bits in groups of 7 bits).
read_varint(Bin) ->
    read_varint(Bin, 0, 0).

read_varint(<<1:1, X:7, Bin/binary>>, Shift, Acc) when Shift < 63 ->
    read_varint(Bin, Shift + 7, Acc bor (X bsl Shift));
read_varint(<<0:1, X:7, Bin/binary>>, Shift, Acc) ->
    {Acc bor (X bsl Shift), Bin};
read_varint(_Bin, _Shift, _Acc) ->
    error({bad_protobuf, bad_varint}).

%% Only reached from MQTT client channel processes; the hook callback enforces this
%% (see `emqx_schema_registry_spb_hookcb:is_client_process/0').
insert_alias_mapping(Key, Mapping) ->
    Mappings0 = get_known_mappings_pd(),
    Mappings = Mappings0#{Key => Mapping},
    put_known_mappings_pd(Mappings).

key(#nbirth{namespace = Namespace, group_id = GroupId, edge_node_id = EdgeNodeId}) ->
    {Namespace, GroupId, EdgeNodeId};
key(#ndata{namespace = Namespace, group_id = GroupId, edge_node_id = EdgeNodeId}) ->
    {Namespace, GroupId, EdgeNodeId};
key(#dbirth{
    namespace = Namespace, group_id = GroupId, edge_node_id = EdgeNodeId, device_id = DeviceId
}) ->
    {Namespace, GroupId, EdgeNodeId, DeviceId};
key(#ddata{
    namespace = Namespace, group_id = GroupId, edge_node_id = EdgeNodeId, device_id = DeviceId
}) ->
    {Namespace, GroupId, EdgeNodeId, DeviceId}.

put_current_mapping_pd(Mapping) ->
    put(?CURR_MAPPING_PD_KEY, Mapping).

get_current_mapping_pd() ->
    get(?CURR_MAPPING_PD_KEY).

get_known_mappings_pd() ->
    case get(?ALL_MAPPINGS_PD_KEY) of
        undefined ->
            #{};
        Keys ->
            Keys
    end.

put_known_mappings_pd(Mappings) ->
    _ = put(?ALL_MAPPINGS_PD_KEY, Mappings),
    ok.
