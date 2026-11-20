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
    emqx_schema_registry_pb_wire:fold(fun scan_payload_field/3, #{}, Payload).

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

%% Field numbers in `priv/sparkplug_b.proto'.
%% `Payload.metrics' (length-delimited).
-define(FIELD_PAYLOAD_METRICS, 2).
%% `Payload.Metric.name' (length-delimited).
-define(FIELD_METRIC_NAME, 1).
%% `Payload.Metric.alias' (varint).
-define(FIELD_METRIC_ALIAS, 2).

scan_payload_field(?FIELD_PAYLOAD_METRICS, {len, Metric}, Acc) ->
    NameAlias = emqx_schema_registry_pb_wire:fold(
        fun scan_metric_field/3, {undefined, undefined}, Metric
    ),
    add_alias(NameAlias, Acc);
scan_payload_field(_Field, _Value, Acc) ->
    Acc.

scan_metric_field(?FIELD_METRIC_NAME, {len, Name}, {_, Alias}) ->
    {Name, Alias};
scan_metric_field(?FIELD_METRIC_ALIAS, {varint, Alias}, {Name, _}) ->
    {Name, Alias};
scan_metric_field(_Field, _Value, Acc) ->
    Acc.

add_alias({Name, Alias}, Acc) when is_binary(Name), is_integer(Alias) ->
    %% Copy the name so that the mapping does not keep the whole payload binary alive.
    Acc#{Alias => binary:copy(Name)};
add_alias(_, Acc) ->
    Acc.

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
