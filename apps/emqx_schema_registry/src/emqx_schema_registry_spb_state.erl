%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_spb_state).

%% API
-export([
    parse_spb_topic/1,
    register_aliases/2,
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
    try emqx_schema_registry_serde:rsf_spb_decode([Payload]) of
        #{<<"metrics">> := Metrics} ->
            do_register_aliases(BirthMsg, Metrics);
        #{metrics := _} = AtomMap ->
            %% If the sparkplugb schema code was compiled and cached in an older EMQX
            %% version (< 6.0.0) that happens to use the same OTP version as the current
            %% node, then it used `gpb` options that output atom-keyed maps.  This is a
            %% workaround for such cases.
            #{<<"metrics">> := Metrics} = emqx_utils_maps:binary_key_map(AtomMap),
            do_register_aliases(BirthMsg, Metrics);
        _ ->
            %% No `metrics` field?
            ok
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

do_register_aliases(BirthMsg, Metrics) ->
    Mapping = gather_aliases(Metrics),
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

gather_aliases(Metrics) ->
    lists:foldl(
        fun
            (#{<<"alias">> := Alias, <<"name">> := Name}, Acc) ->
                Acc#{Alias => Name};
            (_, Acc) ->
                Acc
        end,
        #{},
        Metrics
    ).

%% Called in channel process context.
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
