%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_spb_state).

%% API
-export([
    ensure_table/0,

    parse_spb_topic/1,
    register_aliases/2,
    load_aliases/1,

    get_current_alias_mapping/0,
    delete_all_mappings_in_pd/0
]).

%% Internal exports (for schema registry application)
-export([cleanup_entries_for/1]).

%% Debug/test only
-export([all_mappings/0]).

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-include("emqx_schema_registry_internal_spb.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(TAB, emqx_schema_registry_spb_state).
-define(CURR_MAPPING_PD_KEY, {?MODULE, alias_mapping}).
-define(ALL_MAPPINGS_PD_KEY, {?MODULE, all_alias_mappings}).
-define(ENTRY(KEY, MAPPING, PID), {KEY, MAPPING, PID}).

-type spb_message() :: spb_birth_message() | spb_data_message().
-type spb_birth_message() :: #nbirth{} | #dbirth{}.
-type spb_data_message() :: #ndata{} | #ddata{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec ensure_table() -> ok.
ensure_table() ->
    emqx_utils_ets:new(?TAB, [
        public, ordered_set, {write_concurrency, auto}, {read_concurrency, true}
    ]).

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
            Mapping = gather_aliases(Metrics),
            Key = key(BirthMsg),
            case map_size(Mapping) > 0 of
                true ->
                    register_mapping_key_pd(Key),
                    insert_alias_mapping(Key, Mapping);
                false ->
                    delete_alias_mapping(Key)
            end,
            ok;
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

-spec load_aliases(spb_data_message()) -> ok.
load_aliases(DataMsg) ->
    Key = key(DataMsg),
    case lookup_alias_mapping(Key) of
        undefined ->
            ok;
        #{} = Mapping ->
            put_current_mapping_pd(Mapping)
    end.

get_current_alias_mapping() ->
    case get_current_mapping_pd() of
        undefined -> #{};
        Mapping -> Mapping
    end.

delete_all_mappings_in_pd() ->
    Keys = maps:keys(get_known_mapping_keys_pd()),
    lists:foreach(fun delete_alias_mapping/1, Keys),
    emqx_schema_registry_spb_gc:unregister_client(self()),
    ?tp("sr_mappings_deleted", #{keys => Keys}),
    ok.

%%------------------------------------------------------------------------------
%% Internal exports (for schema registry application)
%%------------------------------------------------------------------------------

-spec cleanup_entries_for(pid()) -> ok.
cleanup_entries_for(Pid) ->
    MS = [{?ENTRY('_', '_', Pid), [], [true]}],
    N = ets:select_delete(?TAB, MS),
    ?tp("sr_mappings_deleted", #{pid => Pid, num_deleted => N}),
    ok.

%%------------------------------------------------------------------------------
%% Debug/test only
%%------------------------------------------------------------------------------

all_mappings() ->
    ets:tab2list(?TAB).

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
    emqx_schema_registry_spb_gc:register_client(self()),
    ets:insert(?TAB, ?ENTRY(Key, Mapping, self())).

lookup_alias_mapping(Key) ->
    emqx_utils_ets:lookup_value(?TAB, Key, undefined).

delete_alias_mapping(Key) ->
    ets:delete(?TAB, Key).

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

get_known_mapping_keys_pd() ->
    case get(?ALL_MAPPINGS_PD_KEY) of
        undefined ->
            #{};
        Keys ->
            Keys
    end.

put_known_mapping_keys_pd(Keys) ->
    put(?ALL_MAPPINGS_PD_KEY, Keys).

register_mapping_key_pd(Key) ->
    Keys0 = get_known_mapping_keys_pd(),
    Keys = Keys0#{Key => true},
    put_known_mapping_keys_pd(Keys),
    ok.
