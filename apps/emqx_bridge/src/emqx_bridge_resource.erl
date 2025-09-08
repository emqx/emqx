%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_resource).

-include("emqx_bridge_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([
    resource_id/1,
    resource_id/2,
    resource_id/3,
    bridge_id/2,
    parse_bridge_id/1,
    parse_bridge_id/2,
    bridge_hookpoint/1,
    bridge_hookpoint_to_bridge_id/1
]).

-define(ROOT_KEY_ACTIONS, actions).

-type namespace() :: binary().

resource_id(BridgeId) when is_binary(BridgeId) ->
    resource_id_for_kind(?ROOT_KEY_ACTIONS, BridgeId).

resource_id(BridgeType, BridgeName) ->
    resource_id(?ROOT_KEY_ACTIONS, BridgeType, BridgeName).

resource_id(ConfRootKey, BridgeType, BridgeName) ->
    BridgeId = bridge_id(BridgeType, BridgeName),
    resource_id_for_kind(ConfRootKey, BridgeId).

resource_id_for_kind(ConfRootKey, BridgeId) when is_binary(BridgeId) ->
    case binary:split(BridgeId, <<":">>) of
        [Type, _Name] ->
            case emqx_bridge_v2:is_bridge_v2_type(Type) of
                true ->
                    emqx_bridge_v2:bridge_v1_id_to_connector_resource_id(ConfRootKey, BridgeId);
                false ->
                    <<"bridge:", BridgeId/binary>>
            end;
        _ ->
            invalid_data(<<"should be of pattern {type}:{name}, but got ", BridgeId/binary>>)
    end.

-doc """
Returns identifier of the `Type:Name` form.
""".
bridge_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

parse_bridge_id(BridgeId) ->
    parse_bridge_id(bin(BridgeId), #{atom_name => true}).

%% Attempts to convert the type in the Id to a bridge v2 type, returning the v1 type if it
%% fails.  At the time of writing, the latter should be impossible, as all v1 bridges have
%% been converted to actions/sources.
-spec parse_bridge_id(binary() | atom(), #{atom_name => boolean()}) ->
    #{
        namespace := ?global_ns | namespace(),
        kind := undefined | action | source,
        type := V2Type,
        name := atom() | binary()
    }
when
    V2Type :: atom().
parse_bridge_id(<<"bridge:", Id/binary>>, Opts) ->
    parse_bridge_id(Id, Opts);
parse_bridge_id(BridgeId, Opts) ->
    {Type, Name} = emqx_resource:parse_resource_id(BridgeId, Opts),
    #{
        namespace => ?global_ns,
        kind => undefined,
        type => emqx_bridge_lib:upgrade_type(Type),
        name => Name
    }.

bridge_hookpoint(BridgeId) ->
    <<"$bridges/", (bin(BridgeId))/binary>>.

bridge_hookpoint_to_bridge_id(?BRIDGE_HOOKPOINT(BridgeId)) ->
    {ok, BridgeId};
bridge_hookpoint_to_bridge_id(?SOURCE_HOOKPOINT(BridgeId)) ->
    {ok, BridgeId};
bridge_hookpoint_to_bridge_id(_) ->
    {error, bad_bridge_hookpoint}.

-spec invalid_data(binary()) -> no_return().
invalid_data(Reason) -> throw(#{kind => validation_error, reason => Reason}).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
