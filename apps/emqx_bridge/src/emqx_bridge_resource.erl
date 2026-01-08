%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_resource).

-include("emqx_bridge_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([
    bridge_id/2,
    parse_bridge_id/1,
    parse_bridge_id/2,
    bridge_hookpoint/1,
    bridge_hookpoint_to_bridge_id/1
]).

-type namespace() :: binary().

-doc """
Returns identifier of the `Type:Name` form.
""".
bridge_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

parse_bridge_id(BridgeId) ->
    parse_bridge_id(bin(BridgeId), #{atom_name => true}).

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
        type => Type,
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

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
