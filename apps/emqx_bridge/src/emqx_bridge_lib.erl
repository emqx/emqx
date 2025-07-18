%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_lib).

-include_lib("emqx/include/emqx_config.hrl").

-export([
    maybe_withdraw_rule_action/4,
    external_ids/4,
    upgrade_type/1,
    downgrade_type/2,
    get_conf/2
]).

%% @doc A bridge can be used as a rule action.
%% The bridge-ID in rule-engine's world is the action-ID.
%% This function is to remove a bridge (action) from all rules
%% using it if the `rule_actions' is included in `DeleteDeps' list
%% **N.B.**: helper for deprecated bridge v1 api
maybe_withdraw_rule_action(ConfRootKey, BridgeType, BridgeName, DeleteDeps) ->
    BridgeIds = external_ids(?global_ns, ConfRootKey, BridgeType, BridgeName),
    DeleteActions = lists:member(rule_actions, DeleteDeps),
    maybe_withdraw_rule_action_loop(BridgeIds, DeleteActions).

maybe_withdraw_rule_action_loop([], _DeleteActions) ->
    ok;
maybe_withdraw_rule_action_loop([BridgeId | More], DeleteActions) ->
    case emqx_rule_engine:get_rule_ids_by_action(BridgeId) of
        [] ->
            maybe_withdraw_rule_action_loop(More, DeleteActions);
        RuleIds when DeleteActions ->
            lists:foreach(
                fun(R) ->
                    emqx_rule_engine:ensure_action_removed(R, BridgeId)
                end,
                RuleIds
            ),
            maybe_withdraw_rule_action_loop(More, DeleteActions);
        RuleIds ->
            {error, #{
                reason => rules_depending_on_this_bridge,
                bridge_id => BridgeId,
                rule_ids => RuleIds
            }}
    end.

%% @doc Kafka producer bridge renamed from 'kafka' to 'kafka_bridge' since 5.3.1.
upgrade_type(Type) when is_atom(Type) ->
    emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(Type);
upgrade_type(Type) when is_binary(Type) ->
    atom_to_binary(emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(Type));
upgrade_type(Type) when is_list(Type) ->
    atom_to_list(emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(list_to_binary(Type))).

%% @doc Kafka producer bridge type renamed from 'kafka' to 'kafka_bridge' since 5.3.1
downgrade_type(Type, Conf) when is_atom(Type) ->
    emqx_bridge_v2:bridge_v2_type_to_bridge_v1_type(Type, Conf);
downgrade_type(Type, Conf) when is_binary(Type) ->
    atom_to_binary(emqx_bridge_v2:bridge_v2_type_to_bridge_v1_type(Type, Conf));
downgrade_type(Type, Conf) when is_list(Type) ->
    atom_to_list(emqx_bridge_v2:bridge_v2_type_to_bridge_v1_type(list_to_binary(Type), Conf)).

%% A rule might be referencing an old version bridge type name
%% i.e. 'kafka' instead of 'kafka_producer' so we need to try both
external_ids(Namespace, ConfRootKey, Type, Name) ->
    case downgrade_type(Type, get_conf(Namespace, ConfRootKey, Type, Name)) of
        Type ->
            [external_id(Type, Name)];
        Type0 ->
            [external_id(Type0, Name), external_id(Type, Name)]
    end.

get_conf(BridgeType, BridgeName) ->
    get_conf(?global_ns, _ConfRootKey = undefined, BridgeType, BridgeName).

get_conf(Namespace, ConfRootKey, BridgeType, BridgeName) ->
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true ->
            ConfRootKey1 =
                case ConfRootKey of
                    undefined ->
                        emqx_bridge_v2:get_conf_root_key_if_only_one(
                            Namespace, BridgeType, BridgeName
                        );
                    _ ->
                        ConfRootKey
                end,
            get_raw_config(Namespace, [ConfRootKey1, BridgeType, BridgeName]);
        false ->
            undefined
    end.

%% Creates the external id for the bridge_v2 that is used by the rule actions
%% to refer to the bridge_v2
external_id(Type0, Name0) ->
    Name = bin(Name0),
    Type = bin(Type0),
    <<Type/binary, ":", Name/binary>>.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

get_raw_config(Namespace, KeyPath) when is_binary(Namespace) ->
    emqx:get_raw_namespaced_config(Namespace, KeyPath);
get_raw_config(?global_ns, KeyPath) ->
    emqx:get_raw_config(KeyPath).
