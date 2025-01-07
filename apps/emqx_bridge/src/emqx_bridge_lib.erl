%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_bridge_lib).

-export([
    maybe_withdraw_rule_action/3,
    maybe_withdraw_rule_action/4,
    upgrade_type/1,
    downgrade_type/2,
    get_conf/2,
    get_conf/3
]).

%% @doc A bridge can be used as a rule action.
%% The bridge-ID in rule-engine's world is the action-ID.
%% This function is to remove a bridge (action) from all rules
%% using it if the `rule_actions' is included in `DeleteDeps' list
maybe_withdraw_rule_action(BridgeType, BridgeName, DeleteDeps) ->
    maybe_withdraw_rule_action(undefined, BridgeType, BridgeName, DeleteDeps).

maybe_withdraw_rule_action(ConfRootKey, BridgeType, BridgeName, DeleteDeps) ->
    BridgeIds = external_ids(ConfRootKey, BridgeType, BridgeName),
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
external_ids(ConfRootKey, Type, Name) ->
    case downgrade_type(Type, get_conf(ConfRootKey, Type, Name)) of
        Type ->
            [external_id(Type, Name)];
        Type0 ->
            [external_id(Type0, Name), external_id(Type, Name)]
    end.

get_conf(BridgeType, BridgeName) ->
    get_conf(undefined, BridgeType, BridgeName).
get_conf(ConfRootKey, BridgeType, BridgeName) ->
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true ->
            ConfRootKey1 =
                case ConfRootKey of
                    undefined ->
                        emqx_bridge_v2:get_conf_root_key_if_only_one(BridgeType, BridgeName);
                    _ ->
                        ConfRootKey
                end,
            emqx_conf:get_raw([ConfRootKey1, BridgeType, BridgeName]);
        false ->
            undefined
    end.

%% Creates the external id for the bridge_v2 that is used by the rule actions
%% to refer to the bridge_v2
external_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
