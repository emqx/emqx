%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
%% @doc
%% This module converts authz rule fields obtained from
%% external sources like database or API to the format
%% accepted by emqx_authz_rule module.
%%--------------------------------------------------------------------

-module(emqx_authz_rule_raw).

-export([parse_rule/1, parse_and_compile_rules/1, format_rule/1]).

-include("emqx_authz.hrl").

-type rule_raw() :: #{binary() => binary() | [binary()]}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Parse and compile raw ACL rules.
%% If any bad rule is found, `{bad_acl_rule, ..}' is thrown.
-spec parse_and_compile_rules([rule_raw()]) -> [emqx_authz_rule:rule()].
parse_and_compile_rules(Rules) ->
    lists:map(
        fun(Rule) ->
            case parse_rule(Rule) of
                {ok, {Permission, Action, Topics}} ->
                    try
                        emqx_authz_rule:compile({Permission, all, Action, Topics})
                    catch
                        throw:Reason ->
                            throw({bad_acl_rule, Reason})
                    end;
                {error, Reason} ->
                    throw({bad_acl_rule, Reason})
            end
        end,
        Rules
    ).

-spec parse_rule(rule_raw()) ->
    {ok, {
        emqx_authz_rule:permission(),
        emqx_authz_rule:action_condition(),
        emqx_authz_rule:topic_condition()
    }}
    | {error, map()}.
parse_rule(
    #{
        <<"permission">> := PermissionRaw,
        <<"action">> := ActionTypeRaw
    } = RuleRaw
) ->
    try
        Topics = validate_rule_topics(RuleRaw),
        Permission = validate_rule_permission(PermissionRaw),
        ActionType = validate_rule_action_type(ActionTypeRaw),
        Action = validate_rule_action(ActionType, RuleRaw),
        {ok, {Permission, Action, Topics}}
    catch
        throw:{Invalid, Which} ->
            {error, #{
                reason => Invalid,
                value => Which
            }}
    end;
parse_rule(RuleRaw) ->
    {error, #{
        reason => invalid_rule,
        value => RuleRaw,
        explain => "missing 'permission' or 'action' field"
    }}.

-spec format_rule({
    emqx_authz_rule:permission(),
    emqx_authz_rule:action_condition(),
    emqx_authz_rule:topic_condition()
}) -> map().
format_rule({Permission, Action, Topics}) when is_list(Topics) ->
    maps:merge(
        #{
            topic => lists:map(fun format_topic/1, Topics),
            permission => Permission
        },
        format_action(Action)
    );
format_rule({Permission, Action, Topic}) ->
    maps:merge(
        #{
            topic => format_topic(Topic),
            permission => Permission
        },
        format_action(Action)
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

validate_rule_topics(#{<<"topic">> := TopicRaw}) when is_binary(TopicRaw) ->
    [validate_rule_topic(TopicRaw)];
validate_rule_topics(#{<<"topics">> := TopicsRaw}) when is_list(TopicsRaw) ->
    lists:map(fun validate_rule_topic/1, TopicsRaw);
validate_rule_topics(RuleRaw) ->
    throw({missing_topic_or_topics, RuleRaw}).

validate_rule_topic(<<"eq ", TopicRaw/binary>>) ->
    {eq, TopicRaw};
validate_rule_topic(TopicRaw) when is_binary(TopicRaw) -> TopicRaw.

validate_rule_permission(<<"allow">>) -> allow;
validate_rule_permission(<<"deny">>) -> deny;
validate_rule_permission(PermissionRaw) -> throw({invalid_permission, PermissionRaw}).

validate_rule_action_type(P) when P =:= <<"pub">> orelse P =:= <<"publish">> -> publish;
validate_rule_action_type(S) when S =:= <<"sub">> orelse S =:= <<"subscribe">> -> subscribe;
validate_rule_action_type(<<"all">>) -> all;
validate_rule_action_type(ActionRaw) -> throw({invalid_action, ActionRaw}).

validate_rule_action(ActionType, RuleRaw) ->
    validate_rule_action(emqx_authz:feature_available(rich_actions), ActionType, RuleRaw).

%% rich_actions disabled
validate_rule_action(false, ActionType, _RuleRaw) ->
    ActionType;
%% rich_actions enabled
validate_rule_action(true, publish, RuleRaw) ->
    Qos = validate_rule_qos(maps:get(<<"qos">>, RuleRaw, ?DEFAULT_RULE_QOS)),
    Retain = validate_rule_retain(maps:get(<<"retain">>, RuleRaw, <<"all">>)),
    {publish, [{qos, Qos}, {retain, Retain}]};
validate_rule_action(true, subscribe, RuleRaw) ->
    Qos = validate_rule_qos(maps:get(<<"qos">>, RuleRaw, ?DEFAULT_RULE_QOS)),
    {subscribe, [{qos, Qos}]};
validate_rule_action(true, all, RuleRaw) ->
    Qos = validate_rule_qos(maps:get(<<"qos">>, RuleRaw, ?DEFAULT_RULE_QOS)),
    Retain = validate_rule_retain(maps:get(<<"retain">>, RuleRaw, <<"all">>)),
    {all, [{qos, Qos}, {retain, Retain}]}.

validate_rule_qos(QosInt) when is_integer(QosInt) andalso QosInt >= 0 andalso QosInt =< 2 ->
    [QosInt];
validate_rule_qos(QosBin) when is_binary(QosBin) ->
    try
        QosRawList = binary:split(QosBin, <<",">>, [global]),
        lists:map(fun validate_rule_qos_atomic/1, QosRawList)
    catch
        _:_ ->
            throw({invalid_qos, QosBin})
    end;
validate_rule_qos(QosList) when is_list(QosList) ->
    try
        lists:map(fun validate_rule_qos_atomic/1, QosList)
    catch
        invalid_qos ->
            throw({invalid_qos, QosList})
    end;
validate_rule_qos(undefined) ->
    ?DEFAULT_RULE_QOS;
validate_rule_qos(null) ->
    ?DEFAULT_RULE_QOS;
validate_rule_qos(QosRaw) ->
    throw({invalid_qos, QosRaw}).

validate_rule_qos_atomic(<<"0">>) -> 0;
validate_rule_qos_atomic(<<"1">>) -> 1;
validate_rule_qos_atomic(<<"2">>) -> 2;
validate_rule_qos_atomic(0) -> 0;
validate_rule_qos_atomic(1) -> 1;
validate_rule_qos_atomic(2) -> 2;
validate_rule_qos_atomic(QoS) -> throw({invalid_qos, QoS}).

validate_rule_retain(<<"0">>) -> false;
validate_rule_retain(<<"1">>) -> true;
validate_rule_retain(0) -> false;
validate_rule_retain(1) -> true;
validate_rule_retain(<<"true">>) -> true;
validate_rule_retain(<<"false">>) -> false;
validate_rule_retain(true) -> true;
validate_rule_retain(false) -> false;
validate_rule_retain(undefined) -> ?DEFAULT_RULE_RETAIN;
validate_rule_retain(null) -> ?DEFAULT_RULE_RETAIN;
validate_rule_retain(<<"all">>) -> ?DEFAULT_RULE_RETAIN;
validate_rule_retain(Retain) -> throw({invalid_retain, Retain}).

format_action(Action) ->
    format_action(emqx_authz:feature_available(rich_actions), Action).

%% rich_actions disabled
format_action(false, Action) when is_atom(Action) ->
    #{
        action => Action
    };
format_action(false, {ActionType, _Opts}) ->
    #{
        action => ActionType
    };
%% rich_actions enabled
format_action(true, Action) when is_atom(Action) ->
    #{
        action => Action
    };
format_action(true, {ActionType, Opts}) ->
    #{
        action => ActionType,
        qos => proplists:get_value(qos, Opts, ?DEFAULT_RULE_QOS),
        retain => proplists:get_value(retain, Opts, ?DEFAULT_RULE_RETAIN)
    }.

format_topic({eq, Topic}) when is_binary(Topic) ->
    <<"eq ", Topic/binary>>;
format_topic(Topic) when is_binary(Topic) ->
    Topic.
