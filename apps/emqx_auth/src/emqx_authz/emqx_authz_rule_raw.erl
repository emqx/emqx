%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_rule_raw).

-export([parse_rule/1, parse_and_compile_rules/1, format_rule/1]).

-include("emqx_authz.hrl").

%% Raw rules have the following format:
%%    [
%%        #{
%%            %% <<"allow">> | <"deny">>,
%%            <<"permission">> => <<"allow">>,
%%
%%            %% <<"pub">> | <<"sub">> | <<"all">>
%%            <<"action">> => <<"pub">>,
%%
%%            %% <<"a/$#">>, <<"eq a/b/+">>, ...
%%            <<"topic">> => TopicFilter,
%%
%%            %% when 'topic' is not provided
%%            <<"topics">> => [TopicFilter],
%%
%%            %%  0 | 1 | 2 | [0, 1, 2] | <<"0">> | <<"1">> | ...
%%            <<"qos">> => 0,
%%
%%            %% true | false | all | 0 | 1 | <<"true">> | ...
%%            %% only for pub action
%%            <<"retain">> => true,
%%
%%            %% Optional filters.
%%            %% Each filter should match for the rule to be appiled.
%%            <<"clientid_re">> => <<"^client-[0-9]+$">>,
%%            <<"username_re">> => <<"^user-[0-9]+$">>,
%%            <<"ipaddr">> => <<"192.168.5.0/24">>
%%            <<"zone">> => <<"zone1">>,
%%            <<"zone_re">> => <<"^zone-[0-9]+$">>,
%%            <<"listener">> => <<"tcp:default">>,
%%            <<"listener_re">> => <<"^tcp:.*$">>,
%%        },
%%        ...
%%    ],
-type rule_raw() :: #{binary() => binary() | [binary()]}.
-type legacy_rule() :: {
    emqx_authz_rule:permission_resolution_precompile(),
    emqx_authz_rule:action_precompile(),
    emqx_authz_rule:topic_precompile()
}.

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
                {ok, {Permission, Who, Action, Topics}} ->
                    try
                        emqx_authz_rule:compile({Permission, Who, Action, Topics})
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
        emqx_authz_rule:permission_resolution_precompile(),
        emqx_authz_rule:who_precompile(),
        emqx_authz_rule:action_precompile(),
        emqx_authz_rule:topic_precompile()
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
        Who = validate_rule_who(RuleRaw),
        ActionType = validate_rule_action_type(ActionTypeRaw),
        Action = validate_rule_action(ActionType, RuleRaw),
        {ok, {Permission, Who, Action, Topics}}
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

-spec format_rule(emqx_authz_rule:rule() | legacy_rule()) -> map().
format_rule({Permission, Action, Topics}) ->
    format_rule({Permission, all, Action, Topics});
format_rule({Permission, Who, Action, Topics}) when is_list(Topics) ->
    merge_maps(
        [
            #{
                topic => lists:map(fun format_topic/1, Topics),
                permission => Permission
            },
            format_action(Action),
            format_who(Who)
        ]
    );
format_rule({Permission, Who, Action, Topic}) ->
    merge_maps([
        #{
            topic => format_topic(Topic),
            permission => Permission
        },
        format_action(Action),
        format_who(Who)
    ]).

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

validate_rule_action(publish, RuleRaw) ->
    Qos = validate_rule_qos(maps:get(<<"qos">>, RuleRaw, ?DEFAULT_RULE_QOS)),
    Retain = validate_rule_retain(maps:get(<<"retain">>, RuleRaw, <<"all">>)),
    {publish, [{qos, Qos}, {retain, Retain}]};
validate_rule_action(subscribe, RuleRaw) ->
    Qos = validate_rule_qos(maps:get(<<"qos">>, RuleRaw, ?DEFAULT_RULE_QOS)),
    {subscribe, [{qos, Qos}]};
validate_rule_action(all, RuleRaw) ->
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

validate_rule_who(RuleRaw) ->
    case validate_rule_who(maps:to_list(RuleRaw), []) of
        [] -> all;
        [Who] -> Who;
        WhoList -> {'and', WhoList}
    end.

validate_rule_who([], WhoList) ->
    WhoList;
validate_rule_who([{<<"username_re">>, UsernameReRaw} | Rest], WhoList) ->
    validate_rule_who(Rest, [
        {username, {re, validate(re, invalid_username_re, UsernameReRaw)}} | WhoList
    ]);
validate_rule_who([{<<"clientid_re">>, ClientIdReRaw} | Rest], WhoList) ->
    validate_rule_who(Rest, [
        {clientid, {re, validate(re, invalid_clientid_re, ClientIdReRaw)}} | WhoList
    ]);
validate_rule_who([{<<"zone">>, ZoneRaw} | Rest], WhoList) ->
    validate_rule_who(Rest, [{zone, validate(str, invalid_zone, ZoneRaw)} | WhoList]);
validate_rule_who([{<<"zone_re">>, ZoneReRaw} | Rest], WhoList) ->
    validate_rule_who(Rest, [{zone, {re, validate(re, invalid_zone_re, ZoneReRaw)}} | WhoList]);
validate_rule_who([{<<"listener">>, ListenerRaw} | Rest], WhoList) ->
    validate_rule_who(Rest, [{listener, validate(str, invalid_listener, ListenerRaw)} | WhoList]);
validate_rule_who([{<<"listener_re">>, ListenerReRaw} | Rest], WhoList) ->
    validate_rule_who(Rest, [
        {listener, {re, validate(re, invalid_listener_re, ListenerReRaw)}} | WhoList
    ]);
validate_rule_who([{<<"ipaddr">>, IpAddrRaw} | Rest], WhoList) ->
    validate_rule_who(Rest, [{ipaddr, validate(ipaddr, invalid_ipaddr, IpAddrRaw)} | WhoList]);
validate_rule_who([_ | Rest], WhoList) ->
    %% Drop unknown fields (including username and clientid).
    %% The "exact match (of username and clientid)" are deliberately dropped
    %% here because they should be a part of the query parameters.
    %% Adding them back is perhaps more complete in a sense, but it might become
    %% a breaking change for existing users who happen to have been returning
    %% username or clientid which do not match the current client.
    validate_rule_who(Rest, WhoList).

validate(re, Invalid, Raw) ->
    try
        true = (is_binary(Raw) andalso size(Raw) > 0),
        case re:compile(Raw) of
            {ok, _} -> Raw;
            {error, _} -> throw({Invalid, Raw})
        end
    catch
        _:_ -> throw({Invalid, Raw})
    end;
validate(str, Invalid, Raw) ->
    try
        R = unicode:characters_to_binary(Raw),
        true = (is_binary(R) andalso size(R) > 0),
        R
    catch
        _:_ -> throw({Invalid, Raw})
    end;
validate(ipaddr, Invalid, Raw) ->
    try
        [_ | _] = unicode:characters_to_list(Raw)
    catch
        _:_ -> throw({Invalid, Raw})
    end.

format_action(Action) when is_atom(Action) ->
    #{
        action => Action
    };
format_action({ActionType, Opts}) ->
    #{
        action => ActionType,
        qos => proplists:get_value(qos, Opts, ?DEFAULT_RULE_QOS),
        retain => proplists:get_value(retain, Opts, ?DEFAULT_RULE_RETAIN)
    }.

format_topic({eq, Topic}) when is_binary(Topic) ->
    <<"eq ", Topic/binary>>;
format_topic(Topic) when is_binary(Topic) ->
    Topic.

format_who(all) ->
    #{};
format_who({username, {re, UsernameRe}}) ->
    #{username_re => UsernameRe};
format_who({clientid, {re, ClientIdRe}}) ->
    #{clientid_re => ClientIdRe};
format_who({zone, {re, ZoneRe}}) ->
    #{zone_re => ZoneRe};
format_who({zone, Zone}) ->
    #{zone => Zone};
format_who({listener, {re, ListenerRe}}) ->
    #{listener_re => ListenerRe};
format_who({listener, Listener}) ->
    #{listener => Listener};
format_who({ipaddr, IpAddr}) when is_list(IpAddr) ->
    #{ipaddr => unicode:characters_to_binary(IpAddr)};
format_who({'and', WhoList}) ->
    merge_maps(lists:map(fun format_who/1, WhoList));
format_who(Who) ->
    throw({invalid_who, Who}).

merge_maps(Maps) ->
    lists:foldl(fun(Map, Acc) -> maps:merge(Acc, Map) end, #{}, Maps).
