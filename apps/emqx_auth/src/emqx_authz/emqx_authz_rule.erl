%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_rule).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% APIs
-export([
    match/4,
    matches/4,
    compile/1,
    compile/4
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include("emqx_authz.hrl").

%%--------------------------------------------------------------------
%% "condition" types describe compiled rules used internally for matching
%%--------------------------------------------------------------------

-type permission_resolution() :: allow | deny.

-type who_condition() ::
    ipaddress()
    | username()
    | clientid()
    | {'and', [ipaddress() | username() | clientid()]}
    | {'or', [ipaddress() | username() | clientid()]}
    | all.
-type ipaddress() ::
    {ipaddr, esockd_cidr:cidr_string()}
    | {ipaddrs, list(esockd_cidr:cidr_string())}.
-type username() :: {username, binary()}.
-type clientid() :: {clientid, binary()}.

-type action_condition() ::
    subscribe
    | publish
    | #{action_type := subscribe, qos := qos_condition()}
    | #{action_type := publish | all, qos := qos_condition(), retain := retain_condition()}
    | all.
-type qos_condition() :: [qos()].
-type retain_condition() :: retain() | all.

-type topic_condition() :: list(emqx_types:topic() | {eq, emqx_types:topic()}).

-type rule() :: {permission_resolution(), who_condition(), action_condition(), topic_condition()}.

-export_type([
    permission_resolution/0,
    action_condition/0,
    topic_condition/0,
    rule/0
]).

%%--------------------------------------------------------------------
%% `action()` type describes client's actions that are mached
%% against the compiled "condition" rules
%%--------------------------------------------------------------------

-type qos() :: emqx_types:qos().
-type retain() :: boolean().
-type action() ::
    #{action_type := subscribe, qos := qos()}
    | #{action_type := publish, qos := qos(), retain := retain()}.

-export_type([action/0, qos/0, retain/0]).

%%--------------------------------------------------------------------
%% "precompiled" types describe rule DSL that is used in "acl.conf" file
%% to describe rules. Also, rules extracted from external sources
%% like database, etc. are preprocessed into these types first
%%--------------------------------------------------------------------

-type permission_resolution_precompile() :: permission_resolution().

-type who_precompile() :: who_condition().

-type subscribe_option_precompile() :: {qos, qos() | [qos()]}.
-type publish_option_precompile() :: {qos, qos() | [qos()]} | {retain, retain_condition()}.

-type action_precompile() ::
    subscribe
    | {subscribe, [subscribe_option_precompile()]}
    | publish
    | {publish, [publish_option_precompile()]}
    | all
    | {all, [publish_option_precompile()]}.

%% besides exact `topic_condition()` we also accept `<<"eq ...">>` and `"eq ..."`
%% as precompiled topic conditions
-type topic_precompile() :: topic_condition() | binary() | string().

-type rule_precompile() :: {
    permission_resolution_precompile(), who_condition(), action_precompile(), [topic_precompile()]
}.

-export_type([
    permission_resolution_precompile/0,
    action_precompile/0,
    topic_precompile/0,
    rule_precompile/0
]).

-define(IS_PERMISSION(Permission), (Permission =:= allow orelse Permission =:= deny)).
-define(ALLOWED_VARS, [
    ?VAR_USERNAME,
    ?VAR_CLIENTID,
    ?VAR_CERT_CN_NAME,
    ?VAR_ZONE,
    ?VAR_NS_CLIENT_ATTRS
]).

-spec compile(permission_resolution_precompile(), who_precompile(), action_precompile(), [
    topic_precompile()
]) -> rule().
compile(Permission, Who, Action, TopicFilters) ->
    compile({Permission, Who, Action, TopicFilters}).

-spec compile({permission_resolution_precompile(), all} | rule_precompile()) -> rule().
compile({Permission, all}) when
    ?IS_PERMISSION(Permission)
->
    {Permission, all, all, [compile_topic(<<"#">>)]};
compile({Permission, Who, Action, TopicFilters}) when
    ?IS_PERMISSION(Permission) andalso is_list(TopicFilters)
->
    {Permission, compile_who(Who), compile_action(Action), [
        compile_topic(Topic)
     || Topic <- TopicFilters
    ]};
compile({Permission, _Who, _Action, _TopicFilter}) when not ?IS_PERMISSION(Permission) ->
    throw(#{
        reason => invalid_authorization_permission,
        value => Permission
    });
compile(BadRule) ->
    throw(#{
        reason => invalid_authorization_rule,
        value => BadRule
    }).

compile_action(Action) ->
    compile_action(emqx_authz:feature_available(rich_actions), Action).

-define(IS_ACTION_WITH_RETAIN(Action), (Action =:= publish orelse Action =:= all)).

compile_action(_RichActionsOn, subscribe) ->
    subscribe;
compile_action(_RichActionsOn, Action) when ?IS_ACTION_WITH_RETAIN(Action) ->
    Action;
compile_action(true = _RichActionsOn, {subscribe, Opts}) when is_list(Opts) ->
    #{
        action_type => subscribe,
        qos => qos_from_opts(Opts)
    };
compile_action(true = _RichActionsOn, {Action, Opts}) when
    ?IS_ACTION_WITH_RETAIN(Action) andalso is_list(Opts)
->
    #{
        action_type => Action,
        qos => qos_from_opts(Opts),
        retain => retain_from_opts(Opts)
    };
compile_action(_RichActionsOn, Action) ->
    throw(#{
        reason => invalid_authorization_action,
        value => Action
    }).

qos_from_opts(Opts) ->
    try
        case proplists:get_all_values(qos, Opts) of
            [] ->
                ?DEFAULT_RULE_QOS;
            QoSs ->
                lists:flatmap(
                    fun
                        (QoS) when is_integer(QoS) ->
                            [validate_qos(QoS)];
                        (QoS) when is_list(QoS) ->
                            lists:map(fun validate_qos/1, QoS)
                    end,
                    QoSs
                )
        end
    catch
        throw:{bad_qos, QoS} ->
            throw(#{
                reason => invalid_authorization_qos,
                qos => QoS
            })
    end.

validate_qos(QoS) when is_integer(QoS), QoS >= 0, QoS =< 2 ->
    QoS;
validate_qos(QoS) ->
    throw({bad_qos, QoS}).

retain_from_opts(Opts) ->
    case proplists:get_value(retain, Opts, ?DEFAULT_RULE_RETAIN) of
        all ->
            all;
        Retain when is_boolean(Retain) ->
            Retain;
        Value ->
            throw(#{
                reason => invalid_authorization_retain,
                value => Value
            })
    end.

compile_who(all) ->
    all;
compile_who({user, Username}) ->
    compile_who({username, Username});
compile_who({username, {re, Username}}) ->
    {ok, MP} = re:compile(bin(Username)),
    {username, MP};
compile_who({username, Username}) ->
    {username, {eq, bin(Username)}};
compile_who({client, Clientid}) ->
    compile_who({clientid, Clientid});
compile_who({clientid, {re, Clientid}}) ->
    {ok, MP} = re:compile(bin(Clientid)),
    {clientid, MP};
compile_who({clientid, Clientid}) ->
    {clientid, {eq, bin(Clientid)}};
compile_who({ipaddr, CIDR}) ->
    {ipaddr, esockd_cidr:parse(CIDR, true)};
compile_who({ipaddrs, CIDRs}) ->
    {ipaddrs, lists:map(fun(CIDR) -> esockd_cidr:parse(CIDR, true) end, CIDRs)};
compile_who({'and', L}) when is_list(L) ->
    {'and', [compile_who(Who) || Who <- L]};
compile_who({'or', L}) when is_list(L) ->
    {'or', [compile_who(Who) || Who <- L]};
compile_who(Who) ->
    throw(#{
        reason => invalid_client_match_condition,
        identifier => Who
    }).

compile_topic("eq " ++ Topic) ->
    {eq, emqx_topic:words(bin(Topic))};
compile_topic(<<"eq ", Topic/binary>>) ->
    {eq, emqx_topic:words(Topic)};
compile_topic({eq, Topic}) ->
    {eq, emqx_topic:words(bin(Topic))};
compile_topic(Topic) ->
    Template = emqx_auth_template:parse_str(Topic, ?ALLOWED_VARS),
    case emqx_template:is_const(Template) of
        true -> emqx_topic:words(bin(Topic));
        false -> {pattern, Template}
    end.

bin(L) when is_list(L) ->
    unicode:characters_to_binary(L);
bin(B) when is_binary(B) ->
    B.

-spec matches(emqx_types:clientinfo(), action(), emqx_types:topic(), [rule()]) ->
    {matched, allow} | {matched, deny} | nomatch.
matches(_Client, _Action, _Topic, []) ->
    nomatch;
matches(Client, Action, Topic, [{Permission, WhoCond, ActionCond, TopicCond} | Tail]) ->
    case match(Client, Action, Topic, {Permission, WhoCond, ActionCond, TopicCond}) of
        nomatch -> matches(Client, Action, Topic, Tail);
        Matched -> Matched
    end.

-spec match(emqx_types:clientinfo(), action(), emqx_types:topic(), rule()) ->
    {matched, allow} | {matched, deny} | nomatch.
match(Client, Action, Topic, {Permission, WhoCond, ActionCond, TopicCond}) ->
    case
        match_action(Action, ActionCond) andalso
            match_who(Client, WhoCond) andalso
            match_topics(Client, Topic, TopicCond)
    of
        true -> {matched, Permission};
        _ -> nomatch
    end.

-spec match_action(action(), action_condition()) -> boolean().
match_action(#{action_type := publish}, PubSubCond) when is_atom(PubSubCond) ->
    match_pubsub(publish, PubSubCond);
match_action(
    #{action_type := publish, qos := QoS, retain := Retain}, #{
        action_type := publish, qos := QoSCond, retain := RetainCond
    }
) ->
    match_qos(QoS, QoSCond) andalso match_retain(Retain, RetainCond);
match_action(#{action_type := publish, qos := QoS, retain := Retain}, #{
    action_type := all, qos := QoSCond, retain := RetainCond
}) ->
    match_qos(QoS, QoSCond) andalso match_retain(Retain, RetainCond);
match_action(#{action_type := subscribe}, PubSubCond) when is_atom(PubSubCond) ->
    match_pubsub(subscribe, PubSubCond);
match_action(#{action_type := subscribe, qos := QoS}, #{action_type := subscribe, qos := QoSCond}) ->
    match_qos(QoS, QoSCond);
match_action(#{action_type := subscribe, qos := QoS}, #{action_type := all, qos := QoSCond}) ->
    match_qos(QoS, QoSCond);
match_action(_, PubSubCond) ->
    true = is_pubsub_cond(PubSubCond),
    false.

is_pubsub_cond(publish) ->
    true;
is_pubsub_cond(subscribe) ->
    true;
is_pubsub_cond(#{action_type := A}) ->
    is_pubsub_cond(A).

match_pubsub(publish, publish) -> true;
match_pubsub(subscribe, subscribe) -> true;
match_pubsub(_, all) -> true;
match_pubsub(_, _) -> false.

match_qos(QoS, QoSs) -> lists:member(QoS, QoSs).

match_retain(_, all) -> true;
match_retain(Retain, Retain) when is_boolean(Retain) -> true;
match_retain(_, _) -> false.

match_who(_, all) ->
    true;
match_who(#{username := undefined}, {username, _}) ->
    false;
match_who(#{username := Username}, {username, {eq, Username}}) ->
    true;
match_who(#{username := Username}, {username, {re_pattern, _, _, _, _} = MP}) ->
    case re:run(Username, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_who(#{clientid := Clientid}, {clientid, {eq, Clientid}}) ->
    true;
match_who(#{clientid := Clientid}, {clientid, {re_pattern, _, _, _, _} = MP}) ->
    case re:run(Clientid, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_who(#{peerhost := undefined}, {ipaddr, _CIDR}) ->
    false;
match_who(#{peerhost := IpAddress}, {ipaddr, CIDR}) ->
    esockd_cidr:match(IpAddress, CIDR);
match_who(#{peerhost := undefined}, {ipaddrs, _CIDR}) ->
    false;
match_who(#{peerhost := IpAddress}, {ipaddrs, CIDRs}) ->
    lists:any(
        fun(CIDR) ->
            esockd_cidr:match(IpAddress, CIDR)
        end,
        CIDRs
    );
match_who(ClientInfo, {'and', Principals}) when is_list(Principals) ->
    lists:foldl(
        fun(Principal, Permission) ->
            Permission andalso match_who(ClientInfo, Principal)
        end,
        true,
        Principals
    );
match_who(ClientInfo, {'or', Principals}) when is_list(Principals) ->
    lists:foldl(
        fun(Principal, Permission) ->
            Permission orelse match_who(ClientInfo, Principal)
        end,
        false,
        Principals
    );
match_who(_, _) ->
    false.

match_topics(_ClientInfo, _Topic, []) ->
    false;
match_topics(ClientInfo, Topic, [{pattern, PatternFilter} | Filters]) ->
    TopicFilter = render_topic(PatternFilter, ClientInfo),
    (is_binary(TopicFilter) andalso
        match_topic(emqx_topic:words(Topic), emqx_topic:words(TopicFilter))) orelse
        match_topics(ClientInfo, Topic, Filters);
match_topics(ClientInfo, Topic, [TopicFilter | Filters]) ->
    match_topic(emqx_topic:words(Topic), TopicFilter) orelse
        match_topics(ClientInfo, Topic, Filters).

match_topic(Topic, {'eq', TopicFilter}) ->
    Topic =:= TopicFilter;
match_topic(Topic, TopicFilter) ->
    emqx_topic:match(Topic, TopicFilter).

render_topic(Topic, ClientInfo) ->
    try
        bin(emqx_auth_template:render_strict(Topic, ClientInfo))
    catch
        error:Reason ->
            ?SLOG(debug, #{
                msg => "failed_to_render_topic_template",
                template => Topic,
                reason => Reason
            }),
            error
    end.
