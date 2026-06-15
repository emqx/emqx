%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% re:mp()
-type re_pattern() :: tuple().

-type condition() ::
    all
    | client_condition()
    | security_profile()
    | {'and', [condition()]}
    | {'or', [condition()]}.
-type client_condition() ::
    ipaddress()
    | username()
    | clientid()
    | client_attr()
    | zone()
    | listener().
-type ipaddress() :: {ipaddr, esockd_cidr:cidr_string()} | {ipaddrs, [esockd_cidr:cidr_string()]}.
-type username() :: {username, binary() | re_pattern()}.
-type clientid() :: {clientid, binary() | re_pattern()}.
-type client_attr() :: {client_attr, Name :: binary(), Value :: binary() | re_pattern()}.
-type zone() :: {zone, Zone :: atom() | binary() | re_pattern()}.
-type listener() :: {listener, Listener :: atom() | binary() | re_pattern()}.
-type security_profile() :: {security_profile, legacy | hardened}.

-type action_condition() ::
    subscribe
    | publish
    | #{action_type := subscribe, qos := qos_condition()}
    | #{action_type := publish | all, qos := qos_condition(), retain := retain_condition()}
    | all.
-type qos_condition() :: [qos()].
-type retain_condition() :: retain() | all.

-type topic_condition() :: list(emqx_types:topic() | {eq, emqx_types:topic()} | ?ALL_TOPICS).

-type rule() :: {permission_resolution(), condition(), action_condition(), topic_condition()}.

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

-type ip_address_precompile() ::
    {ipaddr, esockd_cidr:cidr_string()} | {ipaddrs, list(esockd_cidr:cidr_string())}.
-type username_precompile() :: {username, binary()} | {username, {re, iodata()}}.
-type clientid_precompile() :: {clientid, binary()} | {clientid, {re, iodata()}}.
-type client_attr_precompile() ::
    {client_attr, binary(), binary()} | {client_attr, binary(), {re, iodata()}}.
-type zone_precompile() :: {zone, atom() | binary() | {re, iodata()}}.
-type listener_precompile() :: {listener, atom() | binary() | {re, iodata()}}.
-type security_profile_precompile() :: {security_profile, legacy | hardened}.

-type condition_precompile() ::
    all
    | client_condition_precompile()
    | security_profile_precompile()
    | {'and', [condition_precompile()]}
    | {'or', [condition_precompile()]}.
-type client_condition_precompile() ::
    ip_address_precompile()
    | username_precompile()
    | clientid_precompile()
    | client_attr_precompile()
    | zone_precompile()
    | listener_precompile().

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
    permission_resolution_precompile(),
    condition_precompile(),
    action_precompile(),
    all | [topic_precompile()]
}.

-export_type([
    permission_resolution_precompile/0,
    condition_precompile/0,
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

-define(RE_PATTERN, {re_pattern, _, _, _, _}).

-spec compile(permission_resolution_precompile(), condition_precompile(), action_precompile(), [
    topic_precompile()
]) -> rule().
compile(Permission, Cond, Action, TopicFilters) ->
    compile({Permission, Cond, Action, TopicFilters}).

-spec compile(
    {permission_resolution_precompile(), all | security_profile_precompile()} | rule_precompile()
) -> rule().
compile({Permission, all}) when ?IS_PERMISSION(Permission) ->
    compile({Permission, all, all, all});
compile({Permission, {security_profile, _} = Cond}) when ?IS_PERMISSION(Permission) ->
    compile({Permission, Cond, all, all});
compile({Permission, Cond, Action, all}) when ?IS_PERMISSION(Permission) ->
    {Permission, compile_condition(Cond), compile_action(Action), [?ALL_TOPICS]};
compile({Permission, Cond, Action, TopicFilters}) when
    ?IS_PERMISSION(Permission) andalso is_list(TopicFilters)
->
    {Permission, compile_condition(Cond), compile_action(Action), [
        compile_topic(Topic)
     || Topic <- TopicFilters
    ]};
compile({Permission, _Cond, _Action, _TopicFilter}) when not ?IS_PERMISSION(Permission) ->
    throw(#{
        reason => invalid_authorization_permission,
        value => Permission
    });
compile(BadRule) ->
    throw(#{
        reason => invalid_authorization_rule,
        value => BadRule
    }).

-define(IS_ACTION_WITH_RETAIN(Action), (Action =:= publish orelse Action =:= all)).

compile_action(all) ->
    all;
compile_action(subscribe) ->
    subscribe;
compile_action(Action) when ?IS_ACTION_WITH_RETAIN(Action) ->
    Action;
compile_action({subscribe, Opts}) when is_list(Opts) ->
    #{
        action_type => subscribe,
        qos => qos_from_opts(Opts)
    };
compile_action({Action, Opts}) when
    ?IS_ACTION_WITH_RETAIN(Action) andalso is_list(Opts)
->
    #{
        action_type => Action,
        qos => qos_from_opts(Opts),
        retain => retain_from_opts(Opts)
    };
compile_action(Action) ->
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

compile_condition(all) ->
    all;
compile_condition({user, Username}) ->
    compile_condition({username, Username});
compile_condition({username, {re, Username}}) ->
    re_compile(username, Username);
compile_condition({username, Username}) ->
    {username, {eq, bin(Username)}};
compile_condition({client, Clientid}) ->
    compile_condition({clientid, Clientid});
compile_condition({clientid, {re, Clientid}}) ->
    re_compile(clientid, Clientid);
compile_condition({clientid, Clientid}) ->
    {clientid, {eq, bin(Clientid)}};
compile_condition({client_attr, Name, {re, Attr}}) ->
    {_, MP} = re_compile({client_attr, Name}, bin(Attr)),
    {client_attr, bin(Name), MP};
compile_condition({client_attr, Name, Attr}) ->
    {client_attr, bin(Name), {eq, bin(Attr)}};
compile_condition({zone, {re, Zone}}) ->
    re_compile(zone, Zone);
compile_condition({zone, Zone}) ->
    {zone, {eq, Zone}};
compile_condition({listener, {re, Listener}}) ->
    re_compile(listener, Listener);
compile_condition({listener, Listener}) ->
    {listener, {eq, Listener}};
compile_condition({security_profile, Profile}) when Profile =:= legacy; Profile =:= hardened ->
    {security_profile, Profile};
compile_condition({ipaddr, CIDR}) ->
    {ipaddr, esockd_cidr:parse(CIDR, true)};
compile_condition({ipaddrs, CIDRs}) ->
    {ipaddrs, lists:map(fun(CIDR) -> esockd_cidr:parse(CIDR, true) end, CIDRs)};
compile_condition({'and', L}) when is_list(L) ->
    {'and', [compile_condition(Cond) || Cond <- L]};
compile_condition({'or', L}) when is_list(L) ->
    {'or', [compile_condition(Cond) || Cond <- L]};
compile_condition(Cond) ->
    throw(#{
        reason => invalid_client_match_condition,
        identifier => Cond
    }).
re_compile(Tag, Pattern) ->
    case re:compile(bin(Pattern)) of
        {ok, MP} ->
            {Tag, MP};
        {error, Reason} ->
            throw(#{
                reason => invalid_re_pattern,
                type => Tag,
                error => Reason
            })
    end.

compile_topic("eq " ++ Topic) ->
    {eq, emqx_topic:words(bin(Topic))};
compile_topic(<<"eq ", Topic/binary>>) ->
    {eq, emqx_topic:words(Topic)};
compile_topic({eq, Topic}) ->
    {eq, emqx_topic:words(bin(Topic))};
compile_topic(Topic) ->
    {_, Template} = emqx_auth_template:parse_str(Topic, ?ALLOWED_VARS),
    case emqx_template:is_const(Template) of
        true -> emqx_topic:words(bin(Topic));
        false -> {pattern, Template}
    end.

bin(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
bin(L) when is_list(L) ->
    unicode:characters_to_binary(L);
bin(B) when is_binary(B) ->
    B.

-spec matches(emqx_types:clientinfo(), action(), emqx_types:topic(), [rule()]) ->
    {matched, allow} | {matched, deny} | nomatch.
matches(_Client, _Action, _Topic, []) ->
    nomatch;
matches(Client, Action, Topic, [{Permission, Cond, ActionCond, TopicCond} | Tail]) ->
    case match(Client, Action, Topic, {Permission, Cond, ActionCond, TopicCond}) of
        nomatch -> matches(Client, Action, Topic, Tail);
        Matched -> Matched
    end.

-spec match(emqx_types:clientinfo(), action(), emqx_types:topic(), rule()) ->
    {matched, allow} | {matched, deny} | nomatch.
match(Client, Action, Topic, {Permission, Cond, ActionCond, TopicCond}) ->
    try
        match_action(Action, ActionCond) andalso
            match_condition(Client, Cond) andalso
            match_topics(Client, Topic, TopicCond)
    of
        true -> {matched, Permission};
        _ -> nomatch
    catch
        throw:_Reason ->
            case emqx_authz_utils:authz_backend_failure_policy() of
                ignore -> nomatch;
                deny -> {matched, deny}
            end
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

match_condition(_, all) ->
    true;
match_condition(#{username := undefined}, {username, _}) ->
    false;
match_condition(#{username := Username}, {username, {eq, Username}}) ->
    true;
match_condition(#{username := Username}, {username, ?RE_PATTERN = MP}) ->
    is_re_match(Username, MP);
match_condition(#{clientid := Clientid}, {clientid, {eq, Clientid}}) ->
    true;
match_condition(#{clientid := Clientid}, {clientid, ?RE_PATTERN = MP}) ->
    is_re_match(Clientid, MP);
match_condition(#{client_attrs := Attrs}, {client_attr, Name, {eq, Value}}) ->
    maps:get(Name, Attrs, undefined) =:= Value;
match_condition(#{client_attrs := Attrs}, {client_attr, Name, ?RE_PATTERN = MP}) ->
    Value = maps:get(Name, Attrs, undefined),
    is_binary(Value) andalso is_re_match(Value, MP);
match_condition(#{zone := Zone}, {zone, {eq, Zone1}}) ->
    Zone =:= Zone1 orelse bin(Zone) =:= bin(Zone1);
match_condition(#{zone := Zone}, {zone, ?RE_PATTERN = MP}) ->
    is_re_match(Zone, MP);
match_condition(#{listener := Listener}, {listener, {eq, Listener1}}) ->
    Listener =:= Listener1 orelse bin(Listener) =:= bin(Listener1);
match_condition(#{listener := Listener}, {listener, ?RE_PATTERN = MP}) ->
    is_re_match(Listener, MP);
match_condition(_ClientInfo, {security_profile, Profile}) ->
    emqx_security_profile:profile() =:= Profile;
match_condition(#{peerhost := undefined}, {ipaddr, _CIDR}) ->
    false;
match_condition(#{peerhost := IpAddress}, {ipaddr, CIDR}) ->
    esockd_cidr:match(IpAddress, CIDR);
match_condition(#{peerhost := undefined}, {ipaddrs, _CIDR}) ->
    false;
match_condition(#{peerhost := IpAddress}, {ipaddrs, CIDRs}) ->
    lists:any(
        fun(CIDR) ->
            esockd_cidr:match(IpAddress, CIDR)
        end,
        CIDRs
    );
match_condition(ClientInfo, {'and', Conds}) when is_list(Conds) ->
    lists:foldl(
        fun(Cond, Matched) ->
            Matched andalso match_condition(ClientInfo, Cond)
        end,
        true,
        Conds
    );
match_condition(ClientInfo, {'or', Conds}) when is_list(Conds) ->
    lists:foldl(
        fun(Cond, Matched) ->
            Matched orelse match_condition(ClientInfo, Cond)
        end,
        false,
        Conds
    );
match_condition(_, _) ->
    false.

is_re_match(Value, Pattern) ->
    case re:run(bin(Value), Pattern) of
        {match, _} -> true;
        _ -> false
    end.

match_topics(_ClientInfo, _Topic, []) ->
    false;
match_topics(_ClientInfo, _Topic, [?ALL_TOPICS | _Filters]) ->
    true;
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
        TopicTemplateAllow = topic_template_allow(),
        bin(
            emqx_auth_template:render_strict(Topic, ClientInfo, #{
                var_trans => fun(Name, Value) ->
                    validate_topic_template_value(Name, Value, TopicTemplateAllow)
                end
            })
        )
    catch
        error:Reason ->
            ?SLOG(debug, #{
                msg => "failed_to_render_topic_template",
                template => Topic,
                reason => Reason
            }),
            case emqx_security_profile:policy(authz_backend_failure) of
                ignore -> error;
                deny -> throw({cannot_render_topic_template, Reason})
            end
    end.

topic_template_allow() ->
    AllowMap = emqx:get_config([authorization, topic_template_allow], #{
        plus => false,
        hash => false,
        slash => false
    }),
    #{
        $+ => maps:get(plus, AllowMap, false),
        $# => maps:get(hash, AllowMap, false),
        $/ => maps:get(slash, AllowMap, false)
    }.

validate_topic_template_value(Name, Value, TopicTemplateAllow) ->
    Rendered = emqx_template:to_string(Value),
    ok = do_validate_topic_template_value(
        Name, unicode:characters_to_list(Rendered), TopicTemplateAllow
    ),
    Rendered.

do_validate_topic_template_value(_Name, [], _TopicTemplateAllow) ->
    ok;
do_validate_topic_template_value(Name, [Char | Rest], TopicTemplateAllow) when
    Char =:= $/ orelse Char =:= $# orelse Char =:= $+
->
    case maps:get(Char, TopicTemplateAllow) of
        true ->
            do_validate_topic_template_value(Name, Rest, TopicTemplateAllow);
        false ->
            throw({invalid_char_in_value_substituted_in_topic, {Name, Char}})
    end;
do_validate_topic_template_value(Name, [_Char | Rest], TopicTemplateAllow) ->
    do_validate_topic_template_value(Name, Rest, TopicTemplateAllow);
do_validate_topic_template_value(Name, InvalidUnicode, _TopicTemplateAllow) ->
    throw({invalid_value_substituted_in_topic, {Name, InvalidUnicode}}).
